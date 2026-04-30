#!/usr/bin/env python3
"""Bulk-import prehraj.to uploads for films already present in the DB.

Issue #520 — parent epic #518. Reads all prehraj.to sub-sitemaps, reconstructs
per-film upload clusters, joins them against TMDB-matched IMDB IDs from the
pilot CSV, and inserts per-upload rows into `film_prehrajto_uploads` for every
cluster whose IMDB ID already exists in `films`. Recomputes rollup flags and
`prehrajto_primary_upload_id` on `films`.

Safety guarantees (hard-enforced at runtime):
  - never DELETEs from films, film_prehrajto_uploads, or any other table
  - never UPDATEs existing films columns other than the prehraj.to rollup ones
  - INSERT ... ON CONFLICT DO UPDATE for uploads (idempotent)
  - row-count invariant: films count before == films count after (abort if not)
  - --dry-run uses a single transaction + ROLLBACK at the end
  - live run commits in batches (every --commit-every films) to avoid
    multi-hour transactions; the invariant still fires at the end, but
    already-committed batches are not reverted when it trips

Mark-dead behaviour (#644):
  - At end of run, for every film_id touched in this run, any
    `film_prehrajto_uploads` / `video_sources(prehrajto)` row whose upload_id
    is no longer in the live sitemap gets `is_alive = FALSE`. Catches
    rotated upload_ids (Spasitel 2026 case: prehraj.to silently re-uploaded
    under a new 16-hex while the old went 404 "Soubor nenalezen").
  - Per-film, not global — `--limit` partial runs only flag films they
    actually visited. A full-catalog mark-dead requires a full sitemap pull.
  - Pass `--no-mark-dead` for partial-sitemap runs (test/pilot/single shard)
    that would otherwise mis-flag rows whose uploads simply weren't in the
    pulled subset.

Usage:
  DATABASE_URL=postgres://... python3 scripts/import-prehrajto-uploads.py \\
      --sitemap-dir /tmp/prehrajto-pilot \\
      --matches /tmp/prehrajto-pilot/matches-full.csv \\
      --dry-run
"""

from __future__ import annotations

import argparse
import csv
import html
import math
import os
import re
import sys
import time
import unicodedata
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)

# Dual-write helper (#607 / #610).
sys.path.insert(0, str(Path(__file__).parent))
from video_sources_helper import (  # noqa: E402
    get_provider_ids,
    dual_write_prehrajto_upload,
)


# ---------------------------------------------------------------------------
# Sitemap parsing + clustering (vendored from /tmp/prehrajto-pilot/match_tmdb.py)
# ---------------------------------------------------------------------------

_LOC_RE = re.compile(r"<loc>([^<]+)</loc>")
_TITLE_RE = re.compile(r"<video:title>([^<]*)</video:title>")
_DUR_RE = re.compile(r"<video:duration>(\d+)</video:duration>")
_VIEWS_RE = re.compile(r"<video:view_count>(\d+)</video:view_count>")
_LIVE_RE = re.compile(r"<video:live>(yes|no)</video:live>")
_URL_BLOCK_RE = re.compile(r"<url>(.*?)</url>", re.DOTALL)
_UPLOAD_ID_RE = re.compile(r"/([a-f0-9]{13,16})(?:[/?#]|$)")
_YEAR_RE = re.compile(r"\b(19[2-9]\d|20[0-3]\d)\b")
_EPISODE_RE = re.compile(r"\bS\d{1,2}[\s._-]?E\d{1,3}\b", re.IGNORECASE)


def extract_year(title: str) -> int | None:
    m = _YEAR_RE.search(title)
    return int(m.group(1)) if m else None


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def strip_title(title: str) -> str:
    t = title
    t = re.sub(r"\[([^\]]*)\]", r" \1 ", t)
    t = re.sub(r"\(([^)]*)\)", r" \1 ", t)
    t = _YEAR_RE.sub(" ", t)
    t = re.sub(r"\.(?=[A-Za-z])", " ", t)
    for g in (
        r"c(?:z|s)\s*dabing", r"s(?:k|l)\s*dabing",
        r"c(?:z|s)\s*tit(?:ulky)?", r"s(?:k|l)\s*tit(?:ulky)?",
        r"c(?:z|s)\s*dab", r"s(?:k|l)\s*dab",
        r"cztit", r"cesky\s*dabing", r"dabing", r"dabovane",
    ):
        t = re.sub(g, " ", t, flags=re.IGNORECASE)
    t = re.sub(
        r"\b(cz|sk|en|cesky|slovensky|titulky|tit|subs?|dub|eng|"
        r"hd|fhd|full\s*hd|1080p|720p|4k|2160p|uhd|webrip|bluray|bdrip|dvdrip|"
        r"hdtv|tvrip|hd\s*rip|dvd\s*rip|web\.?dl|x264|x265|h\.?264|h\.?265|hevc|"
        r"aac|ac3|5\.1|avi|mkv|mp4|"
        r"cely\s*film|cely|remastered|extended|uncut|directors?\s*cut|novinka|"
        r"top\s*hit|hit|novinka|premiera|"
        r"romant\.?|drama|horor|thriller|akc\.?|komedie|sci[-.]?fi|fantasy|rodinny|"
        r"muzikal|p\.?p\.?|valec\.?|dobrodruzny|animovany|animovane|anim\.?|"
        r"krimi|sportovni|koko|povidky|cd\.?\d*)\b",
        " ", t, flags=re.IGNORECASE,
    )
    t = re.sub(r"\s+", " ", t).strip(" -_.,/|")
    t = re.sub(r"[,\.]\s*(?=[,\.])", "", t)
    t = re.sub(r"\s*,\s*$", "", t)
    t = re.sub(r"^\s*[,\.-]+\s*", "", t)
    return re.sub(r"\s+", " ", t).strip(" -_.,/|")


def _unescape(s: str) -> str:
    # Sitemap values can be XML-entity-escaped once or twice; auto_import's
    # title_parser also double-unescapes. Idempotent on fully-decoded text.
    return html.unescape(html.unescape(s))


def parse_sitemap(path: Path, chunk_size: int = 1 << 20) -> Iterator[dict]:
    """Stream-parse a sitemap file, yielding one dict per <url> element.

    Uses a chunked regex parser rather than ElementTree.iterparse: some pilot
    shards contain raw backslashes / stray bytes in descriptions that break
    strict XML (e.g. video-sitemap-358.xml), and we still want to extract the
    surrounding valid <url> blocks. Reads `chunk_size` bytes at a time and
    carries a partial trailing block between chunks, so peak RSS stays bounded
    regardless of file size.
    """
    carry = ""
    with open(path, encoding="utf-8", errors="replace") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            data = carry + chunk
            last_close = data.rfind("</url>")
            if last_close < 0:
                carry = data
                continue
            complete = data[: last_close + len("</url>")]
            carry = data[last_close + len("</url>") :]
            for m in _URL_BLOCK_RE.finditer(complete):
                block = m.group(1)
                loc_m = _LOC_RE.search(block)
                title_m = _TITLE_RE.search(block)
                if not loc_m or not title_m:
                    continue
                dur_m = _DUR_RE.search(block)
                views_m = _VIEWS_RE.search(block)
                live_m = _LIVE_RE.search(block)
                yield {
                    "url": _unescape(loc_m.group(1)),
                    "title": _unescape(title_m.group(1)),
                    "duration": int(dur_m.group(1)) if dur_m else 0,
                    "views": int(views_m.group(1)) if views_m else 0,
                    "live": live_m.group(1) if live_m else "no",
                }


def film_shape(row: dict) -> bool:
    t, d = row["title"], row["duration"]
    if row["live"] == "yes" or not t:
        return False
    if _EPISODE_RE.search(t):
        return False
    if d < 60 * 60 or d > 240 * 60:
        return False
    if extract_year(t) is None:
        return False
    return row["views"] >= 50


def cluster_key(row: dict) -> tuple:
    core = normalize(strip_title(row["title"]))
    year = extract_year(row["title"])
    dur_bucket = row["duration"] // (3 * 60)
    return (core, year, dur_bucket)


def extract_upload_id(url: str) -> str | None:
    m = _UPLOAD_ID_RE.search(url or "")
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Language detection (vendored from /tmp/prehrajto-pilot/report.py)
# ---------------------------------------------------------------------------

CZ_DIACRITICS = set("ěščřžýáíéúůťďňôäľĺŕ")
CZ_WORDS = {
    "a", "i", "do", "na", "se", "si", "ze", "za", "po", "pro", "pod", "nad",
    "v", "u", "o", "s", "k", "ke", "ku", "je", "jsou", "byl", "byla", "bylo",
    "mě", "mně", "mi", "tě", "ty", "ten", "ta", "to", "jeho", "její",
    "není", "náš", "naše", "svůj", "svá", "svou", "svém", "svému",
    "co", "kdo", "kde", "kdy", "proč", "jak", "jaký", "která",
    "jsem", "jsi", "jsme", "jste",
}

CZ_DUB_RE = re.compile(r"(?:\bcz\s*dab(?:ing)?\b|\bczdab\w*|\bczdub\w*|\bcesk[aáyý]\s*dab(?:ing)?\b|\bc[zs]\s*dabing\b|cesky\s*dabing|cz\s*\.dab\b)", re.IGNORECASE)
CZ_SUB_RE = re.compile(r"(?:\bcz\s*tit(?:ulky)?\b|\bcztit\w*|\bcz\s*subs?\b|\bc[zs]\s*titulky\b|cesk[yé]\s*titulky)", re.IGNORECASE)
SK_DUB_RE = re.compile(r"(?:\bsk\s*dab(?:ing)?\b|\bskdab\w*|\bskdub\w*|\bsloven(?:sk[yáé]|ina)\s*dab(?:ing)?\b)", re.IGNORECASE)
SK_SUB_RE = re.compile(r"(?:\bsk\s*tit(?:ulky)?\b|\bsktit\w*)", re.IGNORECASE)
EN_ONLY_RE = re.compile(r"(?:\bengsub\b|\beng\s*sub\b|\beng\s*only\b|\bengdub\b)", re.IGNORECASE)


def detect_lang(title: str) -> str:
    if not title:
        return "UNKNOWN"
    t = title.lower()
    if CZ_DUB_RE.search(t):
        return "CZ_DUB"
    if SK_DUB_RE.search(t):
        return "SK_DUB"
    if CZ_SUB_RE.search(t):
        return "CZ_SUB"
    if SK_SUB_RE.search(t):
        return "SK_SUB"
    has_cz = bool(re.search(r"\bcz\b", t)) or bool(re.search(r"\bcesk[yáyé]", t))
    if EN_ONLY_RE.search(t) and not has_cz:
        return "EN"
    dia_hits = sum(1 for c in t if c in CZ_DIACRITICS)
    tokens = re.findall(r"[a-záčďéěíňóřšťúůýž]+", t)
    cz_word_hits = sum(1 for tok in tokens if tok in CZ_WORDS)
    if dia_hits >= 1 and cz_word_hits >= 1:
        return "CZ_NATIVE"
    if dia_hits >= 2:
        return "CZ_NATIVE"
    return "UNKNOWN"


# ---------------------------------------------------------------------------
# Primary-upload scoring
# ---------------------------------------------------------------------------

LANG_PRIORITY = {
    "CZ_DUB": 6, "CZ_NATIVE": 5, "CZ_SUB": 4,
    "SK_DUB": 3, "SK_SUB": 2, "UNKNOWN": 1, "EN": 0,
}

_RES_RE = re.compile(r"(2160p|1080p|720p|480p|BDRip|BluRay|WEBRip|WEB[\s-]?DL|HDRip|DVDRip|HDTV|TVRip|CAM|TS)", re.IGNORECASE)
_RES_SCORE = {
    "2160p": 6, "1080p": 5, "720p": 4, "480p": 2,
    "BLURAY": 5, "BDRIP": 4, "WEBRIP": 4, "WEBDL": 4, "WEB-DL": 4, "WEB DL": 4,
    "HDRIP": 3, "HDTV": 3, "TVRIP": 2, "DVDRIP": 2,
    "CAM": 0, "TS": 0,
}


def extract_resolution(title: str) -> str | None:
    m = _RES_RE.search(title or "")
    if not m:
        return None
    return m.group(1).lower()


def _res_score(hint: str | None) -> int:
    if not hint:
        return 1
    key = hint.upper().replace("-", "").replace(" ", "")
    return _RES_SCORE.get(key, 1)


def rank(lang_class: str, resolution_hint: str | None, view_count: int) -> float:
    lp = LANG_PRIORITY.get(lang_class, 0)
    rs = _res_score(resolution_hint)
    vs = math.log10((view_count or 0) + 1)
    return lp * 1000 + rs * 10 + vs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_matches(path: Path) -> dict[tuple, dict]:
    """Load MATCHED/LIKELY rows from the pilot matches CSV, keyed by cluster."""
    matches_by_key: dict[tuple, dict] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["verdict"] not in ("MATCHED", "LIKELY"):
                continue
            if not row["imdb_id"]:
                continue
            try:
                year = int(row["cluster_year"]) if row["cluster_year"] else None
                dur_bucket = int(row["cluster_duration_min"]) // 3
            except ValueError:
                continue
            key = (row["cluster_core"], year, dur_bucket)
            matches_by_key[key] = row
    return matches_by_key


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sitemap-dir", required=True,
                    help="Directory containing video-sitemap-*.xml files")
    ap.add_argument("--matches", required=True,
                    help="Path to matches-full.csv (from pilot)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse, compute, but ROLLBACK at the end — no changes committed")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N distinct films (0 = all)")
    ap.add_argument("--commit-every", type=int, default=500,
                    help="In live (non-dry-run) mode, commit after every N films "
                         "(default 500). Set 0 to keep a single transaction.")
    ap.add_argument("--no-mark-dead", action="store_true",
                    help="Skip the end-of-run sweep that flips is_alive=FALSE for "
                         "uploads no longer in the sitemap. Use with partial "
                         "sitemaps or test runs to avoid mis-flagging live uploads.")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print("ERROR: DATABASE_URL env var required", file=sys.stderr)
        return 2

    sitemap_dir = Path(args.sitemap_dir)
    files = sorted(sitemap_dir.glob("video-sitemap-*.xml"),
                   key=lambda p: int(re.search(r"(\d+)", p.stem).group(1)))
    if not files:
        print(f"ERROR: no video-sitemap-*.xml files in {sitemap_dir}", file=sys.stderr)
        return 2

    # ---- Load IMDB matches from pilot CSV first ----
    # We need the wanted cluster-key set up front so we can discard sitemap rows
    # outside it as we stream, instead of materialising the full 9M-entry catalog.
    print(f"Loading matches from {args.matches}...")
    matches_by_key = load_matches(Path(args.matches))
    wanted_keys = set(matches_by_key.keys())
    print(f"  {len(matches_by_key):,} IMDB-matched clusters in CSV")

    # ---- Stream-parse sitemaps, clustering only rows whose key is wanted ----
    print(f"Parsing {len(files)} sitemaps from {sitemap_dir}...")
    t0 = time.time()
    clusters: dict[tuple, list[dict]] = defaultdict(list)
    total_entries = 0
    film_shape_count = 0
    for p in files:
        for r in parse_sitemap(p):
            total_entries += 1
            if not film_shape(r):
                continue
            film_shape_count += 1
            k = cluster_key(r)
            if k in wanted_keys:
                clusters[k].append(r)
    print(f"  {total_entries:,} total entries scanned in {time.time()-t0:.1f}s")
    print(f"  {film_shape_count:,} film-shape entries")
    print(f"  {len(clusters):,} clusters matched wanted set")

    # ---- Connect + find films in DB ----
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM films")
        films_count_before = cur.fetchone()[0]
        print(f"films baseline count: {films_count_before:,}")

        # Pre-fetch imdb_id → film_id for all candidate imdb_ids (deduped —
        # many cluster keys can share the same IMDb ID).
        candidate_imdbs = sorted({m["imdb_id"] for m in matches_by_key.values()})
        cur.execute("SELECT imdb_id, id FROM films WHERE imdb_id = ANY(%s)",
                    (candidate_imdbs,))
        imdb_to_film_id = {imdb: fid for imdb, fid in cur.fetchall()}
        print(f"  {len(imdb_to_film_id):,} of them already in DB (target for this run)")

        # ---- Aggregate uploads per film_id ----
        # One IMDB can resolve to several cluster keys (different title/duration
        # variants in sitemap). All their uploads must land on the same film_id
        # before we pick a primary — otherwise the primary would flip based on
        # dict iteration order.
        film_uploads: dict[int, list[dict]] = defaultdict(list)
        cluster_hits = 0
        for key, match in matches_by_key.items():
            film_id = imdb_to_film_id.get(match["imdb_id"])
            if film_id is None:
                continue
            uploads = clusters.get(key, [])
            if not uploads:
                continue
            film_uploads[film_id].extend(uploads)
            cluster_hits += 1
            if args.limit and len(film_uploads) >= args.limit:
                break
        print(f"  {cluster_hits:,} clusters → {len(film_uploads):,} distinct films to enrich")

        if not film_uploads:
            print("Nothing to do.")
            return 0

        # ---- Upsert uploads + compute rollups + UPDATE films ----
        inserted = 0
        updated_flags = 0
        skipped_no_upload_id = 0
        films_with_no_upload_id = 0
        # #644: per-film record of upload_ids seen in THIS run. Used at end of
        # the loop to flag rows whose upload_id is no longer on prehraj.to as
        # is_alive=FALSE — catches the rotated-IDs case (Spasitel 2026:
        # prehraj.to silently re-uploaded under a new 16-hex while the old
        # one became 404 "Soubor nenalezen"). Per-film instead of global so a
        # `--limit` partial run only touches the films it actually visited.
        seen_per_film: dict[int, set[str]] = defaultdict(set)

        upsert_sql = """
        INSERT INTO film_prehrajto_uploads
            (film_id, upload_id, url, title, duration_sec, view_count,
             lang_class, resolution_hint, last_seen_at, is_alive)
        VALUES
            (%(film_id)s, %(upload_id)s, %(url)s, %(title)s, %(duration_sec)s, %(view_count)s,
             %(lang_class)s, %(resolution_hint)s, NOW(), TRUE)
        ON CONFLICT (film_id, upload_id) DO UPDATE SET
            url             = EXCLUDED.url,
            title           = EXCLUDED.title,
            duration_sec    = EXCLUDED.duration_sec,
            view_count      = EXCLUDED.view_count,
            lang_class      = EXCLUDED.lang_class,
            resolution_hint = EXCLUDED.resolution_hint,
            last_seen_at    = EXCLUDED.last_seen_at,
            is_alive        = TRUE
        """

        # films update — rollup flags are assigned directly from the current
        # run's aggregated uploads for this film, not OR'd onto previous values.
        # This makes reruns authoritative: if a language marker disappears from
        # all matching uploads in the sitemap, the flag flips back to false.
        # (Partial runs — e.g. --limit — only touch films they actually reach.)
        update_film_sql = """
        UPDATE films SET
            prehrajto_primary_upload_id = %(primary)s,
            prehrajto_has_dub           = %(has_cz_audio)s,
            prehrajto_has_subs          = %(has_cz_subs)s,
            prehrajto_has_sk_dub        = %(has_sk_dub)s,
            prehrajto_has_sk_subs       = %(has_sk_subs)s
        WHERE id = %(film_id)s
        """

        BATCH = 500
        batch_rows: list[dict] = []

        def flush():
            nonlocal inserted, batch_rows
            if not batch_rows:
                return
            psycopg2.extras.execute_batch(cur, upsert_sql, batch_rows, page_size=200)
            inserted += len(batch_rows)
            batch_rows = []

        commit_every = 0 if args.dry_run else args.commit_every

        t1 = time.time()
        total_films = len(film_uploads)
        for i, (film_id, uploads) in enumerate(film_uploads.items(), 1):
            # Dedup uploads by upload_id (same upload might appear in multiple
            # cluster keys if the clustering overlapped).
            seen_ids: set[str] = set()
            per_upload: list[dict] = []
            for u in uploads:
                upload_id = extract_upload_id(u["url"])
                if not upload_id:
                    skipped_no_upload_id += 1
                    continue
                if upload_id in seen_ids:
                    continue
                seen_ids.add(upload_id)
                lang = detect_lang(u["title"])
                res = extract_resolution(u["title"])
                per_upload.append({
                    "film_id": film_id,
                    "upload_id": upload_id,
                    "url": u["url"],
                    "title": u["title"],
                    "duration_sec": u["duration"] or None,
                    "view_count": u["views"] or None,
                    "lang_class": lang,
                    "resolution_hint": res,
                    "_rank": rank(lang, res, u["views"]),
                })

            if not per_upload:
                films_with_no_upload_id += 1
                continue

            per_upload.sort(key=lambda d: -d["_rank"])
            primary_upload_id = per_upload[0]["upload_id"]
            has_cz_audio = any(u["lang_class"] in ("CZ_DUB", "CZ_NATIVE") for u in per_upload)
            has_cz_subs = any(u["lang_class"] == "CZ_SUB" for u in per_upload)
            has_sk_dub = any(u["lang_class"] == "SK_DUB" for u in per_upload)
            has_sk_subs = any(u["lang_class"] == "SK_SUB" for u in per_upload)

            for u in per_upload:
                batch_rows.append({
                    k: v for k, v in u.items() if not k.startswith("_")
                })
                seen_per_film[film_id].add(u["upload_id"])
            if len(batch_rows) >= BATCH:
                flush()

            cur.execute(update_film_sql, {
                "primary": primary_upload_id,
                "has_cz_audio": has_cz_audio,
                "has_cz_subs": has_cz_subs,
                "has_sk_dub": has_sk_dub,
                "has_sk_subs": has_sk_subs,
                "film_id": film_id,
            })
            updated_flags += 1

            # Dual-write into the unified video_sources schema (#607 / #610).
            # Runs inside the caller's transaction, right after the legacy
            # batch flush path, so video_sources stays in lock-step with
            # film_prehrajto_uploads. `primary_upload_id` was just written to
            # `films.prehrajto_primary_upload_id`; passing it here makes the
            # same upload's video_sources row `is_primary=TRUE`.
            providers = get_provider_ids(cur)
            for upload in per_upload:
                dual_write_prehrajto_upload(
                    cur,
                    providers=providers,
                    film_id=film_id,
                    upload_row=upload,
                    primary_upload_id=primary_upload_id,
                )

            if commit_every and i % commit_every == 0:
                flush()
                conn.commit()

            if i % 2000 == 0:
                rate = i / (time.time() - t1)
                print(f"  [{i:>6}/{total_films}]  uploads={inserted}  rate={rate:.0f}/s", flush=True)

        flush()
        print(f"\nImported: {inserted:,} upload rows across {updated_flags:,} films")
        if skipped_no_upload_id:
            print(f"  (skipped {skipped_no_upload_id} entries without recognizable upload_id)")
        if films_with_no_upload_id:
            print(f"  ({films_with_no_upload_id} films had zero parseable uploads)")

        # ---- #644: mark dead uploads (sitemap diff) ----
        # For every film_id we touched, flip is_alive=FALSE on rows whose
        # upload_id wasn't in this run's sitemap. Both the legacy
        # `film_prehrajto_uploads` table and the unified `video_sources` rows
        # for that film + provider=prehrajto get the same treatment so the
        # two stay consistent (they're written together in the upsert path).
        if args.no_mark_dead:
            print("Skipping mark-dead sweep (--no-mark-dead).")
        elif not seen_per_film:
            print("Mark-dead skipped: no films touched this run.")
        else:
            print(f"\nMark-dead sweep across {len(seen_per_film):,} films...")
            mark_dead_legacy_total = 0
            mark_dead_vs_total = 0
            t_md = time.time()
            mark_dead_legacy_sql = """
                UPDATE film_prehrajto_uploads
                   SET is_alive = FALSE
                 WHERE film_id = %s
                   AND is_alive = TRUE
                   AND NOT (upload_id = ANY(%s))
            """
            mark_dead_vs_sql = """
                UPDATE video_sources vs
                   SET is_alive = FALSE,
                       last_checked = now()
                  FROM video_providers p
                 WHERE vs.provider_id = p.id
                   AND p.slug = 'prehrajto'
                   AND vs.film_id = %s
                   AND vs.is_alive = TRUE
                   AND NOT (vs.external_id = ANY(%s))
            """
            for film_id, seen_ids in seen_per_film.items():
                ids_arr = list(seen_ids)
                cur.execute(mark_dead_legacy_sql, (film_id, ids_arr))
                mark_dead_legacy_total += cur.rowcount
                cur.execute(mark_dead_vs_sql, (film_id, ids_arr))
                mark_dead_vs_total += cur.rowcount
            print(f"  legacy film_prehrajto_uploads → {mark_dead_legacy_total:,} rows flagged dead")
            print(f"  unified video_sources         → {mark_dead_vs_total:,} rows flagged dead")
            print(f"  ({time.time()-t_md:.1f}s)")

        # ---- Invariant: films count unchanged ----
        # In live mode with batched commits, earlier batches are already committed;
        # an invariant violation here can't fully revert them, but it still flags
        # an anomaly (external DELETE/INSERT on films during the run, or a bug).
        cur.execute("SELECT COUNT(*) FROM films")
        films_count_after = cur.fetchone()[0]
        if films_count_after != films_count_before:
            print(f"FATAL: films count changed {films_count_before} → {films_count_after}",
                  file=sys.stderr)
            if args.dry_run or not commit_every:
                conn.rollback()
            return 3
        print(f"films count invariant OK: {films_count_before:,} == {films_count_after:,}")

        if args.dry_run:
            print("Dry-run: ROLLBACK")
            conn.rollback()
        else:
            conn.commit()
            print("COMMIT")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
