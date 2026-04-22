#!/usr/bin/env python3
"""Bulk-import NEW films + their sledujteto uploads (issue #545).

Reads the four prepared data files from the sledujteto crawler pipeline
(see scripts/sledujteto-*.py + issue #599):

  data/sledujteto/sledujteto-films-candidates-sources-usable-<date>.jsonl
      one film per line: `{tmdb_id, tmdb_title, tmdb_release_date,
      sources: [{upload_id, slug_id, full_url, name, filesize,
      resolution, duration_seconds, cdn_type, ...}]}`

  data/sledujteto/sledujteto-films-candidates-audio-usable-<date>.jsonl
      one upload per line: `{upload_id, audio_language, cdn, video_url,
      duration_seconds, ...}` — whisper language detection on a sample
      of the actual audio track.

  data/sledujteto/sledujteto-films-tmdb-overviews-<date>.jsonl
      one film per line: TMDB metadata (`title_cs`, `overview_cs`,
      `original_title`, `poster_path`, `runtime`, `release_date`,
      `original_language`, ...) — used to populate the `films` row.

  data/sledujteto/sledujteto-films-gemma-overviews-<date>.jsonl
      one film per line: `{tmdb_id, title, description_cs, model}` —
      Czech unique overview rewritten via local Gemma-3-27B, preferred
      over raw TMDB `overview_cs` for SEO.

For every tmdb_id present in the sources file:
  1. Pre-lookup `SELECT id FROM films WHERE tmdb_id = %s` — if a row
     exists (possibly imported earlier by another source like prehrajto),
     reuse it; otherwise `INSERT INTO films ...` with metadata from the
     TMDB overview file and description from Gemma (fallback to TMDB
     `overview_cs`). `films.tmdb_id` is not UNIQUE at the schema level
     (there is no UNIQUE index), so we cannot rely on `ON CONFLICT
     (tmdb_id)` — the pre-lookup is what enforces "one film per tmdb_id"
     within this import.
  2. `INSERT ... ON CONFLICT (film_id, file_id) DO UPDATE` into
     `film_sledujteto_uploads` for every upload, merging title-regex +
     whisper audio-language hints into `lang_class`.
  3. `UPDATE films SET sledujteto_* = …` rollups (has_dub / has_subs /
     has_sk_dub / has_sk_subs / primary_file_id) from the film's alive
     uploads.

Safety guarantees (matches scripts/import-prehrajto-new-films.py):
  * Never DELETE — this script only inserts and UPDATE-rolls.
  * Pre-lookup on `tmdb_id` detects existing rows; duplicates (multiple
     rows with the same `tmdb_id`) are treated as a hard error so the
     importer does not silently attach uploads to an arbitrary film.
  * `--dry-run` wraps the entire transaction in ROLLBACK.
  * Row-count monotonicity is asserted at the end.

Usage:
  DATABASE_URL=postgres://... \\
      python3 scripts/import-sledujteto-films.py \\
          --sources data/sledujteto/sledujteto-films-candidates-sources-usable-2026-04-21.jsonl \\
          --audio   data/sledujteto/sledujteto-films-candidates-audio-usable-2026-04-21.jsonl \\
          --tmdb    data/sledujteto/sledujteto-films-tmdb-overviews-2026-04-22.jsonl \\
          --gemma   data/sledujteto/sledujteto-films-gemma-overviews-2026-04-22.jsonl \\
          --limit 10 \\
          --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)

log = logging.getLogger("import-sledujteto-films")


# ---------------------------------------------------------------------------
# Language classification (vendored + extended from import-prehrajto-new-films)
# ---------------------------------------------------------------------------

CZ_DIACRITICS = set("ěščřžýáíéúůťďňôäľĺŕ")
CZ_WORDS = {
    "film", "dabing", "film", "ceska", "cesky", "ceska", "dobry", "zivot",
    "jeden", "dva", "tri", "ctyri", "pet", "lásky", "lasky", "pribeh",
    "pribeh", "a", "i", "s", "z", "ze", "k", "ke", "u", "na", "po",
}
CZ_DUB_RE = re.compile(
    r"(?:\bcz\s*dab(?:ing)?\b|\bczdab\w*|\bczdub\w*|"
    r"\bcesk[aáyý]\s*dab(?:ing)?\b|\bc[zs]\s*dabing\b|"
    r"cesky\s*dabing|cz\s*\.dab\b)",
    re.IGNORECASE,
)
CZ_SUB_RE = re.compile(
    r"(?:\bcz\s*tit(?:ulky)?\b|\bcztit\w*|\bcz\s*subs?\b|"
    r"\bc[zs]\s*titulky\b|cesk[yé]\s*titulky)",
    re.IGNORECASE,
)
SK_DUB_RE = re.compile(
    r"(?:\bsk\s*dab(?:ing)?\b|\bskdab\w*|\bskdub\w*|"
    r"\bsloven(?:sk[yáé]|ina)\s*dab(?:ing)?\b)",
    re.IGNORECASE,
)
SK_SUB_RE = re.compile(r"(?:\bsk\s*tit(?:ulky)?\b|\bsktit\w*)", re.IGNORECASE)
EN_ONLY_RE = re.compile(
    r"(?:\bengsub\b|\beng\s*sub\b|\beng\s*only\b|\bengdub\b)", re.IGNORECASE
)


def detect_lang_from_title(title: str) -> str:
    """Classify upload language from the uploader-supplied title string."""
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


def merge_lang_class(title: str, audio_lang: str | None, orig_lang: str | None) -> str:
    """Merge title-regex detection with whisper audio-lang detection.

    Title regex wins when it finds an explicit CZ_DUB / SK_DUB / CZ_SUB /
    SK_SUB / EN marker — uploaders consistently tag those. When the title
    is ambiguous, whisper's audio-language detection is the tiebreaker:
    audio `cs` → CZ_NATIVE (Czech-original) if the film's `original_language`
    is also Czech, else CZ_DUB (dubbed into Czech). Audio `en` → EN.
    """
    title_class = detect_lang_from_title(title)
    if title_class not in ("UNKNOWN",):
        return title_class

    if audio_lang == "cs":
        return "CZ_NATIVE" if orig_lang == "cs" else "CZ_DUB"
    if audio_lang == "sk":
        return "SK_DUB"
    if audio_lang == "en":
        return "EN"

    return "UNKNOWN"


# ---------------------------------------------------------------------------
# Primary-upload scoring (mirrors prehrajto + adds `cdn` preference)
# ---------------------------------------------------------------------------

LANG_PRIORITY = {
    "CZ_DUB": 6, "CZ_NATIVE": 5, "CZ_SUB": 4,
    "SK_DUB": 3, "SK_SUB": 2, "UNKNOWN": 1, "EN": 0,
}

RES_SCORE = {
    "4k": 7, "2160p": 6, "1080p": 5, "720p": 4, "480p": 2,
}


def resolution_score(resolution_hint: str | None) -> int:
    if not resolution_hint:
        return 0
    r = resolution_hint.lower().replace("*", "x").replace(" ", "")
    if "3840x2160" in r or "2160" in r or "4k" in r:
        return RES_SCORE["2160p"]
    if "1920x1080" in r or "1080" in r:
        return RES_SCORE["1080p"]
    if "1280x720" in r or "720" in r:
        return RES_SCORE["720p"]
    if "854x480" in r or "480" in r:
        return RES_SCORE["480p"]
    return 0


def rank_upload(upload: dict) -> tuple[int, int, int]:
    """Higher is better. Order: (cdn=www, lang priority, resolution)."""
    cdn_score = 2 if upload["cdn"] == "www" else 0
    lang_score = LANG_PRIORITY.get(upload["lang_class"], 0)
    res_score = resolution_score(upload.get("resolution_hint"))
    return (cdn_score, lang_score, res_score)


# ---------------------------------------------------------------------------
# Filesize parsing
# ---------------------------------------------------------------------------

_SIZE_RE = re.compile(r"^([0-9]+(?:[.,][0-9]+)?)\s*([KMG]?B)$", re.IGNORECASE)


def parse_filesize(s: str | None) -> int | None:
    """Parse sledujteto filesize strings like `5.36 GB`, `742.1 MB` → bytes."""
    if not s:
        return None
    m = _SIZE_RE.match(s.strip())
    if not m:
        return None
    value = float(m.group(1).replace(",", "."))
    unit = m.group(2).upper()
    multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}
    return int(value * multipliers.get(unit, 1))


# ---------------------------------------------------------------------------
# Resolution normalization (`1920*1080` → `1920x1080`)
# ---------------------------------------------------------------------------


def normalize_resolution(r: str | None) -> str | None:
    if not r:
        return None
    return r.replace("*", "x").strip()


# ---------------------------------------------------------------------------
# Slug generation (vendored from import-prehrajto-new-films)
# ---------------------------------------------------------------------------


def slugify(text: str) -> str:
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def unique_slug(cur, base: str, year: int | None, reserved: set[str]) -> str:
    def free(candidate: str) -> bool:
        if candidate in reserved:
            return False
        cur.execute("SELECT 1 FROM films WHERE slug = %s", (candidate,))
        return cur.fetchone() is None

    if not base:
        base = "film"
    if free(base):
        return base
    if year:
        candidate = f"{base}-{year}"
        if free(candidate):
            return candidate
    n = 2
    while True:
        candidate = f"{base}-{n}"
        if free(candidate):
            return candidate
        n += 1


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_sources(path: Path) -> dict[int, dict]:
    """Return {tmdb_id: {tmdb_title, tmdb_release_date, sources: [...]}}."""
    out: dict[int, dict] = {}
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            row = json.loads(line)
            out[int(row["tmdb_id"])] = row
    return out


def load_audio(path: Path) -> dict[int, dict]:
    """Return {upload_id: {audio_language, cdn, duration_seconds, ...}}."""
    out: dict[int, dict] = {}
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            row = json.loads(line)
            out[int(row["upload_id"])] = row
    return out


def load_jsonl_by_tmdb(path: Path) -> dict[int, dict]:
    """Generic by-tmdb_id loader for TMDB / Gemma overviews."""
    out: dict[int, dict] = {}
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            row = json.loads(line)
            out[int(row["tmdb_id"])] = row
    return out


# ---------------------------------------------------------------------------
# Core import
# ---------------------------------------------------------------------------

INSERT_FILM_SQL = """
INSERT INTO films (
    title, original_title, slug, year, description,
    tmdb_id, runtime_min, tmdb_poster_path, lang,
    created_at, added_at
) VALUES (
    %(title)s, %(original_title)s, %(slug)s, %(year)s, %(description)s,
    %(tmdb_id)s, %(runtime_min)s, %(tmdb_poster_path)s, %(lang)s,
    NOW(), NOW()
)
RETURNING id
"""

UPSERT_UPLOAD_SQL = """
INSERT INTO film_sledujteto_uploads (
    film_id, file_id, title, duration_sec, resolution_hint,
    filesize_bytes, lang_class, cdn, is_alive, last_seen, last_checked
) VALUES (
    %(film_id)s, %(file_id)s, %(title)s, %(duration_sec)s, %(resolution_hint)s,
    %(filesize_bytes)s, %(lang_class)s, %(cdn)s, TRUE, NOW(), NOW()
)
ON CONFLICT (film_id, file_id) DO UPDATE SET
    title           = EXCLUDED.title,
    duration_sec    = EXCLUDED.duration_sec,
    resolution_hint = EXCLUDED.resolution_hint,
    filesize_bytes  = EXCLUDED.filesize_bytes,
    lang_class      = EXCLUDED.lang_class,
    cdn             = EXCLUDED.cdn,
    is_alive        = TRUE,
    last_seen       = NOW(),
    last_checked    = NOW(),
    updated_at      = NOW()
"""

UPDATE_FILM_ROLLUPS_SQL = """
UPDATE films SET
    sledujteto_primary_file_id = %(primary)s,
    sledujteto_has_dub         = %(has_dub)s,
    sledujteto_has_subs        = %(has_subs)s,
    sledujteto_has_sk_dub      = %(has_sk_dub)s,
    sledujteto_has_sk_subs     = %(has_sk_subs)s
WHERE id = %(film_id)s
"""


def cdn_from_sources(source: dict, audio_row: dict | None) -> str:
    """Pick the CDN classifier. Audio detection wins if it was run,
    because it reads `video_url` (the actual playback host) — the source
    `cdn_type` is derived from the preview thumbnail host and is an
    approximation."""
    if audio_row and audio_row.get("cdn"):
        return audio_row["cdn"]
    return source.get("cdn_type", "unknown") or "unknown"


def build_upload_rows(
    film_id: int,
    sources: list[dict],
    audio_by_upload: dict[int, dict],
    orig_lang: str | None,
) -> list[dict]:
    rows = []
    for src in sources:
        # Some source entries are crawler error markers (missing upload_id,
        # populated `error` field) — e.g. uploads the search pass failed to
        # reconcile against. Skip them; the corresponding film still gets
        # whatever other uploads resolved cleanly.
        if "upload_id" not in src:
            log.debug(
                "film_id=%d: skipping source without upload_id (error=%r, slug=%r)",
                film_id, src.get("error"), src.get("slug_id"),
            )
            continue
        upload_id = int(src["upload_id"])
        audio = audio_by_upload.get(upload_id)
        title = src.get("name") or ""
        lang_class = merge_lang_class(
            title=title,
            audio_lang=(audio or {}).get("audio_language"),
            orig_lang=orig_lang,
        )
        rows.append({
            "film_id": film_id,
            "file_id": upload_id,
            "title": title,
            "duration_sec": (
                int(src["duration_seconds"])
                if src.get("duration_seconds") is not None
                else None
            ),
            "resolution_hint": normalize_resolution(src.get("resolution")),
            "filesize_bytes": parse_filesize(src.get("filesize")),
            "lang_class": lang_class,
            "cdn": cdn_from_sources(src, audio),
        })
    return rows


def pick_primary_and_rollups(upload_rows: list[dict]) -> dict:
    """Compute `films.sledujteto_*` rollups from a film's upload rows."""
    if not upload_rows:
        return {
            "primary": None,
            "has_dub": False,
            "has_subs": False,
            "has_sk_dub": False,
            "has_sk_subs": False,
        }
    best = max(upload_rows, key=rank_upload)
    return {
        "primary": best["file_id"],
        "has_dub": any(u["lang_class"] in ("CZ_DUB", "CZ_NATIVE") for u in upload_rows),
        "has_subs": any(u["lang_class"] == "CZ_SUB" for u in upload_rows),
        "has_sk_dub": any(u["lang_class"] == "SK_DUB" for u in upload_rows),
        "has_sk_subs": any(u["lang_class"] == "SK_SUB" for u in upload_rows),
    }


def build_film_row(
    tmdb_id: int,
    tmdb_meta: dict | None,
    gemma_meta: dict | None,
    slug: str,
    src_row: dict | None = None,
) -> dict:
    """Shape a film row from TMDB + Gemma data. Description falls back to
    TMDB `overview_cs` (then `overview_en`) when Gemma is missing. When
    TMDB metadata is absent, title/year fall back to the sources file
    (`tmdb_title`, `tmdb_release_date`) rather than a literal
    "Unknown" — those fields are populated by the crawler for every row."""
    src = src_row or {}
    title = (
        (tmdb_meta or {}).get("title_cs")
        or (tmdb_meta or {}).get("original_title")
        or src.get("tmdb_title")
        or f"tmdb-{tmdb_id}"
    )
    original_title = None
    t_en = (tmdb_meta or {}).get("title_en")
    orig = (tmdb_meta or {}).get("original_title")
    if t_en and t_en != title:
        original_title = t_en
    elif orig and orig != title:
        original_title = orig

    description = (
        (gemma_meta or {}).get("description_cs")
        or (tmdb_meta or {}).get("overview_cs")
        or (tmdb_meta or {}).get("overview_en")
    )

    release_date = (
        (tmdb_meta or {}).get("release_date")
        or src.get("tmdb_release_date")
    )
    year = int(release_date[:4]) if release_date and len(release_date) >= 4 else None

    return {
        "title": title[:255],
        "original_title": (original_title or "")[:255] or None,
        "slug": slug,
        "year": year,
        "description": description,
        "tmdb_id": tmdb_id,
        "runtime_min": (tmdb_meta or {}).get("runtime"),
        "tmdb_poster_path": ((tmdb_meta or {}).get("poster_path") or "")[:64] or None,
        "lang": ((tmdb_meta or {}).get("original_language") or "")[:20] or None,
    }


def run_import(args: argparse.Namespace) -> int:
    sources_by_tmdb = load_sources(Path(args.sources))
    audio_by_upload = load_audio(Path(args.audio))
    tmdb_by_id = load_jsonl_by_tmdb(Path(args.tmdb))
    gemma_by_id = load_jsonl_by_tmdb(Path(args.gemma))

    log.info(
        "loaded: %d films (sources), %d audio entries, %d tmdb, %d gemma",
        len(sources_by_tmdb), len(audio_by_upload),
        len(tmdb_by_id), len(gemma_by_id),
    )

    tmdb_ids = sorted(sources_by_tmdb.keys())
    if args.limit:
        tmdb_ids = tmdb_ids[: args.limit]
        log.info("--limit %d → importing %d films", args.limit, len(tmdb_ids))

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL env var is required")
        return 2

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM films")
    films_before = cur.fetchone()[0]
    log.info("films before: %d", films_before)

    stats = defaultdict(int)
    reserved_slugs: set[str] = set()

    try:
        for tmdb_id in tmdb_ids:
            src_row = sources_by_tmdb[tmdb_id]
            tmdb_meta = tmdb_by_id.get(tmdb_id)
            gemma_meta = gemma_by_id.get(tmdb_id)
            orig_lang = (tmdb_meta or {}).get("original_language")

            cur.execute(
                "SELECT id FROM films WHERE tmdb_id = %s ORDER BY id",
                (tmdb_id,),
            )
            existing_rows = cur.fetchall()
            if len(existing_rows) > 1:
                raise RuntimeError(
                    f"duplicate films rows for tmdb_id={tmdb_id}: "
                    f"{', '.join(str(row[0]) for row in existing_rows)}"
                )
            if existing_rows:
                film_id = existing_rows[0][0]
                stats["existing_films"] += 1
            else:
                title = (
                    (tmdb_meta or {}).get("title_cs")
                    or (tmdb_meta or {}).get("original_title")
                    or src_row.get("tmdb_title")
                    or f"tmdb-{tmdb_id}"
                )
                release_date = (tmdb_meta or {}).get("release_date") or src_row.get(
                    "tmdb_release_date"
                )
                year = (
                    int(release_date[:4])
                    if release_date and len(release_date) >= 4
                    else None
                )
                base = slugify(title)
                slug = unique_slug(cur, base, year, reserved_slugs)
                reserved_slugs.add(slug)

                film_row = build_film_row(
                    tmdb_id, tmdb_meta, gemma_meta, slug, src_row=src_row
                )
                cur.execute(INSERT_FILM_SQL, film_row)
                film_id = cur.fetchone()[0]
                stats["inserted_films"] += 1

            upload_rows = build_upload_rows(
                film_id=film_id,
                sources=src_row.get("sources") or [],
                audio_by_upload=audio_by_upload,
                orig_lang=orig_lang,
            )
            psycopg2.extras.execute_batch(cur, UPSERT_UPLOAD_SQL, upload_rows)
            stats["upserted_uploads"] += len(upload_rows)

            rollups = pick_primary_and_rollups(upload_rows)
            cur.execute(UPDATE_FILM_ROLLUPS_SQL, {**rollups, "film_id": film_id})

            if stats["inserted_films"] + stats["existing_films"] >= 1 and (
                (stats["inserted_films"] + stats["existing_films"]) % args.commit_every == 0
            ):
                if args.dry_run:
                    log.info("[dry-run] would commit batch")
                else:
                    conn.commit()
                    log.info(
                        "committed batch: inserted=%d existing=%d uploads=%d",
                        stats["inserted_films"],
                        stats["existing_films"],
                        stats["upserted_uploads"],
                    )

        cur.execute("SELECT COUNT(*) FROM films")
        films_after = cur.fetchone()[0]
        log.info(
            "films after: %d (delta=%d, expected≤%d)",
            films_after,
            films_after - films_before,
            stats["inserted_films"],
        )
        if films_after - films_before > stats["inserted_films"]:
            raise RuntimeError(
                f"row-count invariant violated: after-before={films_after - films_before} > "
                f"inserted={stats['inserted_films']}"
            )

        if args.dry_run:
            log.info("--dry-run: ROLLBACK")
            conn.rollback()
        else:
            conn.commit()

        log.info(
            "done: inserted_films=%d existing_films=%d upserted_uploads=%d",
            stats["inserted_films"],
            stats["existing_films"],
            stats["upserted_uploads"],
        )
        return 0
    except Exception:
        conn.rollback()
        log.exception("import failed — rolled back")
        return 1
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sources", required=True, help="sources-usable JSONL")
    ap.add_argument("--audio", required=True, help="audio-usable JSONL")
    ap.add_argument("--tmdb", required=True, help="tmdb-overviews JSONL")
    ap.add_argument("--gemma", required=True, help="gemma-overviews JSONL")
    ap.add_argument("--limit", type=int, default=None, help="cap on films processed")
    ap.add_argument(
        "--commit-every",
        type=int,
        default=50,
        help="commit every N films (default 50; ignored with --dry-run)",
    )
    ap.add_argument("--dry-run", action="store_true", help="ROLLBACK at end")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    return run_import(args)


if __name__ == "__main__":
    sys.exit(main())
