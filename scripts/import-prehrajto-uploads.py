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
    # Decompose diacritics here so the ASCII-only `\b...\b` keyword
    # patterns below match Czech release-marker words too. Without this
    # step, "Český Film" stays in the core as "ceskyfilm" because
    # `re.IGNORECASE` doesn't lower diacritics — `\bcesky\b` never
    # matches "Český". The final `normalize()` call would have stripped
    # diacritics anyway, so this is a no-op for output shape, just lets
    # the keyword filter actually fire on diacritic-bearing tokens.
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    for g in (
        r"c(?:z|s)\s*dabing", r"s(?:k|l)\s*dabing",
        r"c(?:z|s)\s*tit(?:ulky)?", r"s(?:k|l)\s*tit(?:ulky)?",
        r"c(?:z|s)\s*dab", r"s(?:k|l)\s*dab",
        r"cztit", r"cesky\s*dabing", r"dabing", r"dabovane",
    ):
        t = re.sub(g, " ", t, flags=re.IGNORECASE)
    t = re.sub(
        r"\b(cz|sk|en|cesky|slovensky|titulky|tit|subs?|dub|dab|eng|"
        r"hd|fhd|full\s*hd|1080p?|720p?|480p|360p|4k|2160p|uhd|hdr10?\+?|"
        r"webrip|web\.?dl|web|bluray|brrip|bdrip|dvdrip|hdtv|tvrip|hd\s*rip|"
        r"dvd\s*rip|hdcam|telesync|telecine|trailer|"
        r"x264|x265|h\.?264|h\.?265|hevc|xvid|divx|"
        r"aac|ac3|dts(?:\.?hd)?|ddp?(?:5\.1|7\.1)?|truehd|atmos|5\.1(?:ch)?|7\.1(?:ch)?|"
        r"avi|mkv|mp4|m4v|3gp|"
        r"cely\s*film|cely|remastered|extended|uncut|unrated|directors?\s*cut|imax|"
        r"novinka|top\s*hit|hit|novinka|premiera|"
        r"3d|2d|"
        r"v\s*obraze|vobraze|vobratu|vyborna|"
        r"romant\.?|drama|horor|thriller|akc\.?|komedie|sci[-.]?fi|fantasy|rodinny|"
        r"muzikal|p\.?p\.?|valec\.?|dobrodruzny|animovany|animovane|anim\.?|"
        r"krimi|sportovni|koko|povidky|cd\.?\d*|"
        r"msdos|s1lv3r|chd(?:rip)?|baf|sparks|fgt|rarbg)\b",
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
                # Canonicalize to prehraj.to. The XML sitemap publishes URLs
                # under the `prehrajto.cz` mirror, but the CZ proxy
                # (chobotnice) validates against the canonical `prehraj.to`
                # host and rejects `prehrajto.cz` with
                # "Missing or invalid prehraj.to URL". Storing the canonical
                # form keeps the resolver's `action=video` calls working
                # without an extra rewrite step on every request.
                raw_loc = _unescape(loc_m.group(1))
                canonical = raw_loc.replace("https://prehrajto.cz/", "https://prehraj.to/", 1)
                yield {
                    "url": canonical,
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


# Separators that uploaders use between localized and original (or
# alternate) titles: " - Project Hail Mary", "/Project Hail Mary",
# " | Posledná šanca", " : Spasitel", "Title:Original", "Title -Original".
# Allow whitespace on only one side for "-", "/", "|" and optional
# spacing around ":" — but require at least one whitespace adjacency for
# the dash/slash/pipe forms so we don't split inside hyphenated words
# like "Spider-Man" or path-shaped tokens.
_TITLE_SEPARATOR_RE = re.compile(r"(?:\s+[-/|]\s*|\s*[-/|]\s+|\s*:\s*)")


# Roman ↔ arabic numerals are interchangeable on uploader-typed titles
# ("Rambo III" vs "Rambo 3", "Mizerové II" vs "Mizerové 2"). Films-side
# titles tend to favour roman for older releases and arabic for newer
# ones, but uploader titles use either. Both sides emit BOTH variants
# at index time so a single normalize-and-compare suffices.
_ROMAN_TO_ARABIC = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5,
                    "vi": 6, "vii": 7, "viii": 8, "ix": 9, "x": 10}
_ARABIC_TO_ROMAN = {v: k for k, v in _ROMAN_TO_ARABIC.items()}


def _digit_variants(core: str) -> list[str]:
    """Return alternate forms of `core` differing only in numeric notation.

    Specifically:
      - trailing arabic digit `[1-9]` → equivalent roman (`shrek1` → `shreki`)
      - trailing roman numeral → equivalent arabic (`ramboiii` → `rambo3`)
      - trailing single-digit `1` stripped (`shrek1` → `shrek`) — uploaders
        commonly add "1" to series-opener titles ("Spiderman 1", "Shrek 1",
        "Insidious 1") to disambiguate from sequels; the films table stores
        them under the bare base. We do NOT strip 2-9: dropping a digit
        would conflate sequels with each other (e.g., "Bastardi 3" must not
        match "Bastardi 2"). The year+duration anchor on the matching key
        protects "1" specifically because the opener has its own year.
    """
    out = [core]
    if not core:
        return out
    # Trailing arabic digit
    m = re.search(r"([1-9])$", core)
    if m:
        d = int(m.group(1))
        if d in _ARABIC_TO_ROMAN:
            out.append(core[:m.start()] + _ARABIC_TO_ROMAN[d])
        if d == 1 and len(core) > 1:
            out.append(core[:m.start()])
    # Trailing roman numeral (greedy — try longest first)
    for r in sorted(_ROMAN_TO_ARABIC.keys(), key=len, reverse=True):
        if core.endswith(r) and len(core) > len(r):
            out.append(core[:-len(r)] + str(_ROMAN_TO_ARABIC[r]))
            break
    return out


def cluster_key_candidates(row: dict) -> list[tuple]:
    """Return all plausible cluster keys for a sitemap row (#654).

    Uploaders combine the localized title with the original / alternate
    title in many forms — "Spasitel - Project Hail Mary HD CZ DABING",
    "Spasitel sci-fi-drama USA Ryan Gosling cztit", etc. The strict
    full-string normalization in `cluster_key` only matches when the
    upload's title is essentially just the film's title; for everything
    else we need to surface the underlying canonical name(s).

    We try (in order):
      1. The full normalized core (current behavior).
      2. Each segment after splitting on `" - " | " / " | " : " | " | "`
         separators (and one-sided variants — see `_TITLE_SEPARATOR_RE`).
      3. The first whitespace-separated word — catches descriptive
         uploads like "Spasitel sci-fi-drama USA Ryan Gosling".

    Year + duration anchor unchanged across all candidates, so false
    positives stay bounded by those fields. The films-table side now
    emits both `title` and `original_title` cores, so candidate-core
    matching is effective in both directions.
    """
    title = row["title"]
    year = extract_year(title)
    dur_bucket = row["duration"] // (3 * 60)
    stripped = strip_title(title)
    candidates: list[str] = []
    seen: set[str] = set()

    def _add(s: str) -> None:
        c = normalize(s)
        if not c or c in seen:
            return
        # Emit the core PLUS its digit-form variants (`_digit_variants`).
        # This lets "Spiderman 1" → ["spiderman1", "spiderman"] match a
        # film stored as "Spider-Man" (core "spiderman"), and lets
        # "Boyka IV" → ["boykaiv", "boyka4"] match a film stored under
        # either notation.
        for variant in _digit_variants(c):
            if variant and variant not in seen:
                seen.add(variant)
                candidates.append(variant)

    _add(stripped)
    for seg in _TITLE_SEPARATOR_RE.split(stripped):
        _add(seg)
    first_word = stripped.split(" ", 1)[0] if stripped else ""
    if first_word:
        _add(first_word)
    return [(c, year, dur_bucket) for c in candidates]


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


def load_matches_from_films(cur, bucket_window: int = 3) -> dict[tuple, dict]:
    """Build the cluster_key → imdb_id map directly from the `films` table.

    Replaces the original CSV path (#646) — the pilot CSV was a one-off
    snapshot of TMDB→sitemap matching, but `films` is now the canonical
    source: every row already carries `imdb_id` + a normalized title and
    year. Using it here means cron-driven syncs don't drift from new TMDB
    imports landing in `films`.

    Returns the same shape `load_matches()` does so the downstream code
    (which expects `matches_by_key[k]["imdb_id"]`) is unchanged.

    Cluster key strategy: prehraj.to clusters use a 3-min duration
    bucket. We anchor each film at `runtime_min // 3` and emit ±`bucket_window`
    buckets to absorb minor variance between TMDB runtime and the
    sitemap's reported duration. Default `±3` (≈±9 min) covers the
    common cases: short opening/closing credits trims, TV vs theatrical
    cuts, broadcast edits, and minor re-encode rounding. Wider windows
    (≥±5) start over-matching across legitimately different cuts of
    the same franchise; narrower (≤±2) loses easily-recoverable
    legitimate matches. Films without `runtime_min` are skipped here —
    without a duration anchor we risk false-positive matches across the
    entire 60-240 min band.

    Determinism: rows are read `ORDER BY id` so collisions on the same
    `(title_core, year, bucket)` key always resolve to the lowest film
    id, and each collision is logged so the operator can investigate.
    """
    cur.execute(
        "SELECT id, title, original_title, year, imdb_id, runtime_min "
        "FROM films "
        "WHERE imdb_id IS NOT NULL AND year IS NOT NULL "
        "ORDER BY id"
    )
    matches: dict[tuple, dict] = {}
    collisions: list[tuple[tuple, int, int]] = []
    skipped_no_runtime = 0
    for film_id, title, original_title, year, imdb_id, runtime_min in cur.fetchall():
        if not title or not imdb_id:
            continue
        if runtime_min is None or runtime_min <= 0:
            skipped_no_runtime += 1
            continue
        # Emit both the localized and the original title as candidate
        # cores (#654). Spasitel (cs) ↔ Project Hail Mary (en) is the
        # canonical example — sitemap titles like
        # "Spasitel - Project Hail Mary HD CZ DABING" expand on the
        # parser side into ['spasitel', 'projecthailmary'] candidates,
        # so both forms produce a match key here.
        cores: set[str] = set()
        c1 = normalize(strip_title(title))
        if c1:
            cores.add(c1)
        if original_title and original_title.strip():
            c2 = normalize(strip_title(original_title))
            if c2:
                cores.add(c2)
        # Numeric-notation variants (Roman ↔ Arabic, trailing "1" drop).
        # Symmetrical to the cluster-side emission in
        # `cluster_key_candidates` so a single canonical comparison
        # suffices regardless of which side carries which notation.
        cores = {v for c in cores for v in _digit_variants(c)}
        if not cores:
            continue
        anchor = int(runtime_min) // 3
        for core in cores:
            for dur_bucket in range(anchor - bucket_window, anchor + bucket_window + 1):
                if dur_bucket < 0:
                    continue
                key = (core, year, dur_bucket)
                existing = matches.get(key)
                if existing is None:
                    matches[key] = {
                        "imdb_id": imdb_id,
                        "_film_id": film_id,
                        "_source": "films_table",
                    }
                elif existing["_film_id"] != film_id:
                    collisions.append((key, existing["_film_id"], film_id))
    if skipped_no_runtime:
        print(f"  load_matches_from_films: skipped {skipped_no_runtime} films "
              f"without runtime_min (would match too widely)", flush=True)
    if collisions:
        print(f"  load_matches_from_films: {len(collisions)} key collisions "
              f"(kept lowest film_id; first 5 below)", flush=True)
        for key, kept_id, dropped_id in collisions[:5]:
            print(f"    key={key} kept=film#{kept_id} dropped=film#{dropped_id}", flush=True)
    return matches


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sitemap-dir", required=True,
                    help="Directory containing video-sitemap-*.xml files")
    ap.add_argument("--matches",
                    help="Path to a pilot matches-full.csv. Mutually exclusive "
                         "with --from-films-table.")
    ap.add_argument("--from-films-table", action="store_true",
                    help="Build the cluster→imdb_id map from the films table "
                         "instead of a pilot CSV. Required for cron-driven runs "
                         "on the server where no pilot CSV exists. Mutually "
                         "exclusive with --matches.")
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

    if bool(args.matches) == bool(args.from_films_table):
        print("ERROR: pass exactly one of --matches or --from-films-table",
              file=sys.stderr)
        return 2

    sitemap_dir = Path(args.sitemap_dir)
    files = sorted(sitemap_dir.glob("video-sitemap-*.xml"),
                   key=lambda p: int(re.search(r"(\d+)", p.stem).group(1)))
    if not files:
        print(f"ERROR: no video-sitemap-*.xml files in {sitemap_dir}", file=sys.stderr)
        return 2

    # ---- Open DB early so --from-films-table can use it ----
    conn_for_matches = psycopg2.connect(dsn) if args.from_films_table else None

    # ---- Load IMDB matches ----
    # We need the wanted cluster-key set up front so we can discard sitemap rows
    # outside it as we stream, instead of materialising the full 9M-entry catalog.
    if args.matches:
        print(f"Loading matches from CSV {args.matches}...")
        matches_by_key = load_matches(Path(args.matches))
        print(f"  {len(matches_by_key):,} IMDB-matched clusters in CSV")
    else:
        print("Building matches map from films table...")
        cur_m = conn_for_matches.cursor()
        matches_by_key = load_matches_from_films(cur_m)
        cur_m.close()
        # cluster keys are duration-bucket-expanded; report distinct films
        distinct_films = len({m["imdb_id"] for m in matches_by_key.values()})
        print(f"  {distinct_films:,} films with imdb_id+year → "
              f"{len(matches_by_key):,} cluster keys (duration-expanded)")
    wanted_keys = set(matches_by_key.keys())

    # ---- Stream-parse sitemaps, clustering only rows whose key is wanted ----
    print(f"Parsing {len(files)} sitemaps from {sitemap_dir}...")
    t0 = time.time()
    clusters: dict[tuple, list[dict]] = defaultdict(list)
    # #657: registry of film-shape clusters that did NOT match any wanted
    # key. Bucketed by their first cluster_key_candidate (the most
    # specific normalized form). Each entry tracks the distinct set of
    # upload_ids seen this run so `upload_count` is a true snapshot, not
    # a cumulative counter inflated by re-seeing the same uploads.
    unmatched_clusters: dict[tuple, dict] = {}
    # When a cluster DOES match — but via a non-first candidate (e.g.
    # winner='spiderman' beats first='spiderman1') — the previous-run
    # registry row was stored under the first candidate. The resolved-
    # marking loop only sees the winning candidate, so it can't clear
    # those rows. Track first→winning candidate pairings here so we
    # can sweep the registry by the first-keys at resolved-time.
    # Maps first_key (cluster_key, year, bucket) → winning_key (same
    # shape) so we can join through `matches_by_key` later for film_id.
    matched_first_to_winner: dict[tuple, tuple] = {}
    total_entries = 0
    film_shape_count = 0
    for p in files:
        for r in parse_sitemap(p):
            total_entries += 1
            if not film_shape(r):
                continue
            film_shape_count += 1
            # Try multiple candidate cluster cores (#654). First match
            # wins, so the order in cluster_key_candidates() matters:
            # full-title core first (preserves prior behaviour for
            # exact matches), then split segments, then first-word.
            # Using a single bucket per row keeps de-duplication trivial.
            candidates = cluster_key_candidates(r)
            matched = False
            for k in candidates:
                if k in wanted_keys:
                    clusters[k].append(r)
                    matched = True
                    if candidates and candidates[0] != k:
                        matched_first_to_winner[candidates[0]] = k
                    break
            if not matched and candidates:
                # Use the first candidate as the canonical "unmatched"
                # key — same shape as wanted_keys so #652 can later
                # consult this registry with identical key arithmetic.
                uk = candidates[0]
                bucket = unmatched_clusters.get(uk)
                if bucket is None:
                    bucket = {
                        "sample_title": r["title"],
                        "sample_url": r["url"],
                        "upload_ids": set(),
                    }
                    unmatched_clusters[uk] = bucket
                uid = extract_upload_id(r["url"])
                if uid:
                    bucket["upload_ids"].add(uid)
    print(f"  {total_entries:,} total entries scanned in {time.time()-t0:.1f}s")
    print(f"  {film_shape_count:,} film-shape entries")
    print(f"  {len(clusters):,} clusters matched wanted set")
    print(f"  {len(unmatched_clusters):,} clusters did NOT match (#657 registry)")

    if conn_for_matches is not None:
        conn_for_matches.close()

    # ---- Connect + find films in DB ----
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM films")
        films_count_before = cur.fetchone()[0]
        print(f"films baseline count: {films_count_before:,}")

        # ---- #657: persist unmatched-cluster registry ----
        # UPSERT happens before the films loop so this observability data
        # lands within the same transaction as the films work — keeping
        # them coupled means a fatal error rolls both back together
        # (including dry-run, which is the desired behavior). On
        # subsequent sightings we refresh `last_seen_at` + the snapshot
        # fields, but explicitly leave `last_attempt_at`,
        # `attempt_count`, and `last_failure_reason` untouched: the
        # current importer doesn't call TMDB / try to resolve, so a
        # sitemap parse is NOT a resolution attempt and bumping those
        # fields would defeat #652's planned 7-day skip-window arithmetic
        # and overwrite TMDB-specific failure diagnostics it will set.
        # `last_failure_reason` uses COALESCE so the initial generic
        # reason persists only until #652 sets a more specific one.
        if unmatched_clusters:
            unmatched_upsert_sql = """
                INSERT INTO prehrajto_unmatched_clusters
                    (cluster_key, year, duration_bucket,
                     sample_title, sample_url, upload_count,
                     first_seen_at, last_seen_at, last_attempt_at,
                     attempt_count, last_failure_reason)
                VALUES
                    (%(cluster_key)s, %(year)s, %(duration_bucket)s,
                     %(sample_title)s, %(sample_url)s, %(upload_count)s,
                     NOW(), NOW(), NOW(), 1,
                     'no films match for cluster key (importer skip)')
                ON CONFLICT (cluster_key, year, duration_bucket) DO UPDATE SET
                    sample_title        = EXCLUDED.sample_title,
                    sample_url          = EXCLUDED.sample_url,
                    upload_count        = EXCLUDED.upload_count,
                    last_seen_at        = EXCLUDED.last_seen_at,
                    last_failure_reason = COALESCE(
                        prehrajto_unmatched_clusters.last_failure_reason,
                        EXCLUDED.last_failure_reason)
                WHERE prehrajto_unmatched_clusters.resolved_at IS NULL
            """
            unmatched_rows = [
                {
                    "cluster_key": k[0],
                    "year": k[1],
                    "duration_bucket": k[2],
                    "sample_title": v["sample_title"],
                    "sample_url": v["sample_url"],
                    "upload_count": len(v["upload_ids"]),
                }
                for k, v in unmatched_clusters.items()
            ]
            psycopg2.extras.execute_batch(
                cur, unmatched_upsert_sql, unmatched_rows, page_size=500,
            )
            print(f"  unmatched registry upserted: {len(unmatched_rows):,} rows")

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

        # `uq_fpu_upload_id` enforces upload_id uniqueness across the whole
        # table — the migration's design intent (20260508_048) is that "same
        # upload_id must not belong to two different films; on cluster-key
        # collision the importer must pick one film_id and reject the
        # other." We use `upload_id` as the conflict target (matches that
        # global unique index) and a WHERE clause so the UPDATE only fires
        # when the existing row's film_id matches — otherwise the row is
        # silently skipped, preserving the original parent. This handles
        # both same-film re-imports (the normal idempotent path) and
        # cross-film cluster collisions (defensive: keep first parent).
        upsert_sql = """
        INSERT INTO film_prehrajto_uploads
            (film_id, upload_id, url, title, duration_sec, view_count,
             lang_class, resolution_hint, last_seen_at, is_alive)
        VALUES
            (%(film_id)s, %(upload_id)s, %(url)s, %(title)s, %(duration_sec)s, %(view_count)s,
             %(lang_class)s, %(resolution_hint)s, NOW(), TRUE)
        ON CONFLICT (upload_id) DO UPDATE SET
            url             = EXCLUDED.url,
            title           = EXCLUDED.title,
            duration_sec    = EXCLUDED.duration_sec,
            view_count      = EXCLUDED.view_count,
            lang_class      = EXCLUDED.lang_class,
            resolution_hint = EXCLUDED.resolution_hint,
            last_seen_at    = EXCLUDED.last_seen_at,
            is_alive        = TRUE
        WHERE film_prehrajto_uploads.film_id = EXCLUDED.film_id
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

            # Cross-film cluster collisions: the legacy table enforces a
            # global unique on `upload_id`, so an upload already linked to
            # ANOTHER film won't be accepted here. Filter those out before
            # computing the primary pointer + rollup flags so
            # `films.prehrajto_primary_upload_id` doesn't end up pointing
            # at an upload this film doesn't actually own (Copilot review
            # on #653: rollup flags would otherwise reflect uploads owned
            # by a different film).
            ids_in_batch = [u["upload_id"] for u in per_upload]
            cur.execute(
                "SELECT upload_id, film_id FROM film_prehrajto_uploads "
                "WHERE upload_id = ANY(%s)",
                (ids_in_batch,),
            )
            owners = {row[0]: row[1] for row in cur.fetchall()}
            per_upload = [
                u for u in per_upload
                if owners.get(u["upload_id"], film_id) == film_id
            ]
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
            # The partial unique index `uq_vs_primary_film` only permits ONE
            # is_primary=TRUE row per (provider, film). When prehraj.to
            # rotates upload_ids, the new primary's external_id differs from
            # the prior winner that's still flagged primary in DB → INSERT
            # would violate the index. Demote any prior primaries for this
            # film first; the loop below then sets is_primary on exactly the
            # new winner (or none, if the chosen primary's row is updated
            # via ON CONFLICT and demoted siblings already exist).
            cur.execute(
                """
                UPDATE video_sources
                   SET is_primary = FALSE,
                       updated_at = now()
                 WHERE provider_id = %s
                   AND film_id     = %s
                   AND is_primary  = TRUE
                   AND external_id <> %s
                """,
                (providers["prehrajto"], film_id, primary_upload_id),
            )
            # Each dual_write runs inside a SAVEPOINT so a single ambiguous
            # upload (e.g. partial-unique-index conflict from a stale prior
            # row, or a cross-film cluster overlap the helper can't repair
            # in-place) doesn't abort the whole run. The savepoint pattern
            # is the standard way to recover from constraint violations
            # mid-transaction in psycopg2 (the alternative — a fresh
            # transaction per row — would lose the importer's batched
            # legacy-table upserts).
            for upload in per_upload:
                cur.execute("SAVEPOINT dw")
                try:
                    dual_write_prehrajto_upload(
                        cur,
                        providers=providers,
                        film_id=film_id,
                        upload_row=upload,
                        primary_upload_id=primary_upload_id,
                    )
                    cur.execute("RELEASE SAVEPOINT dw")
                except psycopg2.errors.UniqueViolation as e:
                    cur.execute("ROLLBACK TO SAVEPOINT dw")
                    cur.execute("RELEASE SAVEPOINT dw")
                    constraint = getattr(getattr(e, "diag", None),
                                         "constraint_name", None)
                    print(
                        f"  WARN dual_write skipped: film_id={film_id} "
                        f"upload_id={upload.get('upload_id')} "
                        f"is_primary={upload['upload_id'] == primary_upload_id} "
                        f"({constraint})",
                        flush=True,
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
            # Resolve the prehrajto provider_id once instead of joining
            # video_providers by slug on every film (#649 Copilot review).
            providers_for_md = get_provider_ids(cur)
            prehrajto_pid = providers_for_md["prehrajto"]
            mark_dead_legacy_sql = """
                UPDATE film_prehrajto_uploads
                   SET is_alive = FALSE
                 WHERE film_id = %s
                   AND is_alive = TRUE
                   AND NOT (upload_id = ANY(%s))
            """
            # Touch updated_at alongside last_checked (#649 Copilot review).
            # video_sources has no auto-update trigger on the column, so we
            # keep the timestamp aligned with other write paths (e.g. the
            # legacy resolver in prehrajto.rs) that set updated_at=now() on
            # liveness changes — otherwise rows flipped here would look
            # "older" than they actually are.
            mark_dead_vs_sql = """
                UPDATE video_sources
                   SET is_alive = FALSE,
                       last_checked = now(),
                       updated_at  = now()
                 WHERE provider_id = %s
                   AND film_id = %s
                   AND is_alive = TRUE
                   AND NOT (external_id = ANY(%s))
            """
            for film_id, seen_ids in seen_per_film.items():
                ids_arr = list(seen_ids)
                cur.execute(mark_dead_legacy_sql, (film_id, ids_arr))
                mark_dead_legacy_total += cur.rowcount
                cur.execute(mark_dead_vs_sql, (prehrajto_pid, film_id, ids_arr))
                mark_dead_vs_total += cur.rowcount
            print(f"  legacy film_prehrajto_uploads → {mark_dead_legacy_total:,} rows flagged dead")
            print(f"  unified video_sources         → {mark_dead_vs_total:,} rows flagged dead")
            print(f"  ({time.time()-t_md:.1f}s)")

        # ---- #657: mark resolved unmatched clusters ----
        # Any cluster key that DID match wanted_keys this run AND now
        # exists as a row in `films` should be flagged resolved in the
        # registry. The match goes through the same (cluster_key, year,
        # duration_bucket) triple #657 stored, so direct hits get
        # cleared. Indirect hits (upload's first candidate differs from
        # the films-side candidate that won the match — e.g. localized
        # vs. original_title) are left for a later run to clear when the
        # cluster shows up under that exact form, or for the operator
        # to clean up manually. False negatives here are harmless.
        resolved_pairs: list[dict] = []
        for key in clusters:
            match = matches_by_key.get(key)
            if not match:
                continue
            film_id = imdb_to_film_id.get(match["imdb_id"])
            if film_id is None:
                continue
            resolved_pairs.append({
                "cluster_key": key[0],
                "year": key[1],
                "duration_bucket": key[2],
                "film_id": film_id,
            })
        # Also resolve registry rows keyed by the FIRST candidate of
        # rows that matched via a later candidate. Without this, every
        # cluster whose registry-key is its first candidate but whose
        # winning candidate is something else (Spider-Man "spiderman1"→
        # "spiderman", Insidious "insidious1"→"insidious") stays
        # unresolved forever even though the importer happily writes
        # uploads for the film.
        for first_key, winning_key in matched_first_to_winner.items():
            match = matches_by_key.get(winning_key)
            if not match:
                continue
            film_id = imdb_to_film_id.get(match["imdb_id"])
            if film_id is None:
                continue
            resolved_pairs.append({
                "cluster_key": first_key[0],
                "year": first_key[1],
                "duration_bucket": first_key[2],
                "film_id": film_id,
            })
        if resolved_pairs:
            update_resolved_sql = """
                UPDATE prehrajto_unmatched_clusters
                   SET resolved_at      = NOW(),
                       resolved_film_id = %(film_id)s
                 WHERE resolved_at IS NULL
                   AND cluster_key      = %(cluster_key)s
                   AND year IS NOT DISTINCT FROM %(year)s
                   AND duration_bucket  = %(duration_bucket)s
            """
            psycopg2.extras.execute_batch(
                cur, update_resolved_sql, resolved_pairs, page_size=500,
            )
            print(f"  unmatched registry: marked-resolved attempts = "
                  f"{len(resolved_pairs):,} (rowcount may be lower — "
                  f"only previously-unresolved entries flip)")

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
