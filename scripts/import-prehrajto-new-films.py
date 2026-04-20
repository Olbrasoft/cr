#!/usr/bin/env python3
"""Bulk-import NEW films from prehraj.to sitemap pilot (issue #524).

Sibling of scripts/import-prehrajto-uploads.py (#520). Where #520 only
enriched films already present in `films`, this script INSERTs brand-new
`films` rows for IMDB-matched clusters whose imdb_id is not yet in the DB,
then attaches their prehraj.to uploads with the same logic.

Metadata comes from TMDB, never from prehraj.to upload titles:
  * `title`          = TMDB cs-CZ `.title` (fallback to `.original_title`)
  * `original_title` = TMDB en-US `.title`  (stored only when different from CZ)
  * `description`    = TMDB cs-CZ `.overview` (fallback to en-US `.overview`)
  * `year`           = `.release_date[:4]`
  * `runtime_min`    = `.runtime`
  * `cover_filename` = downloaded via auto_import.cover_downloader.download_cover
  * `generated_description` = NULL (filled later by Gemma-4 SEO job, #527)

Safety guarantees (hard-enforced at runtime):
  - Never DELETE from films, film_prehrajto_uploads, or any other table.
  - Never UPDATE existing films rows — uses `INSERT ... ON CONFLICT (imdb_id)
    DO NOTHING` so re-runs skip films someone else already inserted.
  - Row-count monotonicity: `SELECT COUNT(*) FROM films` after run ==
    before + len(candidate imdbs missing from DB) − ON-CONFLICT skips.
    Never decreases.
  - --dry-run uses a single transaction + ROLLBACK at the end.
  - Live run commits in batches (every --commit-every films) to avoid
    multi-hour transactions; invariant still fires at the end.

Usage:
  DATABASE_URL=postgres://... TMDB_API_KEY=... \\
      python3 scripts/import-prehrajto-new-films.py \\
          --sitemap-dir /tmp/prehrajto-pilot \\
          --matches /tmp/prehrajto-pilot/matches-full.csv \\
          --covers-dir data/movies/covers-webp \\
          --dry-run
"""

from __future__ import annotations

import argparse
import csv
import html
import logging
import math
import os
import re
import sys
import threading
import time
import unicodedata
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. pip install requests", file=sys.stderr)
    sys.exit(2)

# auto_import is a proper package at scripts/auto_import. Import the existing
# cover downloader to avoid reimplementing TMDB image → WebP conversion.
_SCRIPTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPTS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from scripts.auto_import.cover_downloader import download_cover  # noqa: E402
from scripts.auto_import.enricher import TMDB_MOVIE_GENRE_MAP  # noqa: E402

log = logging.getLogger("import-prehrajto-new-films")

TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_DEFAULT_TIMEOUT = 15


# ---------------------------------------------------------------------------
# Sitemap parsing + clustering (vendored from scripts/import-prehrajto-uploads.py)
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
    return html.unescape(html.unescape(s))


def parse_sitemap(path: Path, chunk_size: int = 1 << 20) -> Iterator[dict]:
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
# Language detection (vendored from scripts/import-prehrajto-uploads.py)
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
# Primary-upload scoring (vendored)
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
    return m.group(1).lower() if m else None


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
# Slug helpers (vendored from scripts/auto_import/enricher.py)
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
    """Find a free slug — check DB + in-batch reserved set.

    Mirrors enricher._unique_slug but additionally de-duplicates against
    slugs already assigned earlier in the same run (reserved), since new
    films are inserted one-by-one and a batch of two films with the same
    title would otherwise collide on the `films.slug` UNIQUE constraint.
    """
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
    counter = 2
    while True:
        candidate = f"{base}-{counter}"
        if free(candidate):
            return candidate
        counter += 1


# ---------------------------------------------------------------------------
# TMDB helper
# ---------------------------------------------------------------------------

# Shared pacing state — every tmdb_get() call honours --tmdb-min-interval-ms,
# so the effective rate is per HTTP call. Thread-safe: the prefetch phase runs
# TMDB calls from a worker pool, so we guard the last-call timestamp with a
# lock to prevent bursts that would blow past TMDB's 50 rps ceiling.
_TMDB_MIN_INTERVAL: float = 0.0
_TMDB_LAST_CALL_TS: float = 0.0
_TMDB_LOCK = threading.Lock()

# requests.Session is NOT thread-safe (connection adapter state, cookie jar,
# headers dict). Phase 1 hits TMDB from a worker pool, so give each thread
# its own session via threading.local(). Each worker reuses its session across
# multiple calls, so we keep the connection-pooling benefit that a single
# Session provides — just not cross-thread.
_THREAD_LOCAL = threading.local()


def _thread_session() -> requests.Session:
    s = getattr(_THREAD_LOCAL, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"Accept": "application/json"})
        _THREAD_LOCAL.session = s
    return s


def _tmdb_pace() -> None:
    """Under lock, sleep if needed so consecutive tmdb_get() calls across all
    threads are >= _TMDB_MIN_INTERVAL seconds apart."""
    global _TMDB_LAST_CALL_TS
    if _TMDB_MIN_INTERVAL <= 0:
        with _TMDB_LOCK:
            _TMDB_LAST_CALL_TS = time.time()
        return
    with _TMDB_LOCK:
        elapsed = time.time() - _TMDB_LAST_CALL_TS
        if elapsed < _TMDB_MIN_INTERVAL:
            time.sleep(_TMDB_MIN_INTERVAL - elapsed)
        _TMDB_LAST_CALL_TS = time.time()


def tmdb_get(path: str, params: dict,
             api_key: str, retries: int = 3) -> dict | None:
    """GET TMDB endpoint with retry on 429 / transient failure. Uses the
    current thread's own `requests.Session` for thread-safety."""
    _tmdb_pace()
    session = _thread_session()
    p = {"api_key": api_key}
    p.update(params)
    url = f"{TMDB_API_BASE}{path}"
    for attempt in range(retries):
        try:
            r = session.get(url, params=p, timeout=TMDB_DEFAULT_TIMEOUT)
        except requests.RequestException as e:
            log.warning("TMDB %s attempt %d failed: %s", path, attempt + 1, e)
            time.sleep(1 + attempt)
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            log.warning("TMDB rate-limited; sleeping %ds", wait)
            time.sleep(wait)
            continue
        if r.status_code != 200:
            log.warning("TMDB %s HTTP %d", path, r.status_code)
            return None
        try:
            return r.json()
        except ValueError:
            return None
    return None


def _cover_worker(poster_path: str, slug: str,
                  covers_dir: Path) -> tuple | None:
    """Thread-pool task for downloading a TMDB poster. Returns the WebP
    paths tuple (small, large) on success, None on failure. Logged
    failures do not raise — they're tallied from the future's result."""
    try:
        return download_cover(poster_path, slug, covers_dir)
    except Exception as e:  # noqa: BLE001 — isolate per-film failures
        log.warning("cover download raised for %s: %s", slug, e)
        return None


def fetch_tmdb_movie(tmdb_id: int, api_key: str) -> dict | None:
    """Fetch cs-CZ + en-US /movie/{id} and merge into one dict.

    Returns keys: title_cs, title_en, original_title, overview_cs, overview_en,
    year, runtime_min, poster_path, genre_ids, imdb_id. Returns None if both
    language fetches fail (usually a stale tmdb_id).
    """
    cs = tmdb_get(f"/movie/{tmdb_id}", {"language": "cs-CZ"}, api_key)
    en = tmdb_get(f"/movie/{tmdb_id}", {"language": "en-US"}, api_key)
    if not cs and not en:
        return None
    src_cs = cs or {}
    src_en = en or {}
    rd = src_cs.get("release_date") or src_en.get("release_date") or ""
    year = int(rd[:4]) if len(rd) >= 4 and rd[:4].isdigit() else None
    return {
        "tmdb_id": tmdb_id,
        "imdb_id": src_cs.get("imdb_id") or src_en.get("imdb_id") or None,
        "title_cs": (src_cs.get("title") or "").strip() or None,
        "title_en": (src_en.get("title") or "").strip() or None,
        "original_title": (src_en.get("original_title") or src_cs.get("original_title") or "").strip() or None,
        "overview_cs": (src_cs.get("overview") or "").strip() or None,
        "overview_en": (src_en.get("overview") or "").strip() or None,
        "year": year,
        "runtime_min": src_cs.get("runtime") or src_en.get("runtime") or None,
        "poster_path": src_cs.get("poster_path") or src_en.get("poster_path") or None,
        "genre_ids": [g["id"] for g in (src_cs.get("genres") or src_en.get("genres") or []) if g.get("id")],
    }


# ---------------------------------------------------------------------------
# Matches CSV loader
# ---------------------------------------------------------------------------


def load_matches(path: Path) -> dict[tuple, dict]:
    """Same shape as scripts/import-prehrajto-uploads.py: cluster_key → row."""
    matches_by_key: dict[tuple, dict] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["verdict"] not in ("MATCHED", "LIKELY"):
                continue
            if not row["imdb_id"] or not row["tmdb_id"]:
                continue
            try:
                year = int(row["cluster_year"]) if row["cluster_year"] else None
                dur_bucket = int(row["cluster_duration_min"]) // 3
            except ValueError:
                continue
            key = (row["cluster_core"], year, dur_bucket)
            matches_by_key[key] = row
    return matches_by_key


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--sitemap-dir", required=True,
                    help="Directory containing video-sitemap-*.xml files")
    ap.add_argument("--matches", required=True,
                    help="Path to matches-full.csv (from pilot)")
    ap.add_argument("--covers-dir", default="data/movies/covers-webp",
                    help="Directory to write cover WebPs into (created if missing)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Run end-to-end but ROLLBACK at the end — no writes persisted "
                         "and no cover files left behind (covers are deleted if dry-run)")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N new films (0 = all)")
    ap.add_argument("--commit-every", type=int, default=500,
                    help="In live mode, commit after every N films (default 500). "
                         "Set 0 to keep a single transaction.")
    ap.add_argument("--tmdb-min-interval-ms", type=int, default=25,
                    help="Minimum ms between TMDB HTTP calls across all workers "
                         "(default 25 = 40 rps aggregate). TMDB's own limit is "
                         "~50 rps.")
    ap.add_argument("--tmdb-workers", type=int, default=6,
                    help="Parallel threads for TMDB metadata prefetch (default 6). "
                         "Each film needs 2 calls (cs-CZ + en-US).")
    ap.add_argument("--cover-workers", type=int, default=6,
                    help="Parallel threads for cover download (default 6).")
    ap.add_argument("--skip-covers", action="store_true",
                    help="Skip cover download entirely — for DRY-RUN sanity checks "
                         "without hitting image.tmdb.org")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        log.error("DATABASE_URL env var required")
        return 2
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not api_key:
        log.error("TMDB_API_KEY env var required")
        return 2

    sitemap_dir = Path(args.sitemap_dir)
    files = sorted(sitemap_dir.glob("video-sitemap-*.xml"),
                   key=lambda p: int(re.search(r"(\d+)", p.stem).group(1)))
    if not files:
        log.error("no video-sitemap-*.xml files in %s", sitemap_dir)
        return 2

    covers_dir = Path(args.covers_dir)
    covers_dir.mkdir(parents=True, exist_ok=True)

    # ---- Load matches from pilot CSV ----
    log.info("Loading matches from %s ...", args.matches)
    matches_by_key = load_matches(Path(args.matches))
    log.info("  %d IMDB-matched clusters in CSV", len(matches_by_key))

    # ---- Stream-parse sitemaps, bucketing uploads by cluster_key ----
    log.info("Parsing %d sitemaps from %s ...", len(files), sitemap_dir)
    t0 = time.time()
    wanted_keys = set(matches_by_key.keys())
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
    log.info("  %d entries scanned in %.1fs (%d film-shape, %d clusters matched)",
             total_entries, time.time() - t0, film_shape_count, len(clusters))

    # ---- Connect, compute NEW cohort (imdb_id not in DB) ----
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    # Per-thread `requests.Session` is managed by `_thread_session()`; no main-
    # thread session to create here.
    # Hoisted out of the try block so the finally cleanup can touch them even
    # if we raise before entering the per-film loop (otherwise UnboundLocalError
    # during cleanup would mask the original exception).
    dry_run_covers_created: list[Path] = []
    cover_pool: ThreadPoolExecutor | None = None
    cover_futures: list = []
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM films")
        films_count_before = cur.fetchone()[0]
        log.info("films baseline count: %d", films_count_before)

        candidate_imdbs = sorted({m["imdb_id"] for m in matches_by_key.values()})
        cur.execute(
            "SELECT imdb_id FROM films WHERE imdb_id = ANY(%s)",
            (candidate_imdbs,),
        )
        existing_imdbs = {r[0] for r in cur.fetchall()}
        missing_imdbs = [i for i in candidate_imdbs if i not in existing_imdbs]
        log.info("  %d candidates, %d already in DB → %d NEW to import",
                 len(candidate_imdbs), len(existing_imdbs), len(missing_imdbs))

        # Build imdb_id → tmdb_id (first seen wins; multiple cluster keys can
        # point to the same IMDB).
        imdb_to_tmdb: dict[str, int] = {}
        # Build imdb_id → list of upload dicts (aggregated across all clusters
        # that resolved to that IMDB).
        imdb_to_uploads: dict[str, list[dict]] = defaultdict(list)
        for key, match in matches_by_key.items():
            imdb = match["imdb_id"]
            if imdb in existing_imdbs:
                continue
            try:
                tid = int(match["tmdb_id"])
            except ValueError:
                continue
            if imdb not in imdb_to_tmdb:
                imdb_to_tmdb[imdb] = tid
            imdb_to_uploads[imdb].extend(clusters.get(key, []))
        log.info("  %d new IMDBs with tmdb_id + uploads queue",
                 sum(1 for u in imdb_to_uploads.values() if u))

        if args.limit:
            missing_imdbs = missing_imdbs[: args.limit]
            log.info("  --limit=%d, narrowing to %d IMDBs",
                     args.limit, len(missing_imdbs))

        if not missing_imdbs:
            log.info("Nothing to do — all candidates already in DB.")
            return 0

        # ---- Genre slug → id lookup ----
        cur.execute("SELECT slug, id FROM genres")
        slug_to_genre_id = dict(cur.fetchall())

        # ---- SQL statements ----
        # INSERT film. ON CONFLICT (imdb_id) requires the partial UNIQUE index
        # added by migration 050; we always supply a non-NULL imdb_id so the
        # conflict target is well-defined.
        insert_film_sql = """
        INSERT INTO films (
            title, original_title, slug, year, description, generated_description,
            imdb_id, tmdb_id, runtime_min, cover_filename,
            imdb_rating, csfd_rating,
            sktorrent_video_id, sktorrent_cdn, sktorrent_qualities,
            has_dub, has_subtitles,
            prehrajto_url, prehrajto_has_dub, prehrajto_has_subs,
            prehrajto_primary_upload_id, prehrajto_has_sk_dub, prehrajto_has_sk_subs,
            created_at, added_at
        ) VALUES (
            %(title)s, %(original_title)s, %(slug)s, %(year)s, %(description)s, NULL,
            %(imdb_id)s, %(tmdb_id)s, %(runtime_min)s, %(cover_filename)s,
            NULL, NULL,
            NULL, NULL, NULL,
            false, false,
            NULL, %(has_cz_audio)s, %(has_cz_subs)s,
            %(primary_upload)s, %(has_sk_dub)s, %(has_sk_subs)s,
            NOW(), NOW()
        )
        ON CONFLICT (imdb_id) WHERE imdb_id IS NOT NULL DO NOTHING
        RETURNING id
        """
        insert_upload_sql = """
        INSERT INTO film_prehrajto_uploads
            (film_id, upload_id, url, title, duration_sec, view_count,
             lang_class, resolution_hint, last_seen_at, is_alive)
        VALUES
            (%(film_id)s, %(upload_id)s, %(url)s, %(title)s, %(duration_sec)s,
             %(view_count)s, %(lang_class)s, %(resolution_hint)s, NOW(), TRUE)
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
        insert_genre_sql = (
            "INSERT INTO film_genres (film_id, genre_id) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING"
        )

        # ---- Loop over missing IMDBs ----
        commit_every = 0 if args.dry_run else args.commit_every
        global _TMDB_MIN_INTERVAL
        _TMDB_MIN_INTERVAL = max(0.0, args.tmdb_min_interval_ms / 1000.0)
        inserted_films = 0
        inserted_uploads = 0
        tmdb_failures = 0
        no_uploads = 0
        no_poster = 0
        conflict_skips = 0
        slug_retries = 0
        reserved_slugs: set[str] = set()
        # Cover downloads are fired off to a pool from the main loop; we
        # collect the futures and drain them at the end so --dry-run can
        # unlink every WebP that actually made it to disk. Assigned to the
        # outer-hoisted `cover_pool` so `finally` can shut it down on an
        # exception path.
        cover_pool = ThreadPoolExecutor(
            max_workers=max(1, args.cover_workers),
            thread_name_prefix="cover",
        )

        # ---- Phase 1: Parallel TMDB prefetch ----
        # Each film needs two TMDB calls (cs-CZ + en-US). Before this split,
        # the per-film loop was single-threaded against TMDB and dominated the
        # end-to-end runtime (~0.7 s/film wall clock → ~2 h for 8.7 K films).
        # Prefetching with a thread pool + shared paced `_tmdb_pace()` moves
        # the wall-clock budget onto TMDB's own 50 rps ceiling instead of our
        # serial request/response cycle.
        #
        # Filter out IMDBs with zero parseable uploads — Phase 2 would skip
        # them as `no_uploads` anyway, so fetching their TMDB metadata burns
        # quota for nothing. In pilot data this typically drops ~10 films of
        # ~8 784, but for smaller/sparser runs the ratio can be larger.
        per_imdb_tmdb_ids: list[tuple[str, int]] = [
            (imdb, imdb_to_tmdb[imdb]) for imdb in missing_imdbs
            if imdb_to_tmdb.get(imdb) and imdb_to_uploads.get(imdb)
        ]
        log.info("Phase 1: prefetching TMDB metadata for %d films "
                 "(%d workers, throttle %.0f ms/call) ...",
                 len(per_imdb_tmdb_ids), args.tmdb_workers,
                 _TMDB_MIN_INTERVAL * 1000)
        tmdb_data: dict[str, dict] = {}
        t_prefetch = time.time()
        with ThreadPoolExecutor(
            max_workers=max(1, args.tmdb_workers),
            thread_name_prefix="tmdb",
        ) as tmdb_pool:
            futures = {
                tmdb_pool.submit(fetch_tmdb_movie, tid, api_key): imdb
                for imdb, tid in per_imdb_tmdb_ids
            }
            done = 0
            for fut in as_completed(futures):
                imdb = futures[fut]
                done += 1
                try:
                    movie = fut.result()
                except Exception as e:
                    log.warning("tmdb prefetch raised for %s: %s", imdb, e)
                    movie = None
                if movie:
                    tmdb_data[imdb] = movie
                else:
                    tmdb_failures += 1
                if done % 1000 == 0:
                    rate = done / max(0.001, time.time() - t_prefetch)
                    log.info("  prefetched %d/%d (rate=%.1f films/s, "
                             "fails=%d)", done, len(futures), rate, tmdb_failures)
        log.info("Phase 1 done in %.1fs: %d films metadata cached, %d TMDB failures",
                 time.time() - t_prefetch, len(tmdb_data), tmdb_failures)

        # ---- Phase 2: main DB loop (serial, consumes cached metadata) ----
        log.info("Phase 2: DB inserts (serial) + cover downloads async "
                 "(%d workers) ...", args.cover_workers)
        t1 = time.time()
        for i, imdb in enumerate(missing_imdbs, 1):
            movie = tmdb_data.get(imdb)
            tmdb_id = imdb_to_tmdb.get(imdb)
            if not tmdb_id:
                continue
            if not movie:
                # prefetch already logged the failure
                continue
            # Sanity: TMDB's imdb_id should match the pilot CSV's imdb_id.
            if movie["imdb_id"] and movie["imdb_id"] != imdb:
                log.warning("IMDB mismatch for tmdb_id=%s: pilot=%s tmdb=%s — skipping",
                            tmdb_id, imdb, movie["imdb_id"])
                continue

            # ---- Metadata ----
            title = movie["title_cs"] or movie["title_en"] or movie["original_title"] or "Film"
            title_en = movie["title_en"]
            original_title = title_en if title_en and title_en != title else None
            description = movie["overview_cs"] or movie["overview_en"]
            year = movie["year"]
            runtime_min = movie["runtime_min"]

            base_slug = slugify(title)
            slug = unique_slug(cur, base_slug, year, reserved_slugs)
            reserved_slugs.add(slug)

            # ---- Aggregate uploads for this IMDB ----
            seen_ids: set[str] = set()
            per_upload: list[dict] = []
            for u in imdb_to_uploads.get(imdb, []):
                upload_id = extract_upload_id(u["url"])
                if not upload_id or upload_id in seen_ids:
                    continue
                seen_ids.add(upload_id)
                lang = detect_lang(u["title"])
                res = extract_resolution(u["title"])
                per_upload.append({
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
                # No uploads attached — per issue, skip (this film wouldn't be
                # playable anyway). Counts toward `no_uploads` stat.
                no_uploads += 1
                continue

            per_upload.sort(key=lambda d: -d["_rank"])
            primary_upload_id = per_upload[0]["upload_id"]
            has_cz_audio = any(u["lang_class"] in ("CZ_DUB", "CZ_NATIVE") for u in per_upload)
            has_cz_subs = any(u["lang_class"] == "CZ_SUB" for u in per_upload)
            has_sk_dub = any(u["lang_class"] == "SK_DUB" for u in per_upload)
            has_sk_subs = any(u["lang_class"] == "SK_SUB" for u in per_upload)

            # ---- Decide cover optimistically, but only submit AFTER INSERT ----
            # `cover_filename` goes into the films row at INSERT time so the
            # detail page can render `{slug}.webp` as soon as the WebP lands.
            # The actual HTTP download is deferred until we know the FINAL
            # slug — if slug-retry kicks in mid-insert, submitting the cover
            # task too early would write `{old_slug}.webp` while the row
            # holds `{new_slug}`, leaving a permanent mismatch + orphan file.
            want_cover = bool(movie["poster_path"]) and not args.skip_covers
            cover_filename: str | None = None  # set after successful INSERT

            # ---- INSERT film (savepoint + retry on slug collision) ----
            # `unique_slug()` is a SELECT-then-INSERT check: if a concurrent
            # writer (e.g. auto-import) claims the same slug between our
            # availability probe and this INSERT, Postgres raises a UNIQUE
            # violation on `films_slug_key`. We catch it behind a savepoint,
            # regenerate a new slug, and retry — bounded so a pathological
            # case doesn't loop forever.
            MAX_SLUG_RETRIES = 3
            row = None
            for attempt in range(MAX_SLUG_RETRIES + 1):
                cur.execute("SAVEPOINT film_insert_sp")
                try:
                    cur.execute(insert_film_sql, {
                        "title": title,
                        "original_title": original_title,
                        "slug": slug,
                        "year": year,
                        "description": description,
                        "imdb_id": imdb,
                        "tmdb_id": tmdb_id,
                        "runtime_min": runtime_min,
                        "cover_filename": slug if want_cover else None,
                        "has_cz_audio": has_cz_audio,
                        "has_cz_subs": has_cz_subs,
                        "primary_upload": primary_upload_id,
                        "has_sk_dub": has_sk_dub,
                        "has_sk_subs": has_sk_subs,
                    })
                    row = cur.fetchone()
                    cur.execute("RELEASE SAVEPOINT film_insert_sp")
                    break
                except psycopg2.errors.UniqueViolation as e:
                    cur.execute("ROLLBACK TO SAVEPOINT film_insert_sp")
                    cur.execute("RELEASE SAVEPOINT film_insert_sp")
                    constraint = getattr(getattr(e, "diag", None), "constraint_name", None) or ""
                    if "slug" not in constraint:
                        # imdb_id conflict (ON CONFLICT DO NOTHING handles it
                        # as row=None) or some other unique index — re-raise.
                        raise
                    if attempt == MAX_SLUG_RETRIES:
                        log.error("slug retry exhausted for imdb=%s slug=%s",
                                  imdb, slug)
                        raise
                    slug_retries += 1
                    log.warning("slug '%s' collided (%s); regenerating for imdb=%s",
                                slug, constraint, imdb)
                    slug = unique_slug(cur, base_slug, year, reserved_slugs)
                    reserved_slugs.add(slug)
            if row is None:
                # ON CONFLICT DO NOTHING — someone else inserted this imdb
                # between our missing-check SELECT and now (or a re-run hit a
                # row created by an earlier crashed run). Skip uploads.
                conflict_skips += 1
                continue
            film_id = row[0]
            inserted_films += 1

            # ---- Submit cover download (AFTER INSERT, with final slug) ----
            # We now know the slug the INSERT actually used (post-retry).
            # The WebP filename matches the `cover_filename` we persisted, so
            # no orphan-mismatch can happen.
            if want_cover:
                cover_filename = slug
                cover_futures.append(cover_pool.submit(
                    _cover_worker, movie["poster_path"], slug, covers_dir,
                ))
            else:
                no_poster += 1

            # ---- INSERT uploads ----
            upload_rows = [
                {**{k: v for k, v in u.items() if not k.startswith("_")},
                 "film_id": film_id}
                for u in per_upload
            ]
            psycopg2.extras.execute_batch(cur, insert_upload_sql, upload_rows, page_size=200)
            inserted_uploads += len(upload_rows)

            # ---- Genre links ----
            for tmdb_gid in movie["genre_ids"]:
                genre_slug = TMDB_MOVIE_GENRE_MAP.get(tmdb_gid)
                if not genre_slug:
                    continue
                gid = slug_to_genre_id.get(genre_slug)
                if gid is None:
                    continue
                cur.execute(insert_genre_sql, (film_id, gid))

            if commit_every and inserted_films % commit_every == 0:
                conn.commit()

            if i % 100 == 0:
                rate = i / (time.time() - t1)
                log.info("[%d/%d]  films+=%d  uploads+=%d  tmdb_fail=%d  rate=%.1f/s",
                         i, len(missing_imdbs), inserted_films, inserted_uploads,
                         tmdb_failures, rate)

        log.info("Phase 2 done in %.1fs: inserted %d films, %d uploads",
                 time.time() - t1, inserted_films, inserted_uploads)
        log.info("  tmdb_failures=%d  no_uploads=%d  no_poster=%d  "
                 "conflict_skips=%d  slug_retries=%d",
                 tmdb_failures, no_uploads, no_poster, conflict_skips, slug_retries)

        # ---- Phase 3: drain cover downloads ----
        if cover_futures:
            log.info("Phase 3: waiting for %d cover downloads to finish ...",
                     len(cover_futures))
            t_cov = time.time()
            cover_ok = 0
            cover_fail = 0
            for done_n, fut in enumerate(as_completed(cover_futures), 1):
                try:
                    paths = fut.result()
                except Exception as e:  # noqa: BLE001
                    log.warning("cover future raised: %s", e)
                    paths = None
                if paths:
                    cover_ok += 1
                    if args.dry_run:
                        dry_run_covers_created.extend(paths)
                else:
                    cover_fail += 1
                if done_n % 1000 == 0:
                    rate = done_n / max(0.001, time.time() - t_cov)
                    log.info("  covers %d/%d (rate=%.1f/s, ok=%d fail=%d)",
                             done_n, len(cover_futures), rate,
                             cover_ok, cover_fail)
            log.info("Phase 3 done in %.1fs: %d covers ok, %d failed",
                     time.time() - t_cov, cover_ok, cover_fail)
            # If a cover future failed, the films row still has cover_filename
            # set (optimistic). The web handler already falls back to a
            # placeholder when the WebP is missing, so this is a display
            # issue, not data corruption.
        cover_pool.shutdown(wait=True)

        # ---- Row-count invariant ----
        # Monotonic growth is the hard invariant (never decrease). Equality
        # with before+inserted is the "ideal" state; a mismatch can happen
        # legitimately when a concurrent writer inserts rows between our
        # baseline COUNT and the final COUNT, so we only warn there — both
        # in live and dry-run modes.
        cur.execute("SELECT COUNT(*) FROM films")
        films_count_after = cur.fetchone()[0]
        expected_after = films_count_before + inserted_films
        if films_count_after < films_count_before:
            log.error("FATAL: films count DECREASED %d → %d",
                      films_count_before, films_count_after)
            return 3
        if films_count_after != expected_after:
            log.warning(
                "%sfilms count after (%d) != before+inserted (%d); "
                "probably concurrent import — still monotonic",
                "DRY-RUN: " if args.dry_run else "",
                films_count_after, expected_after,
            )
        log.info("films count OK: before=%d after=%d (+%d)",
                 films_count_before, films_count_after,
                 films_count_after - films_count_before)

        if args.dry_run:
            log.info("DRY-RUN: ROLLBACK")
            conn.rollback()
        else:
            conn.commit()
            log.info("COMMIT")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        if cover_pool is not None:
            if args.dry_run:
                # Under --dry-run we MUST drain the pool before scanning for
                # WebPs to delete — in-flight tasks would otherwise write new
                # files after our cleanup pass and violate the "no covers
                # left behind" guarantee. `cancel_futures=True` stops any
                # work that hasn't started; `wait=True` blocks for anything
                # already running to finish.
                cover_pool.shutdown(wait=True, cancel_futures=True)
            else:
                # Live mode: orphan covers are cheap and Phase 3 already
                # awaited the non-cancelled ones. Don't stall teardown on
                # whatever the pool is still chewing.
                cover_pool.shutdown(wait=False, cancel_futures=True)
        # After draining, rescan the covers dir for anything the workers
        # managed to write that wasn't yet in our recorded list (covers that
        # started before an exception but hadn't returned their future yet).
        if args.dry_run:
            recorded_paths = set(dry_run_covers_created)
            for p in recorded_paths:
                try:
                    if p and p.exists():
                        p.unlink()
                except OSError:
                    pass
            # Best-effort cleanup of any further files whose future slipped
            # past our tracking (an exception could have interrupted the
            # `as_completed` drain before we appended to the list).
            if cover_futures:
                for fut in cover_futures:
                    try:
                        paths = fut.result(timeout=0)
                    except Exception:  # noqa: BLE001
                        continue
                    if not paths:
                        continue
                    for p in paths:
                        if p in recorded_paths:
                            continue
                        try:
                            if p and p.exists():
                                p.unlink()
                        except OSError:
                            pass
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
