#!/usr/bin/env python3
"""Phase 1 MVP for Olbrasoft/cr#598 — sledujteto.cz films → TMDB IDs.

Pipeline:
  1. Crawl sledujteto `la` catch-all (1437 pages x 50 = 71 850 uploads) or
     reuse cached JSON dump ({slug_id: name_string} or {slug_id: file_dict}).
  2. Filter out series (SxxExx) and out-of-range durations (60-240 min).
  3. Deduplicate by (norm_title, year).
  4. Match each unique film against TMDB (fuzzy token_set_ratio >= 85).
  5. Emit matched.jsonl, unmatched.csv, stats.json, and optionally
     new-candidates.csv (diff against films.tmdb_id in local DB).

See GitHub issue Olbrasoft/cr#598 for full context.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import time
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path

import requests
from rapidfuzz import fuzz

log = logging.getLogger(__name__)

SLEDUJTETO_BASE = "https://www.sledujteto.cz"
SEARCH_PATH = "/api/web/videos"
LIMIT = 50
LA_PAGES_CAP = 1437
CATCH_ALL_QUERY = "la"
THROTTLE_S = 0.2

TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_RATE_S = 0.05

YEAR_RE = re.compile(r"(?:^|[\s\[\(\{])((?:19|20)\d{2})(?:$|[\s\]\)\}])")
SERIES_RE = re.compile(r"[sS]\d+[eE]\d+")
SLUG_RE = re.compile(r"/file/(\d+)/")
QUALITY_RE = re.compile(
    r"\b("
    r"1080p|720p|480p|2160p|4k|uhd|hd|fullhd|"
    r"bluray|bdrip|webrip|web-dl|dvdrip|hdtv|hdrip|hdcam|cam|ts|tc|"
    r"aac|ac3|dts|dd5\.?1|5\.?1|7\.?1|h\.?264|h\.?265|hevc|x264|x265|"
    r"cz\s*dab(?:ing)?|sk\s*dab(?:ing)?|cz\s*titulky|sk\s*titulky|dab(?:ing)?|titulky|"
    r"akcni|akční|drama|komedie|horor|sci[\-\s]?fi|thriller|romanticky|romantický|"
    r"animovany|animovaný|dokument|valecny|válečný|western|fantasy|rodinny|rodinný|kriminalni|kriminální|"
    r"extended|remastered|uncut|directors?\s*cut"
    r")\b",
    re.IGNORECASE,
)

DURATION_RE = re.compile(r"(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?\s*(?:(\d+)\s*s)?", re.IGNORECASE)

FILM_DURATION_MIN = 60
FILM_DURATION_MAX = 240


def extract_year(name: str) -> int | None:
    m = YEAR_RE.search(name)
    return int(m.group(1)) if m else None


def is_series(name: str) -> bool:
    return bool(SERIES_RE.search(name))


def parse_duration_min(s: str | None) -> int | None:
    if not s or not isinstance(s, str):
        return None
    m = DURATION_RE.fullmatch(s.strip())
    if not m:
        return None
    h, mi, se = (int(x) if x else 0 for x in m.groups())
    total = h * 60 + mi + (1 if se >= 30 else 0)
    return total if total > 0 else None


def is_film_duration(duration_str: str | None) -> tuple[bool, str]:
    minutes = parse_duration_min(duration_str)
    if minutes is None:
        return True, "unknown"
    if minutes < FILM_DURATION_MIN:
        return False, "too_short"
    if minutes > FILM_DURATION_MAX:
        return False, "too_long"
    return True, "ok"


def norm_title(name: str) -> str:
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    n = n.lower()
    n = YEAR_RE.sub(" ", n)
    n = SERIES_RE.sub(" ", n)
    n = QUALITY_RE.sub(" ", n)
    n = re.sub(r"[\[\]\(\)\{\}\-\.,:;!?/\\|]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def slug_from_url(url: str) -> str | None:
    m = SLUG_RE.search(url)
    return m.group(1) if m else None


def fetch_page(query: str, page: int, session: requests.Session) -> list[dict]:
    r = session.get(
        f"{SLEDUJTETO_BASE}{SEARCH_PATH}",
        params={
            "query": query,
            "limit": LIMIT,
            "page": page,
            "sort": "relevance",
            "me": 0,
            "excluded_ids": "",
        },
        headers={"User-Agent": "Mozilla/5.0 (cr-web catalog discovery)"},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["data"]["files"]


def crawl_catalog(limit_pages: int | None = None) -> dict[str, dict]:
    session = requests.Session()
    out: dict[str, dict] = {}
    max_page = min(LA_PAGES_CAP, limit_pages) if limit_pages else LA_PAGES_CAP
    t0 = time.time()
    for page in range(1, max_page + 1):
        try:
            files = fetch_page(CATCH_ALL_QUERY, page, session)
        except Exception as e:
            log.warning("page %d err: %s (retry once)", page, e)
            time.sleep(2)
            try:
                files = fetch_page(CATCH_ALL_QUERY, page, session)
            except Exception as e2:
                log.error("page %d giving up: %s", page, e2)
                continue
        for f in files:
            sid = slug_from_url(f.get("url", ""))
            if sid:
                out[sid] = f
        if page % 100 == 0:
            elapsed = int(time.time() - t0)
            log.info("page %d/%d — %d unique (+%ds)", page, max_page, len(out), elapsed)
        time.sleep(THROTTLE_S)
    return out


def tmdb_search(title: str, year: int | None, api_key: str, session: requests.Session) -> dict | None:
    params = {"api_key": api_key, "query": title, "language": "cs-CZ"}
    if year:
        params["year"] = year
    r = None
    for attempt in range(3):
        try:
            r = session.get(f"{TMDB_API_BASE}/search/movie", params=params, timeout=10)
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            break
        except Exception as e:
            log.warning("TMDB err for '%s' (%s): %s", title, year, e)
            time.sleep(1)
    else:
        return None

    if r is None:
        return None

    results = r.json().get("results", [])
    if not results and year:
        params.pop("year", None)
        try:
            r = session.get(f"{TMDB_API_BASE}/search/movie", params=params, timeout=10)
            r.raise_for_status()
            results = r.json().get("results", [])
        except Exception as e:
            log.warning("TMDB fallback err for '%s': %s", title, e)
            return None

    if not results:
        return None

    best: tuple[int, int, dict] | None = None
    for cand in results[:5]:
        cand_norm = norm_title((cand.get("title") or "") + " " + (cand.get("original_title") or ""))
        score = fuzz.token_set_ratio(title, cand_norm)
        if score < 85:
            continue
        year_bonus = 0
        rel = cand.get("release_date") or ""
        if year and len(rel) >= 4 and rel[:4].isdigit():
            ry = int(rel[:4])
            if ry == year:
                year_bonus = 10
            elif abs(ry - year) <= 1:
                year_bonus = 3
        rank = (score + year_bonus, score)
        if best is None or rank > best[:2]:
            best = (rank[0], rank[1], cand)

    if best is None:
        return None

    _, score, top = best
    return {
        "tmdb_id": top["id"],
        "tmdb_title": top.get("title"),
        "tmdb_original_title": top.get("original_title"),
        "tmdb_release_date": top.get("release_date"),
        "fuzzy_score": score,
    }


def load_known_tmdb_ids(database_url: str) -> set[int]:
    try:
        import psycopg2
    except ImportError:
        log.warning("psycopg2 not installed — skipping DB diff. Install: pip install psycopg2-binary")
        return set()
    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT tmdb_id FROM films WHERE tmdb_id IS NOT NULL")
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE pairs from a .env file into os.environ (no override)."""
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--cache", type=Path, help="Use existing la_catalog.json (skip crawl)")
    p.add_argument("--limit-pages", type=int, help="Crawl only N pages (dev)")
    p.add_argument("--skip-tmdb", action="store_true", help="Skip TMDB matching (catalog only)")
    p.add_argument("--out-dir", type=Path, default=Path("data/sledujteto"))
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    load_env_file(Path.cwd() / ".env")

    today = datetime.now().strftime("%Y-%m-%d")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.cache and args.cache.exists():
        log.info("Loading cache from %s", args.cache)
        with open(args.cache) as fp:
            raw = json.load(fp)
        catalog = {
            sid: ({"name": v, "url": f"/file/{sid}/"} if isinstance(v, str) else v)
            for sid, v in raw.items()
        }
    else:
        log.info("Crawling sledujteto catalog (query='%s', cap=%d pages)",
                 CATCH_ALL_QUERY, args.limit_pages or LA_PAGES_CAP)
        catalog = crawl_catalog(args.limit_pages)
        raw_path = args.out_dir / f"sledujteto-raw-{today}.json"
        with open(raw_path, "w") as fp:
            json.dump(catalog, fp, ensure_ascii=False)
        log.info("Raw dump: %s (%d uploads)", raw_path, len(catalog))

    log.info("Total uploads: %d", len(catalog))

    films_raw: dict[str, dict] = {}
    drop_reasons: Counter = Counter()
    for sid, f in catalog.items():
        name = f.get("name", "")
        if is_series(name):
            drop_reasons["series"] += 1
            continue
        ok, reason = is_film_duration(f.get("duration") or f.get("movie_duration"))
        if not ok:
            drop_reasons[f"duration_{reason}"] += 1
            continue
        drop_reasons[f"kept_duration_{reason}"] += 1
        films_raw[sid] = f
    log.info("Catalog: %d → films: %d. Drop reasons: %s",
             len(catalog), len(films_raw), dict(drop_reasons))

    groups: dict[tuple, list[str]] = {}
    for sid, f in films_raw.items():
        name = f.get("name", "")
        key = (norm_title(name), extract_year(name))
        groups.setdefault(key, []).append(sid)
    log.info("Unique films after dedup: %d", len(groups))

    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    matched: list[dict] = []
    unmatched: list[dict] = []

    if args.skip_tmdb or not api_key:
        if not api_key and not args.skip_tmdb:
            log.warning("TMDB_API_KEY not set — exporting without TMDB match")
        for (title, year), slug_ids in groups.items():
            unmatched.append({
                "norm_title": title,
                "year": year,
                "upload_count": len(slug_ids),
                "sample_slug_id": slug_ids[0],
                "sample_name": films_raw[slug_ids[0]].get("name", ""),
            })
    else:
        tmdb_session = requests.Session()
        for i, ((title, year), slug_ids) in enumerate(groups.items(), 1):
            if not title:
                continue
            hit = tmdb_search(title, year, api_key, tmdb_session)
            sample = films_raw[slug_ids[0]]
            if hit:
                matched.append({
                    "tmdb_id": hit["tmdb_id"],
                    "tmdb_title": hit["tmdb_title"],
                    "tmdb_original_title": hit["tmdb_original_title"],
                    "tmdb_release_date": hit["tmdb_release_date"],
                    "fuzzy_score": hit["fuzzy_score"],
                    "norm_title_query": title,
                    "year_query": year,
                    "sledujteto_slug_ids": slug_ids,
                    "sledujteto_upload_names": [films_raw[s].get("name", "") for s in slug_ids],
                    "sample_filesize": sample.get("filesize"),
                    "sample_duration": sample.get("duration"),
                    "sample_resolution": sample.get("resolution"),
                })
            else:
                unmatched.append({
                    "norm_title": title,
                    "year": year,
                    "upload_count": len(slug_ids),
                    "sample_slug_id": slug_ids[0],
                    "sample_name": sample.get("name", ""),
                })
            if i % 500 == 0:
                log.info("TMDB progress: %d/%d (matched=%d, unmatched=%d)",
                         i, len(groups), len(matched), len(unmatched))
            time.sleep(TMDB_RATE_S)

    matched_path = args.out_dir / f"sledujteto-films-matched-{today}.jsonl"
    with open(matched_path, "w") as fp:
        for rec in matched:
            fp.write(json.dumps(rec, ensure_ascii=False) + "\n")

    unmatched_path = args.out_dir / f"sledujteto-films-unmatched-{today}.csv"
    with open(unmatched_path, "w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=["norm_title", "year", "upload_count",
                                            "sample_slug_id", "sample_name"])
        w.writeheader()
        w.writerows(unmatched)

    stats = {
        "total_uploads": len(catalog),
        "films_after_series_and_duration_filter": len(films_raw),
        "unique_films_after_dedup": len(groups),
        "tmdb_matched": len(matched),
        "unmatched": len(unmatched),
        "matched_pct": round(100 * len(matched) / max(len(groups), 1), 2),
        "with_year": sum(1 for (_, y) in groups if y),
        "without_year": sum(1 for (_, y) in groups if not y),
        "drop_reasons": dict(drop_reasons),
        "film_duration_range_min": [FILM_DURATION_MIN, FILM_DURATION_MAX],
        "crawled_at": datetime.now().isoformat(),
    }
    stats_path = args.out_dir / f"sledujteto-films-stats-{today}.json"
    with open(stats_path, "w") as fp:
        json.dump(stats, fp, indent=2, ensure_ascii=False)
    log.info("Stats: %s", json.dumps(stats, ensure_ascii=False))

    db_url = os.environ.get("DATABASE_URL", "").strip()
    if db_url and matched:
        db_url = db_url.replace("@db:", "@127.0.0.1:")
        try:
            known = load_known_tmdb_ids(db_url)
            log.info("DB contains %d film tmdb_ids", len(known))
            new = [m for m in matched if m["tmdb_id"] not in known]
            cand_path = args.out_dir / f"sledujteto-films-new-candidates-{today}.csv"
            with open(cand_path, "w", newline="") as fp:
                w = csv.DictWriter(fp, fieldnames=[
                    "tmdb_id", "tmdb_title", "tmdb_release_date",
                    "sledujteto_upload_count", "sample_slug_id", "sample_name",
                ])
                w.writeheader()
                for m in new:
                    w.writerow({
                        "tmdb_id": m["tmdb_id"],
                        "tmdb_title": m["tmdb_title"],
                        "tmdb_release_date": m["tmdb_release_date"],
                        "sledujteto_upload_count": len(m["sledujteto_slug_ids"]),
                        "sample_slug_id": m["sledujteto_slug_ids"][0],
                        "sample_name": m["sledujteto_upload_names"][0],
                    })
            log.info("NEW CANDIDATES (not in DB): %d → %s", len(new), cand_path)
        except Exception as e:
            log.error("DB diff failed: %s", e)

    log.info("Done.")


if __name__ == "__main__":
    main()
