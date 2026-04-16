#!/usr/bin/env python3
"""Match TV pořady show names to TMDB TV series.

Reads unique show_name values from sktorrent_tv_porady (Phase 1 must be done),
searches TMDB for each, and stores tmdb_id + metadata back into the staging table.

Usage:
    python3 scripts/match-tv-porady-tmdb.py [--verbose]
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import unicodedata

import psycopg2
import psycopg2.extras
import requests

log = logging.getLogger(__name__)

TMDB_API_BASE = "https://api.themoviedb.org/3"


def normalize_for_search(name: str) -> str:
    """Clean show name for TMDB search — remove trailing punctuation, HTML entities, dots."""
    s = name.strip()
    # Decode HTML entities
    s = s.replace("&#039;", "'").replace("&amp;", "&")
    # Replace dots with spaces (Clarksons.Farm2. → Clarksons Farm2)
    s = re.sub(r'\.+', ' ', s)
    # Remove trailing punctuation and artifacts
    s = re.sub(r'\s*[-/(]+\s*$', '', s)
    s = re.sub(r'\s*\(\s*$', '', s)
    s = s.strip()
    return s


def search_tmdb_tv(query: str, api_key: str) -> dict | None:
    """Search TMDB for a TV series. Returns best match or None."""
    clean = normalize_for_search(query)
    if not clean:
        return None

    # Try Czech first, then English
    for lang in ["cs-CZ", "en-US"]:
        r = requests.get(f"{TMDB_API_BASE}/search/tv", params={
            "api_key": api_key,
            "query": clean,
            "language": lang,
        }, timeout=15)
        if r.status_code != 200:
            log.warning("TMDB search failed for '%s' (lang=%s): HTTP %d", clean, lang, r.status_code)
            continue

        results = r.json().get("results", [])
        if results:
            # Prefer exact name match, fall back to first result
            clean_lower = clean.lower()
            for res in results:
                if res.get("name", "").lower() == clean_lower:
                    return res
                if res.get("original_name", "").lower() == clean_lower:
                    return res
            return results[0]

    return None


def get_tmdb_external_ids(tmdb_id: int, api_key: str) -> dict:
    """Get IMDB ID for a TMDB TV series."""
    r = requests.get(f"{TMDB_API_BASE}/tv/{tmdb_id}/external_ids", params={
        "api_key": api_key,
    }, timeout=15)
    if r.status_code == 200:
        return r.json()
    return {}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        log.error("DATABASE_URL not set")
        sys.exit(1)
    db_url = db_url.replace("@db:", "@127.0.0.1:")

    api_key = os.environ.get("TMDB_API_KEY", "")
    if not api_key:
        log.error("TMDB_API_KEY not set")
        sys.exit(1)

    conn = psycopg2.connect(db_url)

    # Add tmdb columns if not exists
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE sktorrent_tv_porady
              ADD COLUMN IF NOT EXISTS tmdb_id INTEGER,
              ADD COLUMN IF NOT EXISTS imdb_id TEXT,
              ADD COLUMN IF NOT EXISTS tmdb_name TEXT,
              ADD COLUMN IF NOT EXISTS tmdb_first_air_date TEXT,
              ADD COLUMN IF NOT EXISTS tmdb_poster_path TEXT,
              ADD COLUMN IF NOT EXISTS tmdb_overview TEXT;
        """)
    conn.commit()

    # Get unique unmatched show names
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT show_name FROM sktorrent_tv_porady
            WHERE show_name IS NOT NULL AND tmdb_id IS NULL
            ORDER BY show_name
        """)
        shows = [row[0] for row in cur.fetchall()]

    log.info("Searching TMDB for %d unique shows...", len(shows))

    matched = 0
    not_found = 0

    for i, show_name in enumerate(shows, 1):
        clean = normalize_for_search(show_name)
        log.info("[%d/%d] TMDB search: '%s'", i, len(shows), clean)

        result = search_tmdb_tv(show_name, api_key)
        time.sleep(0.3)  # TMDB rate limit: ~40 req/10s

        if result:
            tmdb_id = result["id"]
            tmdb_name = result.get("name", "")
            first_air = result.get("first_air_date", "")
            poster = result.get("poster_path", "")
            overview = result.get("overview", "")

            # Get IMDB ID
            ext_ids = get_tmdb_external_ids(tmdb_id, api_key)
            imdb_id = ext_ids.get("imdb_id")
            time.sleep(0.2)

            log.info("  ✓ tmdb_id=%d name='%s' (%s) imdb=%s",
                     tmdb_id, tmdb_name, first_air[:4] if first_air else "?",
                     imdb_id or "none")

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE sktorrent_tv_porady
                    SET tmdb_id = %s, imdb_id = %s, tmdb_name = %s,
                        tmdb_first_air_date = %s, tmdb_poster_path = %s,
                        tmdb_overview = %s
                    WHERE show_name = %s
                """, (tmdb_id, imdb_id, tmdb_name, first_air, poster, overview, show_name))
            conn.commit()
            matched += 1
        else:
            log.info("  ✗ not found")
            not_found += 1

    # Print report
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(tmdb_id) as with_tmdb,
                COUNT(imdb_id) as with_imdb,
                COUNT(DISTINCT show_name) as unique_shows,
                COUNT(DISTINCT tmdb_id) FILTER (WHERE tmdb_id IS NOT NULL) as unique_tmdb
            FROM sktorrent_tv_porady
        """)
        stats = cur.fetchone()

    print(f"\n{'='*60}")
    print("TMDB MATCHING REPORT")
    print(f"{'='*60}")
    print(f"Total videos:           {stats[0]}")
    print(f"With TMDB ID:           {stats[1]} ({stats[4]} unique shows)")
    print(f"With IMDB ID:           {stats[2]}")
    print(f"Unmatched:              {stats[0] - stats[1]}")
    print(f"Shows matched/searched: {matched}/{matched + not_found}")
    print(f"{'='*60}")

    # Unmatched shows
    with conn.cursor() as cur:
        cur.execute("""
            SELECT show_name, COUNT(*) as cnt
            FROM sktorrent_tv_porady
            WHERE tmdb_id IS NULL AND show_name IS NOT NULL
            GROUP BY show_name
            ORDER BY cnt DESC
        """)
        unmatched = cur.fetchall()

    if unmatched:
        print(f"\nUnmatched shows ({len(unmatched)}):")
        for name, cnt in unmatched:
            print(f"  {cnt:4d} ep  {name}")

    conn.close()


if __name__ == "__main__":
    main()
