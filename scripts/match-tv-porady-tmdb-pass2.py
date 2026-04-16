#!/usr/bin/env python3
"""Second-pass TMDB matching for unmatched TV pořady shows.

Aggressive query normalization handles edge cases the first pass missed:
- Strip trailing " - {episode subtitle}" (e.g. "Paul Hollywood II - Reykjavík")
- Strip Roman numeral season markers (II, III, IV)
- Strip "Stand-up comedy", "live", "space episode" suffixes
- Strip trailing dates, parens, " Episode N ("
- Collapse dots and weird separators

For each remaining unique unmatched show_name, try TMDB with cleaned query.
Apply match to all rows with that show_name.

Usage:
    DATABASE_URL=... TMDB_API_KEY=... python3 scripts/match-tv-porady-tmdb-pass2.py
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time

import psycopg2
import requests

log = logging.getLogger(__name__)

TMDB_API_BASE = "https://api.themoviedb.org/3"


def aggressive_clean(name: str) -> list[str]:
    """Generate multiple candidate queries from noisy show_name.

    Returns list of candidates to try in order (most specific → most generic).
    """
    raw = name.replace("&#039;", "'").replace("&amp;", "&").strip()

    candidates: list[str] = []

    def add(c: str):
        c = re.sub(r"\s+", " ", c).strip(" -.,:/")
        if c and c not in candidates and len(c) >= 3:
            candidates.append(c)

    # Strategy 1: raw
    add(raw)

    # Strategy 2: strip everything after first " - " (episode subtitle)
    if " - " in raw:
        add(raw.split(" - ")[0])

    # Strategy 3: strip everything after first " / " (alt-title separator)
    if " / " in raw:
        add(raw.split(" / ")[0])
        add(raw.split(" / ")[-1])  # sometimes the English title is after

    # Strategy 4: strip trailing Roman numeral + anything after
    s = re.sub(r"\s+(II|III|IV|V|VI|VII|VIII|IX|X)\b.*$", "", raw, flags=re.IGNORECASE)
    if s != raw:
        add(s)

    # Strategy 5: strip trailing " Episode N (" or " Episode N ..."
    s = re.sub(r"\s*-?\s*Episode\s+\d+.*$", "", raw, flags=re.IGNORECASE)
    if s != raw:
        add(s)

    # Strategy 6: replace dots/underscores with spaces
    s = re.sub(r"[._]+", " ", raw)
    if s != raw:
        add(s)
        # Combined: dots + strip after " - "
        if " - " in s:
            add(s.split(" - ")[0])

    # Strategy 7: strip trailing year/date
    s = re.sub(r"\s*\d{4}(-\d{2}-\d{2})?\s*$", "", raw).strip()
    if s and s != raw:
        add(s)

    # Strategy 8: strip "Stand-up comedy špeciál", "Live", "špeciál"
    s = re.sub(r"\s*(stand[- ]up\s+comedy\s+šp?eci[aá]l|šp?eci[aá]l|live|znělka|bonusový díl|bonusove stand upy)\s*$",
               "", raw, flags=re.IGNORECASE)
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)  # strip trailing (parenthesized)
    if s and s != raw:
        add(s)

    # Strategy 9: strip trailing " S\d+" patterns
    s = re.sub(r"\s+S\d+\b.*$", "", raw, flags=re.IGNORECASE)
    if s and s != raw:
        add(s)

    # Strategy 10: strip leading date prefix "10.10_24_" or "Oct 4, 2024"
    s = re.sub(r"^\d{1,2}[._/]\d{1,2}[._/]?\d{0,4}[_\s]+", "", raw)
    s = re.sub(r"^[A-Za-z]{3}\s+\d{1,2},?\s+\d{4}\s+", "", s)
    if s and s != raw:
        add(s)
        if " - " in s:
            add(s.split(" - ")[0])

    return candidates


def search_tmdb_tv(query: str, api_key: str) -> dict | None:
    """Search TMDB, try CZ then EN."""
    for lang in ["cs-CZ", "en-US"]:
        try:
            r = requests.get(f"{TMDB_API_BASE}/search/tv", params={
                "api_key": api_key,
                "query": query,
                "language": lang,
            }, timeout=15)
        except requests.RequestException as e:
            log.warning("TMDB search error for '%s': %s", query, e)
            continue
        if r.status_code != 200:
            continue
        results = r.json().get("results", [])
        if not results:
            continue
        # Prefer exact title match, fall back to first
        q_lower = query.lower()
        for res in results:
            if res.get("name", "").lower() == q_lower or res.get("original_name", "").lower() == q_lower:
                return res
        return results[0]
    return None


def get_imdb_id(tmdb_id: int, api_key: str) -> str | None:
    try:
        r = requests.get(f"{TMDB_API_BASE}/tv/{tmdb_id}/external_ids",
                         params={"api_key": api_key}, timeout=15)
        if r.status_code == 200:
            return r.json().get("imdb_id")
    except requests.RequestException:
        pass
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
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

    api_key = os.environ.get("TMDB_API_KEY", "")
    if not api_key:
        log.error("TMDB_API_KEY not set")
        sys.exit(1)

    conn = psycopg2.connect(db_url)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT show_name FROM sktorrent_tv_porady
            WHERE tmdb_id IS NULL AND show_name IS NOT NULL
            ORDER BY show_name
        """)
        shows = [row[0] for row in cur.fetchall()]

    log.info("Trying TMDB pass-2 matching for %d unique unmatched shows...", len(shows))

    matched = 0
    still_not_found = []

    for i, show_name in enumerate(shows, 1):
        candidates = aggressive_clean(show_name)
        log.debug("[%d/%d] '%s' → candidates: %s", i, len(shows), show_name, candidates)

        result = None
        successful_query = None
        for cand in candidates:
            result = search_tmdb_tv(cand, api_key)
            time.sleep(0.25)
            if result:
                successful_query = cand
                break

        if result:
            tmdb_id = result["id"]
            tmdb_name = result.get("name", "")
            imdb_id = get_imdb_id(tmdb_id, api_key)
            time.sleep(0.15)
            log.info("[%d/%d] ✓ '%s' via '%s' → tmdb=%d '%s'",
                     i, len(shows), show_name, successful_query, tmdb_id, tmdb_name)
            if not args.dry_run:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE sktorrent_tv_porady
                        SET tmdb_id = %s, imdb_id = %s, tmdb_name = %s,
                            tmdb_first_air_date = %s, tmdb_poster_path = %s,
                            tmdb_overview = %s
                        WHERE show_name = %s AND tmdb_id IS NULL
                    """, (
                        tmdb_id, imdb_id, tmdb_name,
                        result.get("first_air_date", ""),
                        result.get("poster_path", ""),
                        result.get("overview", ""),
                        show_name,
                    ))
                conn.commit()
            matched += 1
        else:
            log.info("[%d/%d] ✗ '%s' (tried: %s)", i, len(shows), show_name, candidates[:3])
            still_not_found.append(show_name)

    # Final stats
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), COUNT(tmdb_id), COUNT(imdb_id),
                   COUNT(DISTINCT show_name) FILTER (WHERE tmdb_id IS NULL)
            FROM sktorrent_tv_porady
        """)
        total, with_tmdb, with_imdb, unmatched_shows = cur.fetchone()

    print(f"\n{'='*60}")
    print("PASS-2 MATCHING REPORT")
    print(f"{'='*60}")
    print(f"Pass-2 attempts:       {len(shows)} shows")
    print(f"Pass-2 matched:        {matched}")
    print(f"Pass-2 unmatched:      {len(still_not_found)}")
    print(f"---")
    print(f"Total videos:          {total}")
    print(f"With TMDB ID:          {with_tmdb} ({100*with_tmdb/total:.1f}%)")
    print(f"With IMDB ID:          {with_imdb}")
    print(f"Unmatched shows:       {unmatched_shows}")
    print(f"{'='*60}")

    if still_not_found:
        print(f"\nStill unmatched ({len(still_not_found)}):")
        for name in still_not_found:
            print(f"  {name}")

    conn.close()


if __name__ == "__main__":
    main()
