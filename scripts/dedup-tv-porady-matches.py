#!/usr/bin/env python3
"""Propagate TMDB matches across spelling variants of the same show.

Some shows appear in staging under multiple spellings:
- "Česko Slovensko má talent" (matched) vs "CESKO A SLOVENKO MA TALENT" (unmatched)
- "Clarksons Farm" (matched) vs "Clarksons.Farm2." (unmatched)

Strategy: normalize show_name aggressively (strip diacritics, punct, lowercase,
collapse whitespace), build a map from normalized key → (tmdb_id, imdb_id, etc.)
from already-matched rows, then apply that mapping to unmatched rows with the
same normalized key.

Usage:
    python3 scripts/dedup-tv-porady-matches.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import unicodedata

import psycopg2

log = logging.getLogger(__name__)


def normalize_key(name: str) -> str:
    """Aggressive normalization for fuzzy matching across spelling variants."""
    s = name or ""
    s = s.replace("&#039;", "'").replace("&amp;", "&")
    # Strip diacritics
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # Lowercase
    s = s.lower()
    # Strip common noise: series markers, trailing numbers like "2.", separator slashes, dots
    s = re.sub(r"\bseri[eéií]s?\s*\d+", "", s)
    s = re.sub(r"[/\\\-\(\)\[\]\.,:;!?'\"`]", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # Drop trailing standalone digits (e.g. "farm2" vs "farm")
    s = re.sub(r"\s*\d+\s*$", "", s).strip()
    # Strip common CZ stopwords that often differ (a, ma, má)
    tokens = [t for t in s.split() if t not in {"a", "and", "the"}]
    return " ".join(tokens)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="Don't write to DB, just report")
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

    conn = psycopg2.connect(db_url)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT show_name, tmdb_id, imdb_id, tmdb_name,
                   tmdb_first_air_date, tmdb_poster_path, tmdb_overview
            FROM sktorrent_tv_porady
            WHERE show_name IS NOT NULL
            GROUP BY show_name, tmdb_id, imdb_id, tmdb_name,
                     tmdb_first_air_date, tmdb_poster_path, tmdb_overview
        """)
        rows = cur.fetchall()

    # Build key → matched record
    key_to_match: dict[str, tuple] = {}
    unmatched_keys: dict[str, list[str]] = {}

    for show_name, tmdb_id, imdb_id, tmdb_name, first_air, poster, overview in rows:
        key = normalize_key(show_name)
        if not key:
            continue
        if tmdb_id:
            # Keep first match per key (or longest tmdb_name as tiebreaker)
            existing = key_to_match.get(key)
            if not existing or (tmdb_name and len(tmdb_name) > len(existing[3] or "")):
                key_to_match[key] = (tmdb_id, imdb_id, tmdb_name, first_air, poster, overview)
        else:
            unmatched_keys.setdefault(key, []).append(show_name)

    # Find candidates to propagate
    propagations = []
    for key, names in unmatched_keys.items():
        if key in key_to_match:
            tmdb_id, imdb_id, tmdb_name, first_air, poster, overview = key_to_match[key]
            for name in names:
                propagations.append((name, tmdb_id, imdb_id, tmdb_name, first_air, poster, overview, key))

    log.info("Found %d unmatched show variants that can be propagated from matched siblings", len(propagations))
    for name, tmdb_id, imdb_id, tmdb_name, _, _, _, key in propagations:
        log.info("  '%s' → tmdb_id=%d '%s' (key='%s')", name, tmdb_id, tmdb_name, key)

    if args.dry_run:
        log.info("DRY RUN — no changes made")
        conn.close()
        return

    if not propagations:
        log.info("Nothing to propagate")
        conn.close()
        return

    # Apply
    with conn.cursor() as cur:
        for name, tmdb_id, imdb_id, tmdb_name, first_air, poster, overview, _ in propagations:
            cur.execute("""
                UPDATE sktorrent_tv_porady
                SET tmdb_id = %s, imdb_id = %s, tmdb_name = %s,
                    tmdb_first_air_date = %s, tmdb_poster_path = %s, tmdb_overview = %s
                WHERE show_name = %s AND tmdb_id IS NULL
            """, (tmdb_id, imdb_id, tmdb_name, first_air, poster, overview, name))
    conn.commit()

    # Report
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              COUNT(*) as total,
              COUNT(tmdb_id) as with_tmdb,
              COUNT(imdb_id) as with_imdb,
              COUNT(DISTINCT show_name) as shows,
              COUNT(DISTINCT show_name) FILTER (WHERE tmdb_id IS NULL) as unmatched_shows
            FROM sktorrent_tv_porady
        """)
        total, with_tmdb, with_imdb, shows, unmatched_shows = cur.fetchone()

    print(f"\n{'='*60}")
    print("AFTER DEDUP PROPAGATION")
    print(f"{'='*60}")
    print(f"Total videos:     {total}")
    print(f"With TMDB ID:     {with_tmdb} ({100*with_tmdb/total:.1f}%)")
    print(f"With IMDB ID:     {with_imdb}")
    print(f"Unmatched shows:  {unmatched_shows} / {shows}")
    print(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    main()
