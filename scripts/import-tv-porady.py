#!/usr/bin/env python3
"""Import TMDB-matched TV pořady from staging to production series/episodes.

Source: sktorrent_tv_porady (staging, 864 videos, 745 matched to TMDB)
Target: series + episodes (production tables)

Strategy:
- For each unique tmdb_id in staging:
  - If series exists (by tmdb_id): reuse it, only add new episodes
  - Otherwise: INSERT series with TMDB metadata
  - Covers auto-fetch via existing series_cover handler (TMDB fallback)
- For each video:
  - season = season_number OR 1 (default if only episode_number present)
  - Skip if episode_number is NULL
  - Generate slug: 's{SS}e{EE}' (unique within series)
  - INSERT episode with sktorrent_video_id, sktorrent_cdn, sktorrent_qualities
  - ON CONFLICT DO NOTHING (idempotent)

Skips: SKT-only shows and episodes without episode_number (future work).

Usage:
    DATABASE_URL=... python3 scripts/import-tv-porady.py [--dry-run] [--limit N]
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import unicodedata

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def slugify(text: str) -> str:
    """URL-safe slug: lowercase, strip diacritics, replace non-alphanum with hyphens."""
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s


def unique_slug(cur, base: str, table: str, column: str = "slug", extra_where: str = "") -> str:
    """Generate unique slug by appending -N if needed."""
    if not base:
        base = "tv-porad"
    candidate = base
    n = 1
    while True:
        q = f"SELECT 1 FROM {table} WHERE {column} = %s {extra_where} LIMIT 1"
        cur.execute(q, (candidate,))
        if not cur.fetchone():
            return candidate
        n += 1
        candidate = f"{base}-{n}"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of TMDB shows to process")
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

    conn = psycopg2.connect(db_url)

    # Phase 1: get unique TMDB shows from staging
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT
              tmdb_id,
              MAX(imdb_id) as imdb_id,
              MAX(tmdb_name) as tmdb_name,
              MAX(tmdb_first_air_date) as tmdb_first_air_date,
              MAX(tmdb_poster_path) as tmdb_poster_path,
              MAX(tmdb_overview) as tmdb_overview
            FROM sktorrent_tv_porady
            WHERE tmdb_id IS NOT NULL AND episode_number IS NOT NULL
            GROUP BY tmdb_id
            ORDER BY tmdb_id
        """)
        shows = cur.fetchall()

    if args.limit:
        shows = shows[:args.limit]

    log.info("Processing %d TMDB-matched shows...", len(shows))

    stats = {"series_created": 0, "series_reused": 0, "episodes_inserted": 0,
             "episodes_skipped": 0, "videos_skipped_no_ep": 0}

    for show in shows:
        tmdb_id = show["tmdb_id"]
        tmdb_name = show["tmdb_name"] or ""
        first_air = show["tmdb_first_air_date"] or ""
        first_year = None
        if first_air and len(first_air) >= 4 and first_air[:4].isdigit():
            first_year = int(first_air[:4])

        # Find or create series
        with conn.cursor() as cur:
            cur.execute("SELECT id, slug FROM series WHERE tmdb_id = %s LIMIT 1", (tmdb_id,))
            existing = cur.fetchone()

        if existing:
            series_id, series_slug = existing
            stats["series_reused"] += 1
            log.info("Reusing series id=%d slug='%s' (tmdb=%d '%s')",
                     series_id, series_slug, tmdb_id, tmdb_name)
        else:
            # Create new series
            base_slug = slugify(tmdb_name) or f"tv-porad-{tmdb_id}"
            with conn.cursor() as cur:
                series_slug = unique_slug(cur, base_slug, "series")
                if args.dry_run:
                    log.info("[DRY] Would create series '%s' (slug=%s, tmdb=%d, year=%s)",
                             tmdb_name, series_slug, tmdb_id, first_year)
                    series_id = -1
                else:
                    cur.execute("""
                        INSERT INTO series (title, slug, tmdb_id, imdb_id,
                          first_air_year, description, cover_filename, added_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                        RETURNING id
                    """, (
                        tmdb_name[:255],
                        series_slug,
                        tmdb_id,
                        show["imdb_id"],
                        first_year,
                        show["tmdb_overview"],
                        series_slug,  # cover_filename = slug; actual WebP fetched on demand
                    ))
                    series_id = cur.fetchone()[0]
                    conn.commit()
                    log.info("Created series id=%d slug='%s' (tmdb=%d '%s')",
                             series_id, series_slug, tmdb_id, tmdb_name)
            stats["series_created"] += 1

        if args.dry_run:
            continue

        # Fetch videos for this show
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT sktorrent_video_id, cdn, qualities, duration_str,
                       season_number, episode_number, full_title
                FROM sktorrent_tv_porady
                WHERE tmdb_id = %s AND episode_number IS NOT NULL
                ORDER BY season_number NULLS FIRST, episode_number
            """, (tmdb_id,))
            videos = cur.fetchall()

        for v in videos:
            season = v["season_number"] or 1
            ep = v["episode_number"]
            if ep is None:
                stats["videos_skipped_no_ep"] += 1
                continue

            # Episode slug: just s{SS}e{EE} (unique within series)
            ep_slug = f"s{season:02d}e{ep:02d}"

            # qualities is postgres ARRAY → python list
            qualities = v["qualities"] or []
            if isinstance(qualities, list):
                qualities_str = ",".join(qualities) or "480p"
            else:
                qualities_str = str(qualities) or "480p"

            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO episodes (
                          series_id, season, episode, slug,
                          sktorrent_video_id, sktorrent_cdn, sktorrent_qualities,
                          sktorrent_added_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (series_id, season, episode, sktorrent_video_id)
                        DO NOTHING
                        RETURNING id
                    """, (
                        series_id, season, ep, ep_slug,
                        v["sktorrent_video_id"], v["cdn"], qualities_str,
                    ))
                    row = cur.fetchone()
                    if row:
                        stats["episodes_inserted"] += 1
                    else:
                        stats["episodes_skipped"] += 1
                conn.commit()
            except psycopg2.errors.UniqueViolation as e:
                conn.rollback()
                # Slug collision — try with suffix
                log.warning("Slug collision for series=%d s%de%d, adding suffix", series_id, season, ep)
                with conn.cursor() as cur:
                    suffix_slug = unique_slug(cur, ep_slug, "episodes",
                                              extra_where=f"AND series_id = {series_id}")
                    cur.execute("""
                        INSERT INTO episodes (
                          series_id, season, episode, slug,
                          sktorrent_video_id, sktorrent_cdn, sktorrent_qualities,
                          sktorrent_added_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (series_id, season, episode, sktorrent_video_id) DO NOTHING
                    """, (
                        series_id, season, ep, suffix_slug,
                        v["sktorrent_video_id"], v["cdn"], qualities_str,
                    ))
                conn.commit()
                stats["episodes_inserted"] += 1

    # Update season_count / episode_count on all touched series
    if not args.dry_run:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE series s SET
                  season_count = sub.s_count,
                  episode_count = sub.e_count
                FROM (
                  SELECT series_id,
                         MAX(season) as s_count,
                         COUNT(*) as e_count
                  FROM episodes
                  WHERE series_id IN (SELECT id FROM series WHERE tmdb_id IN %s)
                  GROUP BY series_id
                ) sub
                WHERE s.id = sub.series_id
            """, (tuple(s["tmdb_id"] for s in shows),))
            conn.commit()

    log.info("==== DONE ====")
    for k, v in stats.items():
        log.info("  %s: %d", k, v)

    conn.close()


if __name__ == "__main__":
    main()
