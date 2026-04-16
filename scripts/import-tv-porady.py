#!/usr/bin/env python3
"""Import TMDB-matched TV pořady from staging to production tv_shows/tv_episodes.

Source: sktorrent_tv_porady (staging, 864 videos, 745 matched to TMDB)
Target: tv_shows + tv_episodes (production tables; see migration 041)

Strategy:
- For each unique tmdb_id in staging:
  - If tv_show exists (by tmdb_id): reuse it, only add new episodes
  - Otherwise: INSERT tv_show with TMDB metadata
  - Covers auto-fetch via tv_porad_cover handler (TMDB fallback)
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


def unique_slug(cur, base: str, table: str, column: str = "slug", extra_where: str = "",
                cross_tables: tuple = ()) -> str:
    """Generate unique slug by appending -N if needed.

    Checks `table.column` for collisions, plus any additional `cross_tables`
    (flat slug columns) so that tv_shows slug won't collide with films, series
    or genres — which the DB triggers would reject.
    """
    if not base:
        base = "tv-porad"
    candidate = base
    n = 1
    while True:
        q = f"SELECT 1 FROM {table} WHERE {column} = %s {extra_where} LIMIT 1"
        cur.execute(q, (candidate,))
        collision = cur.fetchone() is not None
        if not collision:
            for other in cross_tables:
                cur.execute(f"SELECT 1 FROM {other} WHERE slug = %s LIMIT 1", (candidate,))
                if cur.fetchone():
                    collision = True
                    break
        if not collision:
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

    stats = {"shows_created": 0, "shows_reused": 0, "episodes_inserted": 0,
             "episodes_skipped": 0, "videos_skipped_no_ep": 0}

    for show in shows:
        tmdb_id = show["tmdb_id"]
        tmdb_name = show["tmdb_name"] or ""
        first_air = show["tmdb_first_air_date"] or ""
        first_year = None
        if first_air and len(first_air) >= 4 and first_air[:4].isdigit():
            first_year = int(first_air[:4])

        # Find or create tv_show
        with conn.cursor() as cur:
            cur.execute("SELECT id, slug FROM tv_shows WHERE tmdb_id = %s LIMIT 1", (tmdb_id,))
            existing = cur.fetchone()

        if existing:
            tv_show_id, tv_show_slug = existing
            stats["shows_reused"] += 1
            log.info("Reusing tv_show id=%d slug='%s' (tmdb=%d '%s')",
                     tv_show_id, tv_show_slug, tmdb_id, tmdb_name)
        else:
            base_slug = slugify(tmdb_name) or f"tv-porad-{tmdb_id}"
            with conn.cursor() as cur:
                tv_show_slug = unique_slug(
                    cur, base_slug, "tv_shows",
                    cross_tables=("films", "series", "genres"),
                )
                if args.dry_run:
                    log.info("[DRY] Would create tv_show '%s' (slug=%s, tmdb=%d, year=%s)",
                             tmdb_name, tv_show_slug, tmdb_id, first_year)
                    tv_show_id = -1
                else:
                    cur.execute("""
                        INSERT INTO tv_shows (title, slug, tmdb_id, imdb_id,
                          first_air_year, description, cover_filename, added_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                        RETURNING id
                    """, (
                        tmdb_name[:255],
                        tv_show_slug,
                        tmdb_id,
                        show["imdb_id"],
                        first_year,
                        show["tmdb_overview"],
                        tv_show_slug,  # cover_filename = slug; actual WebP fetched on demand
                    ))
                    tv_show_id = cur.fetchone()[0]
                    conn.commit()
                    log.info("Created tv_show id=%d slug='%s' (tmdb=%d '%s')",
                             tv_show_id, tv_show_slug, tmdb_id, tmdb_name)
            stats["shows_created"] += 1

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
                        INSERT INTO tv_episodes (
                          tv_show_id, season, episode, slug,
                          sktorrent_video_id, sktorrent_cdn, sktorrent_qualities,
                          sktorrent_added_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (tv_show_id, season, episode, sktorrent_video_id)
                        DO NOTHING
                        RETURNING id
                    """, (
                        tv_show_id, season, ep, ep_slug,
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
                log.warning("Slug collision for tv_show=%d s%de%d, adding suffix", tv_show_id, season, ep)
                with conn.cursor() as cur:
                    suffix_slug = unique_slug(cur, ep_slug, "tv_episodes",
                                              extra_where=f"AND tv_show_id = {tv_show_id}")
                    cur.execute("""
                        INSERT INTO tv_episodes (
                          tv_show_id, season, episode, slug,
                          sktorrent_video_id, sktorrent_cdn, sktorrent_qualities,
                          sktorrent_added_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (tv_show_id, season, episode, sktorrent_video_id) DO NOTHING
                    """, (
                        tv_show_id, season, ep, suffix_slug,
                        v["sktorrent_video_id"], v["cdn"], qualities_str,
                    ))
                conn.commit()
                stats["episodes_inserted"] += 1

    if not args.dry_run and shows:
        # Empty tuple would render as `IN ()` — invalid SQL. Skip the rollup
        # when there's nothing to update.
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE tv_shows s SET
                  season_count = sub.s_count,
                  episode_count = sub.e_count
                FROM (
                  SELECT tv_show_id,
                         MAX(season) as s_count,
                         COUNT(*) as e_count
                  FROM tv_episodes
                  WHERE tv_show_id IN (SELECT id FROM tv_shows WHERE tmdb_id IN %s)
                  GROUP BY tv_show_id
                ) sub
                WHERE s.id = sub.tv_show_id
            """, (tuple(s["tmdb_id"] for s in shows),))
            conn.commit()

    log.info("==== DONE ====")
    for k, v in stats.items():
        log.info("  %s: %d", k, v)

    conn.close()


if __name__ == "__main__":
    main()
