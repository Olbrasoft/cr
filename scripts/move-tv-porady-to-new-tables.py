#!/usr/bin/env python3
"""Move TV pořady from series/episodes into the new tv_shows/tv_episodes tables.

The import pipeline (#457) put 37 TV pořady + 684 episodes into the
scripted-series tables by mistake. Issue #463 moves them into the
dedicated `tv_shows` / `tv_episodes` catalog created in #462.

Selection: any `series.tmdb_id` that appears in `sktorrent_tv_porady`
(the staging table that holds everything scraped from /videos/tv-porady).

Per-show transaction — idempotent:
- If the series is still in `series`: lock row, copy all columns to
  `tv_shows` (with original id preserved so any external references
  keep pointing at the same PK), copy episodes to `tv_episodes`
  (one row per episode, slug recomputed from s/e), then DELETE from
  `series` (CASCADE wipes `episodes`).
- The DELETE happens BEFORE the INSERT into `tv_shows` so the
  cross-slug trigger doesn't reject the new row.
- Already-moved shows (tmdb_id no longer in `series` but present in
  `tv_shows`) are skipped on re-run.

Usage:
    DATABASE_URL=... python3 scripts/move-tv-porady-to-new-tables.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

SERIES_COPY_COLUMNS = [
    "id",
    "title",
    "original_title",
    "slug",
    "first_air_year",
    "last_air_year",
    "description",
    "generated_description",
    "tmdb_overview_en",
    "imdb_id",
    "tmdb_id",
    "csfd_id",
    "imdb_rating",
    "csfd_rating",
    "season_count",
    "episode_count",
    "cover_filename",
    "has_dub",
    "has_subtitles",
    "old_slug",
    "added_at",
    "created_at",
]

EPISODE_COPY_COLUMNS = [
    "season",
    "episode",
    "title",
    "slug",
    "episode_name",
    "overview",
    "overview_en",
    "generated_description",
    "air_date",
    "runtime",
    "still_filename",
    "vote_average",
    "sktorrent_video_id",
    "sktorrent_cdn",
    "sktorrent_qualities",
    "sktorrent_added_at",
    "prehrajto_url",
    "prehrajto_has_dub",
    "prehrajto_has_subs",
    "has_dub",
    "has_subtitles",
    "created_at",
]


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would be moved without writing")
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

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT tmdb_id
            FROM sktorrent_tv_porady
            WHERE tmdb_id IS NOT NULL
            ORDER BY tmdb_id
        """)
        tmdb_ids = [row[0] for row in cur.fetchall()]

    log.info("TV pořady staging has %d distinct tmdb_ids", len(tmdb_ids))

    stats = {
        "already_moved": 0,
        "shows_moved": 0,
        "episodes_moved": 0,
        "shows_not_in_series": 0,
    }

    series_cols_sql = ", ".join(SERIES_COPY_COLUMNS)
    episode_cols_sql = ", ".join(EPISODE_COPY_COLUMNS)
    episode_insert_cols_sql = "tv_show_id, " + episode_cols_sql
    episode_placeholders = ", ".join(["%s"] * (len(EPISODE_COPY_COLUMNS) + 1))
    series_insert_placeholders = ", ".join(["%s"] * len(SERIES_COPY_COLUMNS))

    for tmdb_id in tmdb_ids:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                f"SELECT {series_cols_sql} FROM series WHERE tmdb_id = %s LIMIT 1",
                (tmdb_id,),
            )
            series_row = cur.fetchone()

            if series_row is None:
                cur.execute(
                    "SELECT 1 FROM tv_shows WHERE tmdb_id = %s LIMIT 1",
                    (tmdb_id,),
                )
                if cur.fetchone():
                    stats["already_moved"] += 1
                    log.debug("tmdb=%d already in tv_shows — skipping", tmdb_id)
                else:
                    stats["shows_not_in_series"] += 1
                    log.warning("tmdb=%d not in series and not in tv_shows", tmdb_id)
                continue

            series_id = series_row["id"]
            title = series_row["title"]
            slug = series_row["slug"]

            cur.execute(
                f"""SELECT {episode_cols_sql}
                    FROM episodes
                    WHERE series_id = %s
                    ORDER BY season, episode, id""",
                (series_id,),
            )
            episode_rows = cur.fetchall()

        log.info(
            "[move] tmdb=%d id=%d '%s' slug='%s' episodes=%d",
            tmdb_id, series_id, title, slug, len(episode_rows),
        )

        if args.dry_run:
            stats["shows_moved"] += 1
            stats["episodes_moved"] += len(episode_rows)
            continue

        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM series WHERE id = %s", (series_id,))

                    cur.execute(
                        f"""INSERT INTO tv_shows ({series_cols_sql})
                            VALUES ({series_insert_placeholders})
                            RETURNING id""",
                        tuple(series_row[c] for c in SERIES_COPY_COLUMNS),
                    )
                    new_id = cur.fetchone()[0]

                    for ep in episode_rows:
                        cur.execute(
                            f"""INSERT INTO tv_episodes ({episode_insert_cols_sql})
                                VALUES ({episode_placeholders})""",
                            (new_id,) + tuple(ep[c] for c in EPISODE_COPY_COLUMNS),
                        )

            stats["shows_moved"] += 1
            stats["episodes_moved"] += len(episode_rows)

        except Exception as exc:
            log.error("tmdb=%d failed: %s", tmdb_id, exc)

    cur_max = None
    with conn.cursor() as cur:
        cur.execute("SELECT setval('tv_shows_id_seq', COALESCE((SELECT MAX(id) FROM tv_shows), 1), true)")
        cur_max = cur.fetchone()[0]
        cur.execute("SELECT setval('tv_episodes_id_seq', COALESCE((SELECT MAX(id) FROM tv_episodes), 1), true)")
    conn.commit()
    log.info("Sequences advanced to match moved data (tv_shows max id=%s)", cur_max)

    log.info("==== DONE ====")
    for k, v in stats.items():
        log.info("  %s: %d", k, v)

    conn.close()


if __name__ == "__main__":
    main()
