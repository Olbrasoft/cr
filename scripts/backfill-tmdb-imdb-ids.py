#!/usr/bin/env python3
"""Backfill `imdb_id` from TMDB external_ids for every film/series/tv_show
that already has `tmdb_id` but no `imdb_id` (#593 follow-up).

Background: the daily IMDb rating sync (#690, scripts/sync-imdb-ratings.py)
matches our rows against the public IMDb TSV by `imdb_id` — so a film
without an IMDb ID gets no IMDb rating, no IMDb badge in the UI. The
initial auto-import populates `imdb_id` for SK Torrent films because
that's where the TMDB lookup originates, but newer prehraj.to / sledujteto
imports often skip the second TMDB call that would fetch external_ids.

TMDB exposes IMDb IDs in two places:
  * `GET /movie/{tmdb_id}/external_ids` — dedicated endpoint, smallest payload.
  * `GET /movie/{tmdb_id}` — main movie endpoint, the `imdb_id` field is included.

Either works for films; for `series` / `tv_shows` use the `/tv/...` family
where the same `imdb_id` field appears under external_ids. We hit the
dedicated external_ids endpoint to keep the payload small.

Idempotent — rows that already have a non-NULL `imdb_id` are skipped by
the SELECT, so re-running only touches new gaps.

Usage:
    DATABASE_URL=postgres://... TMDB_API_KEY=... \\
        python3 scripts/backfill-tmdb-imdb-ids.py \\
            [--table films|series|tv_shows|all] [--limit N] [--workers N] \\
            [--dry-run]
"""

from __future__ import annotations

import argparse
import collections
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import requests

TMDB_BASE = "https://api.themoviedb.org/3"
DEFAULT_WORKERS = 8


def _fetch_imdb_id(api_key: str, endpoint: str, tmdb_id: int) -> str | None:
    url = f"{TMDB_BASE}/{endpoint}/{tmdb_id}/external_ids"
    try:
        r = requests.get(url, params={"api_key": api_key}, timeout=10)
    except requests.RequestException as exc:
        logging.warning("TMDB request failed for %s/%s: %s", endpoint, tmdb_id, exc)
        return None
    if r.status_code == 404:
        return None
    if not r.ok:
        logging.warning("TMDB %s/%s returned %s", endpoint, tmdb_id, r.status_code)
        return None
    imdb_id = r.json().get("imdb_id")
    # TMDB returns "" (empty string) for titles missing an IMDb link.
    # Normalise to None so the UPDATE below treats them as "no result"
    # rather than wiping the column with an empty string.
    if not imdb_id:
        return None
    return imdb_id


def _table_config(table: str) -> str:
    if table == "films":
        return "movie"
    if table in ("series", "tv_shows"):
        return "tv"
    raise SystemExit(f"unsupported table: {table}")


def _run_table(
    table: str,
    *,
    api_key: str,
    conn,
    limit: int,
    workers: int,
    dry_run: bool,
) -> collections.Counter:
    endpoint = _table_config(table)
    cur = conn.cursor()

    select_sql = (
        f"SELECT id, tmdb_id FROM {table} "
        "WHERE tmdb_id IS NOT NULL AND imdb_id IS NULL "
        "ORDER BY id"
    )
    if limit:
        select_sql += f" LIMIT {limit}"
    cur.execute(select_sql)
    rows = cur.fetchall()
    mode = " (DRY RUN)" if dry_run else ""
    logging.info("Backfilling imdb_id for %d %s rows via TMDB %s/external_ids%s",
                 len(rows), table, endpoint, mode)

    # Some titles already carry an imdb_id on a sibling row (curated /
    # legacy data). The films table enforces a partial UNIQUE on imdb_id
    # so a duplicate INSERT/UPDATE would explode the transaction. Guard
    # with NOT EXISTS in the UPDATE — collision means we have the same
    # title twice under different tmdb_ids and the human should review.
    update_sql = (
        f"UPDATE {table} SET imdb_id = %s WHERE id = %s "
        f"AND NOT EXISTS (SELECT 1 FROM {table} t2 "
        "WHERE t2.imdb_id = %s AND t2.id <> %s)"
    )
    dup_check_sql = (
        f"SELECT 1 FROM {table} WHERE imdb_id = %s AND id <> %s LIMIT 1"
    )
    counts = collections.Counter()
    counts["scanned"] = len(rows)

    def work(row):
        row_id, tmdb_id = row
        imdb_id = _fetch_imdb_id(api_key, endpoint, tmdb_id)
        return row_id, tmdb_id, imdb_id

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(work, r) for r in rows]
        last_commit = time.monotonic()
        pending = 0
        for i, f in enumerate(as_completed(futures), 1):
            row_id, tmdb_id, imdb_id = f.result()
            if imdb_id:
                if dry_run:
                    cur.execute(dup_check_sql, (imdb_id, row_id))
                    if cur.fetchone():
                        counts["duplicate_skipped"] += 1
                        logging.info("DRY: %s.id=%s tmdb=%s → imdb=%s SKIP (duplicate)",
                                     table, row_id, tmdb_id, imdb_id)
                    else:
                        counts["would_save"] += 1
                        logging.info("DRY: %s.id=%s tmdb=%s → imdb=%s",
                                     table, row_id, tmdb_id, imdb_id)
                else:
                    cur.execute(update_sql, (imdb_id, row_id, imdb_id, row_id))
                    if cur.rowcount:
                        counts["saved"] += 1
                    else:
                        counts["duplicate_skipped"] += 1
                    pending += 1
            else:
                counts["no_imdb_id"] += 1
            if not dry_run and (pending >= 200 or time.monotonic() - last_commit > 5):
                conn.commit()
                pending = 0
                last_commit = time.monotonic()
            if i % 500 == 0:
                logging.info("Progress %s: %d/%d — saved=%d would_save=%d no_imdb=%d dup=%d",
                             table, i, len(rows),
                             counts["saved"], counts["would_save"],
                             counts["no_imdb_id"], counts["duplicate_skipped"])

    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    logging.info("[%s] Done — %s", table, dict(counts))
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="films",
                        choices=["films", "series", "tv_shows", "all"])
    parser.add_argument("--limit", type=int, default=0, help="0 = all")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--dry-run", action="store_true",
                        help="Log proposed writes without persisting.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    dsn = os.environ.get("DATABASE_URL", "").strip()
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not dsn:
        raise SystemExit("DATABASE_URL required")
    if not api_key:
        raise SystemExit("TMDB_API_KEY required")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    tables = ["films", "series", "tv_shows"] if args.table == "all" else [args.table]
    totals = collections.Counter()
    for t in tables:
        c = _run_table(
            t,
            api_key=api_key,
            conn=conn,
            limit=args.limit,
            workers=args.workers,
            dry_run=args.dry_run,
        )
        for k, v in c.items():
            totals[f"{t}:{k}"] += v

    logging.info("Grand totals — %s", dict(totals))
    return 0


if __name__ == "__main__":
    sys.exit(main())
