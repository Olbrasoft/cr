#!/usr/bin/env python3
"""Backfill `films.tmdb_poster_path` from TMDB API for issue #581.

Large film covers move from static R2 storage to a dynamic proxy that
fetches `https://image.tmdb.org/t/p/w780{poster_path}` at request time.
The `poster_path` string (e.g. `/mqlgZ…uJ.jpg`) is a content hash that
cannot be derived from `tmdb_id`, so we store it once and reuse.

Usage:
    DATABASE_URL=postgres://... TMDB_API_KEY=... python3 scripts/backfill-tmdb-poster-paths.py [--table films|series|tv_shows] [--limit N]

Idempotent — rows already holding a non-null `tmdb_poster_path` are skipped.
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


def _fetch_poster_path(api_key: str, endpoint: str, tmdb_id: int) -> str | None:
    url = f"{TMDB_BASE}/{endpoint}/{tmdb_id}"
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
    return r.json().get("poster_path")


def _table_config(table: str) -> tuple[str, str]:
    """Return (tmdb endpoint, primary key column) for the given table."""
    if table == "films":
        return "movie", "id"
    if table in ("series", "tv_shows"):
        return "tv", "id"
    raise SystemExit(f"unsupported table: {table}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="films", choices=["films", "series", "tv_shows"])
    parser.add_argument("--limit", type=int, default=0, help="0 = all")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    dsn = os.environ.get("DATABASE_URL", "").strip()
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not dsn:
        raise SystemExit("DATABASE_URL required")
    if not api_key:
        raise SystemExit("TMDB_API_KEY required")

    endpoint, pk = _table_config(args.table)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    select_sql = (
        f"SELECT {pk}, tmdb_id FROM {args.table} "
        "WHERE tmdb_id IS NOT NULL AND tmdb_poster_path IS NULL "
        f"ORDER BY {pk}"
    )
    if args.limit:
        select_sql += f" LIMIT {args.limit}"
    cur.execute(select_sql)
    rows = cur.fetchall()
    logging.info("Fetching poster_path for %d %s rows via TMDB %s endpoint", len(rows), args.table, endpoint)

    update_sql = f"UPDATE {args.table} SET tmdb_poster_path = %s WHERE {pk} = %s"
    counts = collections.Counter()

    def work(row):
        row_id, tmdb_id = row
        path = _fetch_poster_path(api_key, endpoint, tmdb_id)
        return row_id, tmdb_id, path

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(work, r) for r in rows]
        last_commit = time.monotonic()
        pending = 0
        for i, f in enumerate(as_completed(futures), 1):
            row_id, tmdb_id, path = f.result()
            if path:
                cur.execute(update_sql, (path, row_id))
                counts["saved"] += 1
                counts[path.rsplit(".", 1)[-1].lower() if "." in path else "noext"] += 1
                pending += 1
            else:
                counts["no_poster"] += 1
            if pending >= 200 or time.monotonic() - last_commit > 5:
                conn.commit()
                pending = 0
                last_commit = time.monotonic()
            if i % 500 == 0:
                logging.info("Progress: %d/%d — saved=%d no_poster=%d", i, len(rows), counts["saved"], counts["no_poster"])

    conn.commit()
    logging.info("Done — %s", dict(counts))
    return 0 if counts["saved"] or not rows else 1


if __name__ == "__main__":
    sys.exit(main())
