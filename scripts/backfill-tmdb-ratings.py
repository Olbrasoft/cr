#!/usr/bin/env python3
"""One-shot backfill of `tmdb_rating` + `tmdb_vote_count` for the long
tail of titles that have `tmdb_id` set but no `tmdb_rating` (#698 —
follow-up to #591 / #588).

Background: the nightly `sync-tmdb-ratings.py` only refreshes titles
that changed on TMDB in the last 24 h (via `/movie/changes` +
`/tv/changes`). Older imports — and anything that was created before
the rating column existed (#069) — sit forever with NULL ratings,
because they simply never appear in a /changes payload. The result is
visible on `/serialy-online/` and `/filmy-online/` as cards with only
the yellow IMDb badge and no blue TMDB one.

This script closes that gap once. For each row where `tmdb_id IS NOT
NULL AND tmdb_rating IS NULL`, it fetches `/movie/{id}` or `/tv/{id}`,
reads `vote_average` + `vote_count`, and UPDATEs the row. Titles with
`vote_count == 0` are skipped (TMDB has the title but nobody rated
it yet — same rule as the daily sync).

The whole run is logged as a single row in `rating_sync_runs` with
kind='tmdb' so the admin dashboard reflects it.

Usage:
    DATABASE_URL=postgres://... TMDB_API_KEY=... \\
        python3 scripts/backfill-tmdb-ratings.py [--dry-run] [--workers N] [--limit N]
"""

from __future__ import annotations

import argparse
import collections
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import psycopg2
import psycopg2.extras
import requests

TMDB_BASE = "https://api.themoviedb.org/3"
DEFAULT_WORKERS = 8

# Map our table → TMDB endpoint kind. `series` and `tv_shows` both go
# through `/tv/...` even though they're separate tables in our schema
# (scripted series vs TV pořady) — TMDB doesn't distinguish.
TABLES = (
    ("films", "movie"),
    ("series", "tv"),
    ("tv_shows", "tv"),
)


class _Outcome:
    """Granular tally so the run summary distinguishes "no votes yet"
    from a real fetch failure. The daily sync uses the same discriminator
    (see sync-tmdb-ratings.py::_FetchOutcome) — kept independent here so
    this script can run on hosts that don't ship the daily one yet."""
    OK = "ok"
    NOT_FOUND = "not_found"
    NO_VOTES = "no_votes"
    FAILED = "failed"


def _parse_retry_after(value: str) -> float:
    """Retry-After can be delta-seconds (RFC 7231) or an HTTP-date; both
    forms are valid. Default to 1 s on anything unparseable so a
    misbehaving header doesn't abort the whole run."""
    if not value:
        return 1.0
    try:
        return float(value)
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
            return max((target - datetime.now(timezone.utc)).total_seconds(), 1.0)
        except (TypeError, ValueError):
            return 1.0


def _fetch_rating(
    api_key: str, kind: str, tmdb_id: int
) -> tuple[float | None, int | None, str]:
    """Return (vote_average, vote_count, outcome) for the given title.

    Up to 3 retries on 429, honouring `Retry-After`. The script runs in
    a single burst over ~1300 rows; without the retry a transient
    smoothing-limit would silently leave gaps."""
    r = None
    for attempt in range(3):
        try:
            r = requests.get(
                f"{TMDB_BASE}/{kind}/{tmdb_id}",
                params={"api_key": api_key},
                timeout=15,
            )
        except requests.RequestException as exc:
            logging.warning("%s/%s fetch failed: %s", kind, tmdb_id, exc)
            return None, None, _Outcome.FAILED
        if r.status_code == 429:
            wait = _parse_retry_after(r.headers.get("Retry-After", ""))
            time.sleep(min(wait, 10) + 0.1 * attempt)
            continue
        break
    if r is None or r.status_code == 429:
        return None, None, _Outcome.FAILED
    if r.status_code == 404:
        return None, None, _Outcome.NOT_FOUND
    if not r.ok:
        logging.warning("%s/%s returned %s", kind, tmdb_id, r.status_code)
        return None, None, _Outcome.FAILED
    body = r.json()
    vote_average = body.get("vote_average")
    vote_count = body.get("vote_count")
    if not vote_average or not vote_count:
        return None, None, _Outcome.NO_VOTES
    return float(vote_average), int(vote_count), _Outcome.OK


def _load_targets(cur, table: str, limit: int) -> list[int]:
    """Return tmdb_ids for rows with tmdb_id set but tmdb_rating NULL."""
    sql = (
        f"SELECT tmdb_id FROM {table} "
        "WHERE tmdb_id IS NOT NULL AND tmdb_rating IS NULL "
        "ORDER BY id"
    )
    if limit > 0:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return [row[0] for row in cur]


def _flush(cur, table: str, batch: list[tuple]) -> None:
    if not batch:
        return
    psycopg2.extras.execute_values(
        cur,
        f"""
        UPDATE {table} AS t
           SET tmdb_rating = v.rating,
               tmdb_vote_count = v.votes,
               tmdb_rating_synced_at = now()
          FROM (VALUES %s) AS v(rating, votes, tmdb_id)
         WHERE t.tmdb_id = v.tmdb_id
        """,
        batch,
        template="(%s, %s, %s)",
        page_size=len(batch),
    )


def _insert_run(conn) -> int | None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rating_sync_runs (kind, status) VALUES ('tmdb', 'running') RETURNING id"
            )
            run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    except psycopg2.Error as e:
        conn.rollback()
        logging.warning("rating_sync_runs INSERT failed (migration 070?): %s", e)
        return None


def _finalize_run(
    conn,
    run_id: int | None,
    status: str,
    counts: dict[str, int],
    error_message: str | None,
) -> None:
    if run_id is None:
        return
    # Match sync-tmdb-ratings.py: `failed_count` = 404 + transient fetch
    # failures only. `_no_votes` stays a logs-only counter.
    failed_count = sum(
        v
        for k, v in counts.items()
        if k.endswith("_failed") or k.endswith("_not_found")
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE rating_sync_runs
                   SET finished_at = now(),
                       status = %s,
                       films_refreshed = %s,
                       series_refreshed = %s,
                       tv_shows_refreshed = %s,
                       failed_count = %s,
                       error_message = %s
                 WHERE id = %s
                """,
                (
                    status,
                    counts.get("films_refreshed", 0),
                    counts.get("series_refreshed", 0),
                    counts.get("tv_shows_refreshed", 0),
                    failed_count,
                    error_message,
                    run_id,
                ),
            )
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.warning("rating_sync_runs UPDATE failed: %s", e)


def backfill(
    conn,
    api_key: str,
    workers: int,
    dry_run: bool,
    limit: int,
) -> dict[str, int]:
    counts: dict[str, int] = collections.Counter()
    cur = conn.cursor()

    for table, kind in TABLES:
        targets = _load_targets(cur, table, limit)
        logging.info(
            "%s: %d rows need tmdb_rating (kind=%s)", table, len(targets), kind
        )
        if not targets:
            continue

        BATCH_SIZE = 200
        batch: list[tuple] = []
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_fetch_rating, api_key, kind, tmdb_id): tmdb_id
                for tmdb_id in targets
            }
            done = 0
            for fut in as_completed(futures):
                tmdb_id = futures[fut]
                rating, votes, outcome = fut.result()
                done += 1
                if done % 100 == 0:
                    logging.info(
                        "%s: %d/%d processed (%.1fs)",
                        table,
                        done,
                        len(targets),
                        time.monotonic() - started,
                    )
                if outcome == _Outcome.OK:
                    batch.append((rating, votes, tmdb_id))
                    counts[f"{table}_refreshed"] += 1
                    if len(batch) >= BATCH_SIZE and not dry_run:
                        _flush(cur, table, batch)
                        batch.clear()
                else:
                    counts[f"{table}_{outcome}"] += 1

        if not dry_run:
            _flush(cur, table, batch)
            conn.commit()
        else:
            logging.info("[dry-run] skipping UPDATE + COMMIT for %s", table)

        elapsed = time.monotonic() - started
        logging.info(
            "%s done in %.1fs — refreshed=%d no_votes=%d not_found=%d failed=%d",
            table,
            elapsed,
            counts.get(f"{table}_refreshed", 0),
            counts.get(f"{table}_no_votes", 0),
            counts.get(f"{table}_not_found", 0),
            counts.get(f"{table}_failed", 0),
        )

    return dict(counts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--limit", type=int, default=0,
                        help="0 = process all rows; >0 caps per-table for testing")
    parser.add_argument("--dry-run", action="store_true",
                        help="fetch + tally but do not UPDATE / COMMIT")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    dsn = os.environ.get("DATABASE_URL", "").strip()
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not dsn:
        raise SystemExit("DATABASE_URL required")
    if not api_key:
        raise SystemExit("TMDB_API_KEY required")

    conn = psycopg2.connect(dsn)
    # Skip run tracking on dry-run — admin dashboard should only reflect
    # real production updates, not ad-hoc probing.
    run_id = None if args.dry_run else _insert_run(conn)
    status = "ok"
    error_message: str | None = None
    counts: dict[str, int] = {}
    try:
        counts = backfill(conn, api_key, args.workers, args.dry_run, args.limit)
    except Exception as e:
        status = "error"
        error_message = (str(e) or repr(e))[:500]
        _finalize_run(conn, run_id, status, counts, error_message)
        conn.close()
        raise
    finally:
        if status == "ok":
            _finalize_run(conn, run_id, status, counts, error_message)
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
