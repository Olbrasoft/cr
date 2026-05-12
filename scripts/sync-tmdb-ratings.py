#!/usr/bin/env python3
"""Refresh `tmdb_rating` + `tmdb_vote_count` for films, series and tv_shows
via the TMDB `/movie/changes` + `/tv/changes` endpoints (#591 — sub-issue
of #588).

Unlike IMDb, TMDB has no public batch dataset with rating values. The
daily refresh therefore uses TMDB's `*/changes` endpoint, which returns
the list of titles that have had *any* change in the past 24 h (rating,
poster, overview, …). We intersect that list with our `tmdb_id` set
and re-fetch only those rows via `/movie/{id}` or `/tv/{id}` to read
the current `vote_average` and `vote_count`.

Typical run: ~200–1000 changed titles in the window across all of TMDB,
of which usually ≤300 are in our DB. With 8 worker threads and TMDB's
~40 req/s effective rate, the whole run finishes in under 30 s.

Idempotent: re-running the same window updates the same rows to the
same values and bumps `tmdb_rating_synced_at`. Failed individual fetches
are logged but don't abort the run.

Usage:
    DATABASE_URL=postgres://... TMDB_API_KEY=... \\
        python3 scripts/sync-tmdb-ratings.py [--days N] [--workers N]

`--days` defaults to 1 (the last 24 h window TMDB itself uses when no
start_date/end_date is supplied). Pass 3-7 for a catch-up window after
an outage; pass 30 once to seed initial coverage if no auto-import ran
in that period.
"""

from __future__ import annotations

import argparse
import collections
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import psycopg2
import psycopg2.extras
import requests

TMDB_BASE = "https://api.themoviedb.org/3"
DEFAULT_WORKERS = 8

# Map our table name → TMDB endpoint kind ("movie" or "tv"). `series` and
# `tv_shows` both go through `/tv/...` — they're separate tables in our
# schema (scripted series vs TV-pořady) but TMDB doesn't distinguish.
TABLE_KIND = {
    "films": "movie",
    "series": "tv",
    "tv_shows": "tv",
}


def _load_our_tmdb_ids(cur) -> dict[tuple[int, str], str]:
    """Return (tmdb_id, kind) → table mapping for every row we may want to
    update. `kind` is "movie" or "tv" so the same tmdb_id can't collide
    across the film/tv namespaces.
    """
    mapping: dict[tuple[int, str], str] = {}
    for table, kind in TABLE_KIND.items():
        cur.execute(
            f"SELECT tmdb_id FROM {table} WHERE tmdb_id IS NOT NULL"
        )
        for (tmdb_id,) in cur:
            mapping[(tmdb_id, kind)] = table
    return mapping


def _fetch_changes(api_key: str, kind: str, start: date, end: date) -> set[int]:
    """Paginate /movie/changes or /tv/changes and return every changed id."""
    ids: set[int] = set()
    page = 1
    while True:
        try:
            r = requests.get(
                f"{TMDB_BASE}/{kind}/changes",
                params={
                    "api_key": api_key,
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                    "page": page,
                },
                timeout=15,
            )
        except requests.RequestException as exc:
            logging.warning("/changes page %d failed: %s — stopping", page, exc)
            break
        if not r.ok:
            logging.warning("/changes page %d returned %s — stopping",
                            page, r.status_code)
            break
        payload = r.json()
        for row in payload.get("results", []):
            tmdb_id = row.get("id")
            if isinstance(tmdb_id, int):
                ids.add(tmdb_id)
        total_pages = payload.get("total_pages", 1)
        if page >= total_pages:
            break
        page += 1
    logging.info("%s/changes: %d changed ids in window %s..%s",
                 kind, len(ids), start, end)
    return ids


def _fetch_rating(api_key: str, kind: str, tmdb_id: int) -> tuple[float | None, int | None]:
    """Return (vote_average, vote_count) for the given title, or (None, None)
    on 404 / non-OK / transient failure. TMDB returns 0.0 for titles
    without votes — we normalise that to None so we don't show "TMDB 0.0".

    Handles 429 with up-to-3 retries that honour the `Retry-After` header.
    The /changes window can return a few hundred IDs at once and ~8
    worker threads keep TMDB's per-IP smoothing happy on average, but
    bursts still hit 429 occasionally — without retry the row would
    silently be left stale.
    """
    for attempt in range(3):
        try:
            r = requests.get(
                f"{TMDB_BASE}/{kind}/{tmdb_id}",
                params={"api_key": api_key},
                timeout=15,
            )
        except requests.RequestException as exc:
            logging.warning("%s/%s fetch failed: %s", kind, tmdb_id, exc)
            return None, None
        if r.status_code == 429:
            wait = float(r.headers.get("Retry-After", "1"))
            time.sleep(min(wait, 10) + 0.1 * attempt)
            continue
        break
    if r.status_code == 404:
        return None, None
    if not r.ok:
        logging.warning("%s/%s returned %s", kind, tmdb_id, r.status_code)
        return None, None
    body = r.json()
    vote_average = body.get("vote_average")
    vote_count = body.get("vote_count")
    if not vote_average or not vote_count:
        # vote_count==0 means TMDB has the row but nobody rated it. The
        # column on our side stays unchanged in that case (a previous
        # value, if any, is still more useful than overwriting with NULL).
        return None, None
    return float(vote_average), int(vote_count)


def _flush(cur, table: str, batch: list[tuple]) -> None:
    """Apply one batch of (rating, votes, tmdb_id) updates for `table`."""
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


def sync(conn, api_key: str, days: int, workers: int) -> dict[str, int]:
    cur = conn.cursor()
    mapping = _load_our_tmdb_ids(cur)
    logging.info("Loaded %d (tmdb_id, kind) pairs from our DB", len(mapping))

    end = date.today()
    start = end - timedelta(days=days)

    # TMDB exposes /changes per kind ("movie" / "tv"); fetch each once.
    changes_movie = _fetch_changes(api_key, "movie", start, end)
    changes_tv = _fetch_changes(api_key, "tv", start, end)

    # Intersect with our IDs. A single key in `mapping` is (tmdb_id, kind);
    # the change list for a kind is just IDs — combine them per-kind.
    targets: list[tuple[int, str, str]] = []  # (tmdb_id, kind, table)
    for tmdb_id in changes_movie:
        table = mapping.get((tmdb_id, "movie"))
        if table:
            targets.append((tmdb_id, "movie", table))
    for tmdb_id in changes_tv:
        table = mapping.get((tmdb_id, "tv"))
        if table:
            targets.append((tmdb_id, "tv", table))

    logging.info("Matched %d targets to refresh (films=%d, series=%d, tv_shows=%d)",
                 len(targets),
                 sum(1 for t in targets if t[2] == "films"),
                 sum(1 for t in targets if t[2] == "series"),
                 sum(1 for t in targets if t[2] == "tv_shows"))

    batches: dict[str, list[tuple]] = {t: [] for t in TABLE_KIND}
    counts: dict[str, int] = collections.Counter()
    BATCH_SIZE = 200

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_rating, api_key, kind, tmdb_id): (tmdb_id, table)
            for tmdb_id, kind, table in targets
        }
        for fut in as_completed(futures):
            tmdb_id, table = futures[fut]
            rating, votes = fut.result()
            if rating is None:
                counts[f"{table}_no_change"] += 1
                continue
            batches[table].append((rating, votes, tmdb_id))
            counts[f"{table}_refreshed"] += 1
            if len(batches[table]) >= BATCH_SIZE:
                _flush(cur, table, batches[table])
                batches[table].clear()

    for table, batch in batches.items():
        _flush(cur, table, batch)
    conn.commit()

    elapsed = time.monotonic() - started
    logging.info(
        "Done in %.1fs — refreshed: films=%d series=%d tv_shows=%d "
        "(skipped: films=%d series=%d tv_shows=%d)",
        elapsed,
        counts["films_refreshed"], counts["series_refreshed"], counts["tv_shows_refreshed"],
        counts["films_no_change"], counts["series_no_change"], counts["tv_shows_no_change"],
    )
    return dict(counts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--days", type=int, default=1,
                        help="window size in days (default 1 = last 24 h)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
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
    try:
        sync(conn, api_key, args.days, args.workers)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
