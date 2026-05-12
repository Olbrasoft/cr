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
start_date/end_date is supplied). Pass 3–7 for a catch-up window after
an outage. **TMDB caps the window at 14 days** — anything larger gets
silently truncated on the API side, so the script clamps and warns.
After multi-day downtime exceeding the cap you have to re-seed via the
full /movie/{id} backfill in scripts/backfill-tmdb-imdb-ids.py-style.
"""

from __future__ import annotations

import argparse
import collections
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

TMDB_BASE = "https://api.themoviedb.org/3"
DEFAULT_WORKERS = 8

# TMDB caps /movie/changes + /tv/changes at a 14-day window. Anything
# wider gets silently truncated on the API side, so we clamp here and
# emit a warning instead of letting a 30-day --days hide records.
MAX_WINDOW_DAYS = 14

# Persists "last successful run" so an outage that's still within the
# 14-day cap is auto-recovered on the next tick without needing
# someone to remember --days. Single-line file (UTC ISO date).
DEFAULT_STATE_DIR = Path("data/imdb-cache")
STATE_FILE_NAME = "tmdb-sync-last-run.txt"

# Map our table name → TMDB endpoint kind ("movie" or "tv"). `series` and
# `tv_shows` both go through `/tv/...` — they're separate tables in our
# schema (scripted series vs TV-pořady) but TMDB doesn't distinguish.
TABLE_KIND = {
    "films": "movie",
    "series": "tv",
    "tv_shows": "tv",
}


def _load_our_tmdb_ids(cur) -> dict[tuple[int, str], list[str]]:
    """Return (tmdb_id, kind) → list-of-tables mapping. `kind` is "movie"
    or "tv" so the film/tv namespaces don't collide, but `series` and
    `tv_shows` share the "tv" namespace, and a curator can put the same
    TMDB id on rows in both tables (e.g. a scripted series later
    reclassified as a TV pořad without removing the original row). When
    that happens we want to update BOTH tables — a single-table mapping
    would silently drop one.
    """
    mapping: dict[tuple[int, str], list[str]] = {}
    collisions = 0
    for table, kind in TABLE_KIND.items():
        cur.execute(
            f"SELECT tmdb_id FROM {table} WHERE tmdb_id IS NOT NULL"
        )
        for (tmdb_id,) in cur:
            key = (tmdb_id, kind)
            existing = mapping.get(key)
            if existing is None:
                mapping[key] = [table]
            else:
                existing.append(table)
                collisions += 1
                logging.info("tmdb_id %d appears in both %s and %s (kind=%s) — "
                             "will update both",
                             tmdb_id, existing[0], table, kind)
    if collisions:
        logging.info("Detected %d cross-table tmdb_id collisions", collisions)
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


class _FetchOutcome:
    """Discrete reasons a /movie/{id} or /tv/{id} fetch did NOT yield a
    rating. Returned alongside the rating tuple so the run summary can
    distinguish "TMDB has no votes for this title" from "fetch failed"
    — the original single `no_change` bucket conflated both, which
    masked transient outages from ops/debugging.
    """
    OK = "ok"
    NOT_FOUND = "not_found"   # HTTP 404
    NO_VOTES = "no_votes"     # vote_count==0 — title exists, no rating data
    FAILED = "failed"         # transient network / 5xx / unparseable JSON


def _parse_retry_after(value: str) -> float:
    """`Retry-After` can be either delta-seconds (RFC 7231) or an HTTP-date.
    Parse defensively so a date-form value doesn't blow up `float()` and
    abort the whole run.
    """
    if not value:
        return 1.0
    try:
        return float(value)
    except ValueError:
        # HTTP-date form: parse and subtract now. Stdlib has email.utils.
        from email.utils import parsedate_to_datetime
        try:
            target = parsedate_to_datetime(value)
            delta = (target - datetime.now(timezone.utc)).total_seconds()
            return max(delta, 1.0)
        except (TypeError, ValueError):
            return 1.0


def _fetch_rating(api_key: str, kind: str, tmdb_id: int) -> tuple[float | None, int | None, str]:
    """Return (vote_average, vote_count, outcome) for the given title.
    `outcome` is one of `_FetchOutcome` so the caller can tally the
    reason for any non-`OK` result.

    Handles 429 with up-to-3 retries that honour the `Retry-After`
    header (both delta-seconds and HTTP-date forms). The /changes
    window can return a few hundred IDs at once and ~8 worker threads
    keep TMDB's per-IP smoothing happy on average, but bursts still
    hit 429 occasionally — without retry the row would silently be
    left stale.
    """
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
            return None, None, _FetchOutcome.FAILED
        if r.status_code == 429:
            wait = _parse_retry_after(r.headers.get("Retry-After", ""))
            time.sleep(min(wait, 10) + 0.1 * attempt)
            continue
        break
    if r is None or r.status_code == 429:
        return None, None, _FetchOutcome.FAILED
    if r.status_code == 404:
        return None, None, _FetchOutcome.NOT_FOUND
    if not r.ok:
        logging.warning("%s/%s returned %s", kind, tmdb_id, r.status_code)
        return None, None, _FetchOutcome.FAILED
    body = r.json()
    vote_average = body.get("vote_average")
    vote_count = body.get("vote_count")
    if not vote_average or not vote_count:
        # vote_count==0 means TMDB has the row but nobody rated it. The
        # column on our side stays unchanged in that case (a previous
        # value, if any, is still more useful than overwriting with NULL).
        return None, None, _FetchOutcome.NO_VOTES
    return float(vote_average), int(vote_count), _FetchOutcome.OK


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


def _load_state(state_dir: Path) -> date | None:
    """Return the UTC date of the last successful run, or None on first run."""
    p = state_dir / STATE_FILE_NAME
    if not p.exists():
        return None
    try:
        return date.fromisoformat(p.read_text().strip())
    except (OSError, ValueError):
        return None


def _save_state(state_dir: Path, today: date) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / STATE_FILE_NAME).write_text(today.isoformat())


def sync(
    conn,
    api_key: str,
    days: int,
    workers: int,
    state_dir: Path,
) -> dict[str, int]:
    cur = conn.cursor()
    mapping = _load_our_tmdb_ids(cur)
    logging.info("Loaded %d unique (tmdb_id, kind) pairs from our DB", len(mapping))

    # Window selection (UTC — the timer runs in UTC and `date.today()`
    # would read host-local, which would be off by ±1 day around midnight
    # in non-UTC timezones).
    end = datetime.now(timezone.utc).date()
    last_run = _load_state(state_dir)
    if last_run and last_run <= end:
        # auto-recover from an outage: window goes from the last
        # successful run to today. Clamped to MAX_WINDOW_DAYS — anything
        # older needs a manual full backfill since TMDB's /changes API
        # itself doesn't go further back.
        gap = (end - last_run).days
        if gap > MAX_WINDOW_DAYS:
            logging.warning(
                "Last successful run was %d days ago (state=%s) — TMDB /changes "
                "caps the window at %d days, older rows will need a manual "
                "backfill",
                gap, last_run, MAX_WINDOW_DAYS,
            )
            days = MAX_WINDOW_DAYS
        else:
            days = max(gap, days)
    if days > MAX_WINDOW_DAYS:
        logging.warning("--days %d clamped to TMDB max window %d",
                        days, MAX_WINDOW_DAYS)
        days = MAX_WINDOW_DAYS
    start = end - timedelta(days=days)

    # TMDB exposes /changes per kind ("movie" / "tv"); fetch each once.
    changes_movie = _fetch_changes(api_key, "movie", start, end)
    changes_tv = _fetch_changes(api_key, "tv", start, end)

    # Intersect with our IDs. mapping values are list[str] of tables —
    # cross-table collisions get expanded to one target per table.
    targets: list[tuple[int, str, str]] = []  # (tmdb_id, kind, table)
    for tmdb_id in changes_movie:
        for table in mapping.get((tmdb_id, "movie"), ()):
            targets.append((tmdb_id, "movie", table))
    for tmdb_id in changes_tv:
        for table in mapping.get((tmdb_id, "tv"), ()):
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
            rating, votes, outcome = fut.result()
            if outcome == _FetchOutcome.OK:
                batches[table].append((rating, votes, tmdb_id))
                counts[f"{table}_refreshed"] += 1
                if len(batches[table]) >= BATCH_SIZE:
                    _flush(cur, table, batches[table])
                    batches[table].clear()
            else:
                # Track each failure mode separately so ops can spot a
                # transient TMDB outage in the run summary instead of it
                # being lumped under a single "skipped" bucket.
                counts[f"{table}_{outcome}"] += 1

    for table, batch in batches.items():
        _flush(cur, table, batch)
    conn.commit()

    # Only stamp state on success — if the DB commit raised, the next run
    # should retry the same window rather than skip forward.
    _save_state(state_dir, end)

    elapsed = time.monotonic() - started
    logging.info(
        "Done in %.1fs — refreshed films=%d series=%d tv_shows=%d / "
        "no_votes f=%d s=%d t=%d / not_found f=%d s=%d t=%d / failed f=%d s=%d t=%d",
        elapsed,
        counts["films_refreshed"], counts["series_refreshed"], counts["tv_shows_refreshed"],
        counts["films_no_votes"], counts["series_no_votes"], counts["tv_shows_no_votes"],
        counts["films_not_found"], counts["series_not_found"], counts["tv_shows_not_found"],
        counts["films_failed"], counts["series_failed"], counts["tv_shows_failed"],
    )
    return dict(counts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--days", type=int, default=1,
        help=f"window size in days (default 1; max {MAX_WINDOW_DAYS})",
    )
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument(
        "--state-dir", default=str(DEFAULT_STATE_DIR),
        help="directory holding the last-run state file",
    )
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
        sync(conn, api_key, args.days, args.workers, Path(args.state_dir))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
