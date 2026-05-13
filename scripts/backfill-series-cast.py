#!/usr/bin/env python3
"""Backfill `series_actors` + `series_directors` from TMDB credits (#720).

1082 of 1878 series with `tmdb_id` have zero entries in `series_actors`,
so the `/serialy-online/{slug}/` detail page renders no Tvůrci/Herci
sections at all. This script fills the gap by calling TMDB
`/tv/{tmdb_id}/credits` for each empty series and:

  1. UPSERT into `people` (key: `tmdb_id`). Sets `profile_filename =
     'p{tmdb_id}.webp'` when TMDB returns a `profile_path`, NULL otherwise
     — matches the synthesized filename the new photo handler proxies.
  2. INSERT top-N cast rows into `series_actors` (default N=10, sorted by
     TMDB `order`). `order_index` mirrors `order` so rendering stays
     consistent with shows backfilled by the existing import pipeline.
  3. INSERT into `series_directors` for crew with `job` in
     `{'Director', 'Creator'}` plus anyone listed under TMDB's
     `created_by` (TV shows store their primary creator there, not in
     crew). De-duplicates against `(series_id, person_id)` PK.

After running, kick off `scripts/backfill-person-photos.py` to upload
WebP for the newly-inserted persons — that script picks them up via
`profile_filename IS NOT NULL` so they're already in its work set.

Idempotency: a series is processed only when `series_actors` is empty
for it. Re-runs report `skipped` for already-filled rows. We never
DELETE existing cast — even partial human curation is preserved.

Usage:
  DATABASE_URL=postgres://...@localhost/cr_dev \\
  TMDB_API_KEY=... \\
      python3 scripts/backfill-series-cast.py \\
          --jobs 8 \\
          --limit 50
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass

try:
    import psycopg2
    import requests
except ImportError as e:
    print(f"ERROR: missing dependency ({e.name}). "
          "pip install psycopg2-binary requests",
          file=sys.stderr)
    sys.exit(2)

log = logging.getLogger("backfill-series-cast")

TMDB_API_BASE = "https://api.themoviedb.org/3"
DEFAULT_TIMEOUT = 20

# Top-N cast cap. The series detail page renders 10 actors max
# (cr-web/src/handlers/series.rs:1180 has `LIMIT 10`), so storing more
# is wasted bytes. Order is already sorted by `order` in the API
# response — TMDB's billing-order ranking.
DEFAULT_CAST_LIMIT = 10

# Crew jobs that map to series_directors. "Creator" doesn't usually appear
# in the TV crew array (TMDB puts creators under `created_by`), but we
# accept it for safety. "Executive Producer" is intentionally excluded —
# the table is named `series_directors`, and EPs are typically too
# numerous and too divorced from the show's creative direction to be
# useful page content.
DIRECTOR_JOBS = {"Director", "Creator"}


@dataclass
class CreditRow:
    tmdb_id: int
    name: str
    profile_path: str | None
    character: str | None
    order: int | None
    job: str | None  # None for cast, set for crew/creators


def _request_tmdb(
    session: requests.Session, path: str, api_key: str, retries: int = 3
) -> dict | None:
    """GET TMDB endpoint with Retry-After-aware backoff.

    Returns parsed JSON dict on 200, None on 404 (series not on TMDB) or
    exhausted retries. The caller treats None as "skip this series this
    run" — we never destructively NULL anything based on TMDB read errors.
    """
    url = f"{TMDB_API_BASE}{path}"
    for attempt in range(retries):
        try:
            r = session.get(url, params={"api_key": api_key}, timeout=DEFAULT_TIMEOUT)
        except requests.RequestException as e:
            log.warning("%s attempt %d failed: %s",
                        path, attempt + 1, type(e).__name__)
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            log.warning("rate-limited on %s; sleeping %ds", path, wait)
            time.sleep(wait)
            continue
        if r.status_code != 200:
            log.warning("%s HTTP %d", path, r.status_code)
            return None
        try:
            return r.json()
        except ValueError:
            return None
    return None


def _fetch_credits(
    session: requests.Session, tmdb_id: int, api_key: str
) -> tuple[list[CreditRow], list[CreditRow]] | None:
    """Return (cast, directors_and_creators) for a TV show.

    None means "skip this series" (network or 404). Empty lists mean
    "TMDB has the show but no people on it" — caller logs and moves on
    without writing rows.

    We pull `/tv/{id}` (for `created_by`) and `/tv/{id}/credits` in two
    calls. Could be combined via `append_to_response=credits` but two
    GETs keep retry behavior trivial and the cost is irrelevant.
    """
    main = _request_tmdb(session, f"/tv/{tmdb_id}", api_key)
    if main is None:
        return None
    credits = _request_tmdb(session, f"/tv/{tmdb_id}/credits", api_key)
    if credits is None:
        return None

    cast: list[CreditRow] = []
    for c in credits.get("cast", []) or []:
        if not c.get("id") or not c.get("name"):
            continue
        cast.append(CreditRow(
            tmdb_id=int(c["id"]),
            name=c["name"],
            profile_path=c.get("profile_path"),
            character=c.get("character") or None,
            order=c.get("order"),
            job=None,
        ))

    director_rows: list[CreditRow] = []
    seen_director_ids: set[int] = set()

    # Creators come from the `/tv/{id}` payload's `created_by` array,
    # not crew. Most TV shows in our DB have at least one creator there.
    for cr in main.get("created_by", []) or []:
        if not cr.get("id") or not cr.get("name"):
            continue
        pid = int(cr["id"])
        if pid in seen_director_ids:
            continue
        seen_director_ids.add(pid)
        director_rows.append(CreditRow(
            tmdb_id=pid,
            name=cr["name"],
            profile_path=cr.get("profile_path"),
            character=None,
            order=None,
            job="Creator",
        ))

    # Then crew rows whose job matches our director-class allowlist.
    for cw in credits.get("crew", []) or []:
        if cw.get("job") not in DIRECTOR_JOBS:
            continue
        if not cw.get("id") or not cw.get("name"):
            continue
        pid = int(cw["id"])
        if pid in seen_director_ids:
            continue
        seen_director_ids.add(pid)
        director_rows.append(CreditRow(
            tmdb_id=pid,
            name=cw["name"],
            profile_path=cw.get("profile_path"),
            character=None,
            order=None,
            job=cw["job"],
        ))

    return cast, director_rows


# Per-connection lock keeps thread workers from interleaving statements
# on the same psycopg2 connection (which is NOT thread-safe). We open
# one connection per worker via threading.local instead, which is even
# simpler.
_conn_local = threading.local()


def _get_conn(dsn: str):
    conn = getattr(_conn_local, "conn", None)
    if conn is None or conn.closed:
        conn = psycopg2.connect(dsn)
        _conn_local.conn = conn
    return conn


def _upsert_person(cur, row: CreditRow) -> int:
    """UPSERT into people by tmdb_id. Returns people.id.

    `profile_filename` is set only when TMDB returns a `profile_path` —
    matches the convention the photo backfill uses. Existing rows keep
    their filename even if TMDB has since removed the photo; the photo
    backfill is what decides to NULL it (after a confirmed-missing TMDB
    response), not the cast backfill.
    """
    if row.profile_path:
        filename = f"p{row.tmdb_id}.webp"
    else:
        filename = None
    cur.execute(
        "INSERT INTO people (tmdb_id, name, profile_filename) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (tmdb_id) DO UPDATE SET "
        "    name = EXCLUDED.name, "
        # Don't overwrite a present filename with NULL — if TMDB lost
        # the photo we want the photo backfill to decide that, not the
        # cast UPSERT which can fire for unrelated reasons.
        "    profile_filename = COALESCE(EXCLUDED.profile_filename, people.profile_filename) "
        "RETURNING id",
        (row.tmdb_id, row.name, filename),
    )
    return cur.fetchone()[0]


def _process_series(
    series_id: int,
    tmdb_id: int,
    dsn: str,
    api_key: str,
    cast_limit: int,
    dry_run: bool,
) -> tuple[int, str, int, int]:
    """Returns (series_id, status, actors_inserted, directors_inserted)."""
    session = requests.Session()
    result = _fetch_credits(session, tmdb_id, api_key)
    if result is None:
        return series_id, "tmdb_error", 0, 0
    cast, directors = result
    if not cast and not directors:
        return series_id, "empty_credits", 0, 0

    if dry_run:
        return series_id, "ok_dry", min(len(cast), cast_limit), len(directors)

    conn = _get_conn(dsn)
    actors_inserted = 0
    directors_inserted = 0
    try:
        cur = conn.cursor()

        # Cast: take top-N by TMDB order (already sorted but be defensive).
        cast_sorted = sorted(cast, key=lambda r: r.order if r.order is not None else 999)
        for c in cast_sorted[:cast_limit]:
            pid = _upsert_person(cur, c)
            # ON CONFLICT DO NOTHING keeps human curation in case the
            # row exists with a different order_index — first writer wins.
            # RETURNING + fetchone() is more reliable than rowcount for
            # `ON CONFLICT DO NOTHING` — psycopg2's rowcount on a no-op
            # conflict can read as 0 even when a fresh INSERT actually
            # landed (observed under threaded use). fetchone() returns
            # a row only on real insert, None on conflict.
            cur.execute(
                "INSERT INTO series_actors "
                "(series_id, person_id, character_name, order_index) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (series_id, person_id) DO NOTHING "
                "RETURNING person_id",
                (series_id, pid, c.character, c.order or 0),
            )
            if cur.fetchone() is not None:
                actors_inserted += 1

        for d in directors:
            pid = _upsert_person(cur, d)
            cur.execute(
                "INSERT INTO series_directors (series_id, person_id) "
                "VALUES (%s, %s) ON CONFLICT (series_id, person_id) DO NOTHING "
                "RETURNING person_id",
                (series_id, pid),
            )
            if cur.fetchone() is not None:
                directors_inserted += 1

        conn.commit()
        return series_id, "ok", actors_inserted, directors_inserted
    except psycopg2.Error as e:
        conn.rollback()
        log.warning("db error for series_id=%d tmdb=%d: %s", series_id, tmdb_id, e)
        return series_id, "db_error", 0, 0


def _fetch_candidates(
    dsn: str, limit: int | None
) -> list[tuple[int, int]]:
    """Return [(series_id, tmdb_id), ...] for series with no cast yet.

    NOT EXISTS is faster than LEFT JOIN + IS NULL on this schema; the
    actors PK is (series_id, person_id), so the planner stops at the
    first match. Sorted by `added_at DESC` so freshly-imported shows
    that are visible on the listing fill in first.
    """
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        sql = (
            "SELECT s.id, s.tmdb_id FROM series s "
            "WHERE s.tmdb_id IS NOT NULL "
            "  AND NOT EXISTS ("
            "    SELECT 1 FROM series_actors sa WHERE sa.series_id = s.id"
            "  ) "
            "ORDER BY s.added_at DESC NULLS LAST, s.id"
        )
        if limit is not None:
            cur.execute(sql + " LIMIT %s", (int(limit),))
        else:
            cur.execute(sql)
        return list(cur.fetchall())
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--limit", type=int, help="Process only first N series")
    ap.add_argument("--cast-limit", type=int, default=DEFAULT_CAST_LIMIT,
                    help=f"Max cast rows per series (default {DEFAULT_CAST_LIMIT})")
    ap.add_argument("--dry-run", action="store_true",
                    help="Skip DB writes (test TMDB pipeline only)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL env var is required")
        return 2
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not api_key:
        log.error("TMDB_API_KEY env var is required")
        return 2

    rows = _fetch_candidates(dsn, args.limit)
    log.info("processing %d series with %d workers", len(rows), args.jobs)

    stats = {
        "ok": 0, "ok_dry": 0, "empty_credits": 0,
        "tmdb_error": 0, "db_error": 0,
    }
    actor_total = 0
    director_total = 0

    if args.dry_run:
        log.warning("--dry-run: no DB writes")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futures = [
            ex.submit(_process_series, sid, tmdb_id, dsn, api_key,
                      args.cast_limit, args.dry_run)
            for sid, tmdb_id in rows
        ]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            _sid, status, a, d = fut.result()
            stats[status] += 1
            actor_total += a
            director_total += d
            if i % 25 == 0 or i == len(rows):
                log.info(
                    "progress %d/%d ok=%d empty=%d tmdb_err=%d db_err=%d | actors=%d directors=%d",
                    i, len(rows), stats["ok"] + stats["ok_dry"],
                    stats["empty_credits"], stats["tmdb_error"],
                    stats["db_error"], actor_total, director_total,
                )

    log.info("DONE: %s | actors=%d directors=%d",
             stats, actor_total, director_total)
    return 0 if stats["tmdb_error"] == 0 and stats["db_error"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
