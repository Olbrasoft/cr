#!/usr/bin/env python3
"""Backfill `film_actors` + `film_directors` from TMDB credits.

Companion to `backfill-series-cast.py` — same UPSERT pattern, same
`people` table, just driven by `/movie/{tmdb_id}/credits` instead of
`/tv/{tmdb_id}/credits`. For movies there's no `created_by` analog;
all directors come from `credits.crew` rows with `job == 'Director'`.

  1. UPSERT into `people` (key: `tmdb_id`). Sets `profile_filename =
     'p{tmdb_id}.webp'` when TMDB returns a `profile_path`, NULL otherwise.
  2. INSERT top-N cast rows into `film_actors` (default N=10, sorted by
     TMDB `order`). `order_index` mirrors `order`.
  3. INSERT into `film_directors` for crew with `job == 'Director'`.
     De-duplicates against `(film_id, person_id)` PK.

After running, kick off `scripts/backfill-person-photos.py` so newly-
inserted persons get their WebP uploaded to R2.

Idempotency: a film is processed only when BOTH `film_actors` and
`film_directors` are empty for it (`_fetch_candidates` filters
already-populated films out at the SQL level — they never enter the
worker pool, so there's no `skipped` status to tally). We never
DELETE existing cast — even partial human curation is preserved.

Usage:
  DATABASE_URL=postgres://...@localhost/cr_dev \\
  TMDB_API_KEY=... \\
      python3 scripts/backfill-film-cast.py \\
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

log = logging.getLogger("backfill-film-cast")

TMDB_API_BASE = "https://api.themoviedb.org/3"
DEFAULT_TIMEOUT = 20

# Top-N cast cap. Detail page renders 10 actors max — see
# cr-web/src/handlers/films.rs `films_detail`.
DEFAULT_CAST_LIMIT = 10

# Movies are simpler than TV — only "Director" maps to film_directors.
# Producer/Writer/etc. don't get their own table; if we want them later
# they'd need new join tables and template sections. EPs are typically
# the producers' chair, not creative direction.
DIRECTOR_JOBS = {"Director"}

# `film_actors.character_name` is VARCHAR(255) and `people.name` is
# VARCHAR(255). A handful of TMDB rows carry strings well past that —
# slash-joined cast lists from anthology shows, joint stage names, etc.
# Truncate on the client rather than upsize the columns for every row.
CHAR_NAME_MAX = 255
PERSON_NAME_MAX = 255

# `film_actors.order_index` is SMALLINT (i.e. signed 16-bit, max 32 767).
# TMDB `order` is normally 0..200 but a buggy credits row can carry an
# outlier well past the SMALLINT range; clamp before insert. Missing
# `order` sorts last (`_UNORDERED_RANK`) and stores as the SMALLINT
# maximum so it doesn't shadow billed actors during display.
_SMALLINT_MAX = 32_767
_UNORDERED_RANK = 9_999


@dataclass
class CreditRow:
    tmdb_id: int
    name: str
    profile_path: str | None
    character: str | None
    order: int | None
    job: str | None  # None for cast, set for crew


def _request_tmdb(
    session: requests.Session, path: str, api_key: str, retries: int = 3
) -> dict | None:
    """GET TMDB with Retry-After backoff. None on 404 or exhausted retries."""
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
    """Return (cast, directors). None means skip this film."""
    credits = _request_tmdb(session, f"/movie/{tmdb_id}/credits", api_key)
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
    seen: set[int] = set()
    for cw in credits.get("crew", []) or []:
        if cw.get("job") not in DIRECTOR_JOBS:
            continue
        if not cw.get("id") or not cw.get("name"):
            continue
        pid = int(cw["id"])
        if pid in seen:
            continue
        seen.add(pid)
        director_rows.append(CreditRow(
            tmdb_id=pid,
            name=cw["name"],
            profile_path=cw.get("profile_path"),
            character=None,
            order=None,
            job=cw["job"],
        ))

    return cast, director_rows


# Per-thread connection (psycopg2 isn't thread-safe across connections).
# Per-thread `requests.Session` as well — re-creating one per film would
# tear down/redial HTTPS for every TMDB request and waste ~80% of the
# wall clock on TLS handshakes against api.themoviedb.org.
_conn_local = threading.local()


def _get_conn(dsn: str):
    conn = getattr(_conn_local, "conn", None)
    if conn is None or conn.closed:
        conn = psycopg2.connect(dsn)
        _conn_local.conn = conn
    return conn


def _get_session() -> requests.Session:
    session = getattr(_conn_local, "session", None)
    if session is None:
        session = requests.Session()
        _conn_local.session = session
    return session


def _upsert_person(cur, row: CreditRow) -> int:
    """UPSERT into people by tmdb_id. Returns people.id."""
    filename = f"p{row.tmdb_id}.webp" if row.profile_path else None
    name = _truncate_person_name(row.name)
    cur.execute(
        "INSERT INTO people (tmdb_id, name, profile_filename) "
        "VALUES (%s, %s, %s) "
        "ON CONFLICT (tmdb_id) DO UPDATE SET "
        "    name = EXCLUDED.name, "
        # COALESCE keeps a previously-set filename if TMDB has since
        # dropped the photo. The photo backfill is what NULLs rows
        # after confirming TMDB no longer has them.
        "    profile_filename = COALESCE(EXCLUDED.profile_filename, people.profile_filename) "
        "RETURNING id",
        (row.tmdb_id, name, filename),
    )
    return cur.fetchone()[0]


def _truncate_char_name(name: str | None) -> str | None:
    if name is None:
        return None
    return name[:CHAR_NAME_MAX] if len(name) > CHAR_NAME_MAX else name


def _truncate_person_name(name: str) -> str:
    return name[:PERSON_NAME_MAX] if len(name) > PERSON_NAME_MAX else name


def _order_index(raw_order: int | None) -> int:
    """Clamp TMDB `order` to the SMALLINT column. None → "sort last"."""
    if raw_order is None:
        return _UNORDERED_RANK
    if raw_order < 0:
        return 0
    if raw_order > _SMALLINT_MAX:
        return _SMALLINT_MAX
    return raw_order


def _process_film(
    film_id: int,
    tmdb_id: int,
    dsn: str,
    api_key: str,
    cast_limit: int,
    dry_run: bool,
) -> tuple[int, str, int, int]:
    session = _get_session()
    result = _fetch_credits(session, tmdb_id, api_key)
    if result is None:
        return film_id, "tmdb_error", 0, 0
    cast, directors = result
    if not cast and not directors:
        return film_id, "empty_credits", 0, 0

    if dry_run:
        return film_id, "ok_dry", min(len(cast), cast_limit), len(directors)

    conn = _get_conn(dsn)
    actors_inserted = 0
    directors_inserted = 0
    try:
        cur = conn.cursor()
        cast_sorted = sorted(cast, key=lambda r: _order_index(r.order))
        for c in cast_sorted[:cast_limit]:
            pid = _upsert_person(cur, c)
            # RETURNING + fetchone() is more reliable than rowcount for
            # `ON CONFLICT DO NOTHING` (psycopg2 reports rowcount=0 even
            # when a fresh row landed — same workaround as the series
            # backfill).
            cur.execute(
                "INSERT INTO film_actors "
                "(film_id, person_id, character_name, order_index) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (film_id, person_id) DO NOTHING "
                "RETURNING person_id",
                (film_id, pid, _truncate_char_name(c.character),
                 _order_index(c.order)),
            )
            if cur.fetchone() is not None:
                actors_inserted += 1

        for d in directors:
            pid = _upsert_person(cur, d)
            cur.execute(
                "INSERT INTO film_directors (film_id, person_id) "
                "VALUES (%s, %s) ON CONFLICT (film_id, person_id) DO NOTHING "
                "RETURNING person_id",
                (film_id, pid),
            )
            if cur.fetchone() is not None:
                directors_inserted += 1

        conn.commit()
        return film_id, "ok", actors_inserted, directors_inserted
    except psycopg2.Error as e:
        conn.rollback()
        log.warning("db error for film_id=%d tmdb=%d: %s", film_id, tmdb_id, e)
        return film_id, "db_error", 0, 0


def _fetch_candidates(
    dsn: str, limit: int | None
) -> list[tuple[int, int]]:
    """[(film_id, tmdb_id), ...] for films with tmdb_id and no credits yet.

    A film is "done" once it has at least one row in EITHER
    `film_actors` or `film_directors`. Checking only film_actors would
    leave director-only films (rare — typically silent shorts or
    archive footage with credited director but no listed cast)
    perpetually re-eligible: each re-run would re-fetch TMDB and only
    hit ON CONFLICT.
    """
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        sql = (
            "SELECT f.id, f.tmdb_id FROM films f "
            "WHERE f.tmdb_id IS NOT NULL "
            "  AND NOT EXISTS ("
            "    SELECT 1 FROM film_actors fa WHERE fa.film_id = f.id"
            "  ) "
            "  AND NOT EXISTS ("
            "    SELECT 1 FROM film_directors fd WHERE fd.film_id = f.id"
            "  ) "
            "ORDER BY f.added_at DESC NULLS LAST, f.id"
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
    ap.add_argument("--limit", type=int, help="Process only first N films")
    ap.add_argument("--cast-limit", type=int, default=DEFAULT_CAST_LIMIT)
    ap.add_argument("--dry-run", action="store_true")
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
    log.info("processing %d films with %d workers", len(rows), args.jobs)

    stats = {
        "ok": 0, "ok_dry": 0, "empty_credits": 0,
        "tmdb_error": 0, "db_error": 0, "worker_exception": 0,
    }
    actor_total = 0
    director_total = 0

    if args.dry_run:
        log.warning("--dry-run: no DB writes")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futures = [
            ex.submit(_process_film, fid, tmdb_id, dsn, api_key,
                      args.cast_limit, args.dry_run)
            for fid, tmdb_id in rows
        ]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            # A bare `fut.result()` would bubble unexpected worker
            # exceptions and abort the whole `as_completed` loop —
            # losing the summary tally for every still-pending future.
            # Catch broadly so one bad film can't kill the batch.
            try:
                _fid, status, a, d = fut.result()
            except Exception as e:  # noqa: BLE001
                stats["worker_exception"] += 1
                log.warning("worker raised: %s: %s",
                            type(e).__name__, e)
                continue
            stats[status] += 1
            actor_total += a
            director_total += d
            if i % 100 == 0 or i == len(rows):
                log.info(
                    "progress %d/%d ok=%d empty=%d tmdb_err=%d db_err=%d "
                    "wrk_exc=%d | actors=%d directors=%d",
                    i, len(rows), stats["ok"] + stats["ok_dry"],
                    stats["empty_credits"], stats["tmdb_error"],
                    stats["db_error"], stats["worker_exception"],
                    actor_total, director_total,
                )

    log.info("DONE: %s | actors=%d directors=%d",
             stats, actor_total, director_total)
    return 0 if (stats["tmdb_error"] == 0
                 and stats["db_error"] == 0
                 and stats["worker_exception"] == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
