#!/usr/bin/env python3
"""Backfill `films.runtime_min` from TMDB for rows where it is NULL (#661).

Why: `import-prehrajto-uploads.py::load_matches_from_films` skips every film
without `runtime_min` ("would match too widely") because the cluster key
needs a duration anchor to avoid cross-film false positives. ~6 800 films
on prod are silently excluded from sitemap matching purely because their
runtime field never got populated.

Surfaced by /admin/prehrajto/unmatched (#657 dashboard) — concrete cases
include "Čistá duše" (films.id=179, tmdb=453), "Až tak moc tě nežere"
(4665, 10184), and "Smrt ve tmě" (2045, 300669).

Behavior:
  - SELECT id, tmdb_id FROM films WHERE runtime_min IS NULL AND tmdb_id IS NOT NULL
  - For each: GET /movie/{tmdb_id} → read `runtime` (int minutes)
  - UPDATE films SET runtime_min = $1 WHERE id = $2 AND runtime_min IS NULL
  - Idempotent: re-runs are safe (the WHERE clause guards against
    overwriting values added between runs).
  - Rate-limited: 0.3 s between requests fits comfortably under TMDB's
    free-tier 40-req/10-s ceiling, with headroom for retries.

Required env: DATABASE_URL, TMDB_API_KEY.

Usage:
  python3 scripts/backfill_films_runtime.py [--dry-run] [--limit N] [--sleep 0.3]
"""

from __future__ import annotations

import argparse
import os
import sys
import time

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not installed. apt install python3-psycopg2", file=sys.stderr)
    sys.exit(2)

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. apt install python3-requests", file=sys.stderr)
    sys.exit(2)


TMDB_API_BASE = "https://api.themoviedb.org/3"
USER_AGENT = "ceskarepublika.wiki backfill (https://github.com/Olbrasoft/cr/issues/661)"


def fetch_runtime(session: requests.Session, tmdb_id: int, api_key: str) -> tuple[int | None, str | None]:
    """Return (runtime_min_or_None, error_or_None).

    `runtime` is `None` when TMDB has no value for the field; we treat
    `0` the same way (TMDB sometimes stores it as 0 instead of null for
    incomplete entries — neither is useful as a matching anchor).
    """
    try:
        r = session.get(
            f"{TMDB_API_BASE}/movie/{tmdb_id}",
            params={"api_key": api_key, "language": "en-US"},
            timeout=10,
        )
    except requests.RequestException as e:
        return None, f"http error: {e}"
    if r.status_code == 404:
        return None, "tmdb 404"
    if r.status_code != 200:
        return None, f"tmdb status {r.status_code}"
    try:
        data = r.json()
    except ValueError as e:
        return None, f"json parse: {e}"
    runtime = data.get("runtime")
    if not runtime:  # None or 0 — same outcome (no usable value)
        return None, None
    return int(runtime), None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would be updated; ROLLBACK at the end.")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N films (0 = all). Useful for smoke tests.")
    ap.add_argument("--sleep", type=float, default=0.3,
                    help="Seconds between TMDB requests (default 0.3 → ~33/s, well under "
                         "TMDB's 40-req/10s limit).")
    ap.add_argument("--commit-every", type=int, default=200,
                    help="Commit after every N updates (default 200). Set 0 for one big "
                         "transaction (with --dry-run this is the always rollback case).")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print("ERROR: DATABASE_URL env var required", file=sys.stderr)
        return 2
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not api_key:
        print("ERROR: TMDB_API_KEY env var required", file=sys.stderr)
        return 2

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        cur = conn.cursor()
        # Count up front for nice progress output.
        cur.execute("""
            SELECT COUNT(*) FROM films
             WHERE runtime_min IS NULL AND tmdb_id IS NOT NULL
        """)
        total = cur.fetchone()[0]
        print(f"films with NULL runtime_min and non-NULL tmdb_id: {total:,}")
        if args.limit and args.limit < total:
            print(f"  (--limit {args.limit} → processing first {args.limit})")
            total = args.limit
        if total == 0:
            print("Nothing to do.")
            return 0

        # Fetch the work list. Stable ordering by id keeps the importer's
        # collision-resolution behaviour from #654 (kept lowest film_id) consistent
        # if anyone correlates this run with sitemap matching outputs.
        sql = """
            SELECT id, tmdb_id FROM films
             WHERE runtime_min IS NULL AND tmdb_id IS NOT NULL
             ORDER BY id
        """
        if args.limit:
            sql += f" LIMIT {int(args.limit)}"
        cur.execute(sql)
        work = cur.fetchall()

        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        update_sql = """
            UPDATE films SET runtime_min = %s WHERE id = %s AND runtime_min IS NULL
        """

        filled = 0
        tmdb_no_value = 0
        api_errors = 0
        t0 = time.time()
        last_print = t0
        for i, (film_id, tmdb_id) in enumerate(work, 1):
            runtime, err = fetch_runtime(session, tmdb_id, api_key)
            if err is not None:
                api_errors += 1
                if api_errors <= 10 or api_errors % 50 == 0:
                    print(f"  ERR film_id={film_id} tmdb={tmdb_id}: {err}", flush=True)
            elif runtime is None:
                tmdb_no_value += 1
            else:
                if not args.dry_run:
                    cur.execute(update_sql, (runtime, film_id))
                filled += 1
                if args.commit_every and not args.dry_run and filled % args.commit_every == 0:
                    conn.commit()

            time.sleep(args.sleep)

            now = time.time()
            if now - last_print > 5 or i == total:
                rate = i / (now - t0) if now > t0 else 0.0
                eta = (total - i) / rate if rate > 0 else 0
                print(
                    f"  [{i:>5}/{total}]  filled={filled}  tmdb_null={tmdb_no_value}  "
                    f"errors={api_errors}  rate={rate:.1f}/s  eta={int(eta)}s",
                    flush=True,
                )
                last_print = now

        if args.dry_run:
            conn.rollback()
            print("Dry-run: ROLLBACK")
        else:
            conn.commit()
            print("COMMIT")

        print(f"\nSummary:")
        print(f"  films processed:           {len(work):,}")
        print(f"  runtime_min updated:       {filled:,}")
        print(f"  TMDB has no runtime value: {tmdb_no_value:,}")
        print(f"  API errors:                {api_errors:,}")
        print(f"  total elapsed:             {time.time()-t0:.0f}s")

        return 0 if api_errors == 0 else 1
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
