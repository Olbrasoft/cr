#!/usr/bin/env python3
"""Backfill prehraj.to sources for films that already have a sktorrent source
   but no prehrajto source.

The sktorrent backfill more than doubled `films` last week. Many of those
also exist on prehraj.to, but our `import-prehrajto-uploads.py` cron runs
the OPPOSITE direction (sitemap → match TMDB) and does not retroactively
cover films we add from other providers.

This script flips the lookup: for each (film_id) with SKT but no PRH, we
search `prehraj.to/hledej/{title (year)}` via the CZ proxy, classify hits
and write accepted ones via the dual-write helper.

Idempotent: re-runs are safe because upserts key on `upload_id`. The
daily cron mode (--daily) skips films that got a PRH row in the past 7 days.

Watchdog: any HTTP non-200 or suspiciously short body raises BlockedError
and aborts the run (we share the CZ proxy with the sktorrent scanner —
losing it kills two pipelines).

The actual search/classify/write logic lives in
`scripts/auto_import/prehrajto_search.py` so the daily auto-import can
reuse it directly.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

from scripts.auto_import.prehrajto_search import (  # noqa: E402
    BlockedError, SEARCH_SLEEP_S, try_prehrajto_match,
)
from video_sources_helper import get_provider_ids  # noqa: E402

log = logging.getLogger("backfill_prh")


def _select_target_films(
    cur, providers: dict, limit: int | None, daily: bool,
    only_film_id: int | None,
) -> list[tuple[int, str, str | None, int | None, int | None]]:
    if only_film_id:
        cur.execute(
            "SELECT id, title, original_title, year, runtime_min "
            "FROM films WHERE id = %s",
            (only_film_id,),
        )
        return list(cur.fetchall())
    where_extra = ""
    if daily:
        where_extra = """
          AND NOT EXISTS (
            SELECT 1 FROM film_prehrajto_uploads fpu
             WHERE fpu.film_id = f.id
               AND fpu.last_seen_at > NOW() - INTERVAL '7 days'
          )
        """
    # Provider IDs are seeded per-DB and not guaranteed stable across
    # environments, so resolve them by slug rather than hard-coding 1/2.
    sql = f"""
        SELECT f.id, f.title, f.original_title, f.year, f.runtime_min
          FROM films f
          JOIN video_sources vs1
            ON vs1.film_id = f.id AND vs1.provider_id = %(skt)s
         WHERE NOT EXISTS (
              SELECT 1 FROM video_sources vs2
               WHERE vs2.film_id = f.id AND vs2.provider_id = %(prh)s
         )
         {where_extra}
         ORDER BY f.id DESC
    """
    params = {"skt": providers["sktorrent"], "prh": providers["prehrajto"]}
    if limit:
        sql += " LIMIT %(limit)s"
        params["limit"] = limit
    cur.execute(sql, params)
    return list(cur.fetchall())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="max films to process")
    ap.add_argument("--film-id", type=int, help="single film_id (debug)")
    ap.add_argument("--daily", action="store_true",
                    help="skip films that got a PRH hit in the last 7 days")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse + score, but do not write to DB")
    ap.add_argument("--commit-every", type=int, default=50)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL not set")
        return 2
    db_url = db_url.replace("@db:", "@127.0.0.1:")  # prod→local SSH-tunnel rewrite

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    providers = get_provider_ids(cur)
    films = _select_target_films(cur, providers, args.limit, args.daily, args.film_id)
    log.info("selected %d films (limit=%s daily=%s film_id=%s)",
             len(films), args.limit, args.daily, args.film_id)
    if not films:
        return 0

    sess = requests.Session()

    counters = {
        "films_total": len(films), "films_processed": 0,
        "films_with_hits": 0, "films_no_hits": 0, "films_no_results": 0,
        "rows_written": 0, "rows_repointed": 0,
        "blocked": 0, "errors": 0,
    }

    t0 = time.time()
    try:
        for i, (film_id, title, orig, year, runtime_min) in enumerate(films, 1):
            counters["films_processed"] += 1
            try:
                if args.dry_run:
                    # In dry-run we still call try_prehrajto_match but
                    # rollback any DB changes immediately afterwards. Cheap.
                    cur.execute("SAVEPOINT dry")
                result = try_prehrajto_match(
                    cur, providers, film_id,
                    title=title, original_title=orig, year=year,
                    runtime_min=runtime_min, sess=sess,
                )
                if args.dry_run:
                    cur.execute("ROLLBACK TO SAVEPOINT dry")
                    cur.execute("RELEASE SAVEPOINT dry")
            except BlockedError as e:
                log.error("ABORT: blocked by prehraj.to — %s", e)
                counters["blocked"] += 1
                conn.rollback()
                return 3
            except Exception as e:
                log.error("error film_id=%d: %s", film_id, e)
                counters["errors"] += 1
                conn.rollback()
                time.sleep(SEARCH_SLEEP_S)
                continue

            tier_summary = {k: v for k, v in result["tier_counts"].items() if v}
            if result["hits"] == 0:
                counters["films_no_results"] += 1
                log.info("[%d/%d] film_id=%d %r — 0 hits",
                         i, len(films), film_id, result["query"])
            elif result["accepted"] == 0:
                counters["films_no_hits"] += 1
                log.info("[%d/%d] film_id=%d %r — %d hits, no acceptable tier (%s)",
                         i, len(films), film_id, result["query"],
                         result["hits"], tier_summary)
            else:
                counters["films_with_hits"] += 1
                counters["rows_written"] += result["written"]
                counters["rows_repointed"] += result["repointed"]
                log.info("[%d/%d] film_id=%d %r — %d hits → %s "
                         "(written=%d repointed=%d refreshed=%d collisions=%d)",
                         i, len(films), film_id, result["query"],
                         result["hits"], tier_summary,
                         result["written"], result["repointed"],
                         result.get("refreshed", 0), result["collisions"])

            if not args.dry_run and i % args.commit_every == 0:
                conn.commit()
                log.info("COMMIT @ %d (rows_written=%d)",
                         i, counters["rows_written"])
            time.sleep(SEARCH_SLEEP_S)

        if not args.dry_run:
            conn.commit()
    finally:
        cur.close()
        conn.close()

    dur = time.time() - t0
    log.info("finished: %s in %.0fs (%.2f films/s)",
             counters, dur, counters["films_processed"] / max(dur, 1e-3))
    return 0


if __name__ == "__main__":
    sys.exit(main())
