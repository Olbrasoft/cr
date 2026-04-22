#!/usr/bin/env python3
"""Backfill `films.runtime_min` from sledujteto primary upload duration.

The bulk-import (`scripts/import-sledujteto-films.py`) copies
`films.runtime_min` from TMDB's `runtime` field. For some films TMDB
either returns 0/NULL (we then rendered "0 min" on the listing) or
returns a value that disagrees grossly with the actual playback file —
for example `nightshift-2018` has `runtime_min = 13` while the primary
sledujteto upload is 4814 s (~80 min), which is what the user sees in
the <video> element.

Since the duration the user actually watches comes from the sledujteto
file we prefer that number when:

  - `runtime_min` is NULL or 0 (missing TMDB data), OR
  - the primary upload duration is at least 15 minutes *longer* than
    the TMDB runtime (TMDB clearly has wrong / short-form data).

We do NOT overwrite TMDB when the upload is *shorter* than TMDB — that
case usually means the upload is a trailer / teaser / single episode
and TMDB's feature runtime is the right label.

Usage:
  DATABASE_URL=postgres://cr:...@localhost:5433/cr \\
      python3 scripts/backfill-sledujteto-runtime.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. pip install psycopg2-binary",
          file=sys.stderr)
    sys.exit(2)

log = logging.getLogger("backfill-sledujteto-runtime")


SELECT_CANDIDATES_SQL = """
SELECT f.id, f.slug, f.runtime_min, u.duration_sec
FROM films f
JOIN film_sledujteto_uploads u
  ON u.film_id = f.id AND u.file_id = f.sledujteto_primary_file_id
WHERE f.sledujteto_primary_file_id IS NOT NULL
  AND u.duration_sec IS NOT NULL
  AND u.duration_sec > 0
  AND (
       f.runtime_min IS NULL
    OR f.runtime_min = 0
    OR u.duration_sec > (f.runtime_min * 60 + 900)
  )
ORDER BY f.id
"""

UPDATE_RUNTIME_SQL = "UPDATE films SET runtime_min = %s WHERE id = %s"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="ROLLBACK at end")
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

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute(SELECT_CANDIDATES_SQL)
        rows = cur.fetchall()
        log.info("found %d films to update", len(rows))

        updated = 0
        for film_id, slug, old_min, duration_sec in rows:
            new_min = max(1, round(duration_sec / 60.0))
            log.debug(
                "film_id=%d slug=%s old=%r new=%d (duration_sec=%d)",
                film_id, slug, old_min, new_min, duration_sec,
            )
            cur.execute(UPDATE_RUNTIME_SQL, (new_min, film_id))
            updated += 1

        log.info("updated %d rows", updated)

        if args.dry_run:
            log.info("--dry-run: ROLLBACK")
            conn.rollback()
        else:
            conn.commit()
            log.info("committed")

        return 0
    except Exception:
        conn.rollback()
        log.exception("backfill failed — rolled back")
        return 1
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
