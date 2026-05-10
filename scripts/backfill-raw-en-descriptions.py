#!/usr/bin/env python3
"""Re-generate Czech descriptions for films that ended up with raw English.

The auto-import pipeline silently fell back to TMDB's English overview when
Gemma 4 returned a 5xx (the `gemma-4-31b-it` model started erroring around
2026-05-10). The fix in `gemma_writer.py` adds a model fallback chain so
new imports don't regress, but ~58 historical films are still stuck with
short English descriptions on a Czech-language site. This one-shot script
re-runs Gemma for each of them.

Filter: `description IS NOT NULL AND LENGTH(description) < 200 AND
description !~ '[ěščřžýáíéůúóďťň]'` — short text without any Czech
diacritic. The combination is a strong signal of "this is raw TMDB EN".

Usage:
  DATABASE_URL=...  TMDB_API_KEY=...  GEMINI_API_KEY=... \
      python3 scripts/backfill-raw-en-descriptions.py [--dry-run] [--limit N]
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

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.auto_import.gemma_writer import generate_unique_cs  # noqa: E402

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_SLEEP = 0.25
GEMMA_SLEEP = 0.5

log = logging.getLogger("backfill-raw-en")

# Same heuristic the user-facing audit query uses to count "suspect raw EN":
# short description + no Czech diacritics + has a tmdb_id we can re-query.
SUSPECT_SQL = """
    SELECT id, title, year, tmdb_id
      FROM films
     WHERE description IS NOT NULL
       AND LENGTH(description) < 200
       AND description !~ '[ěščřžýáíéůúóďťň]'
       AND tmdb_id IS NOT NULL
"""


def fetch_overview(http: requests.Session, key: str, tmdb_id: int,
                   lang: str) -> str | None:
    r = http.get(
        f"{TMDB_BASE}/movie/{tmdb_id}",
        params={"api_key": key, "language": lang}, timeout=30,
    )
    if r.status_code != 200:
        log.warning("TMDB %d for tmdb=%d lang=%s", r.status_code, tmdb_id, lang)
        return None
    return (r.json().get("overview") or "").strip() or None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    dsn = os.environ.get("DATABASE_URL", "").strip()
    tmdb_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not (dsn and tmdb_key):
        log.error("DATABASE_URL and TMDB_API_KEY required")
        return 2

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    sql = SUSPECT_SQL + " ORDER BY id"
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    cur.execute(sql)
    films = cur.fetchall()
    log.info("processing %d films", len(films))

    http = requests.Session()
    http.headers.update({"User-Agent": "ceskarepublika.wiki desc-backfill-raw-en"})

    stats = {"updated_gemma": 0, "no_change": 0,
             "skipped_no_overview": 0, "failed": 0}

    for i, (film_id, title, year, tmdb_id) in enumerate(films, 1):
        cs = fetch_overview(http, tmdb_key, tmdb_id, "cs-CZ")
        time.sleep(TMDB_SLEEP)
        en = fetch_overview(http, tmdb_key, tmdb_id, "en-US")
        time.sleep(TMDB_SLEEP)

        if not cs and not en:
            stats["skipped_no_overview"] += 1
            log.info("[%d/%d] film_id=%d %r — TMDB has no overview, skipping",
                     i, len(films), film_id, title)
            continue

        sources = []
        if cs: sources.append(("TMDB CS", cs))
        if en: sources.append(("TMDB EN", en))

        try:
            generated = generate_unique_cs(title, year, sources, is_series=False)
        except Exception as e:
            log.warning("Gemma error film_id=%d: %s", film_id, e)
            generated = None
        time.sleep(GEMMA_SLEEP)

        if not generated:
            stats["failed"] += 1
            log.info("[%d/%d] film_id=%d %r — Gemma returned None, leaving as-is",
                     i, len(films), film_id, title)
            continue

        if args.dry_run:
            log.info("[%d/%d] DRY film_id=%d %r → %s",
                     i, len(films), film_id, title, generated[:120])
            continue

        # Only overwrite if the row STILL matches the suspect pattern — guards
        # against an admin who edited the description manually between SELECT
        # and UPDATE.
        try:
            cur.execute(
                """UPDATE films
                      SET description = %s
                    WHERE id = %s
                      AND description IS NOT NULL
                      AND LENGTH(description) < 200
                      AND description !~ '[ěščřžýáíéůúóďťň]'""",
                (generated, film_id),
            )
            if cur.rowcount == 0:
                stats["no_change"] += 1
                log.info("[%d/%d] film_id=%d — no longer matches filter, skipping",
                         i, len(films), film_id)
            else:
                stats["updated_gemma"] += 1
                log.info("[%d/%d] film_id=%d %r ← gemma (%d chars)",
                         i, len(films), film_id, title, len(generated))
            conn.commit()
        except Exception as e:
            log.error("DB error film_id=%d: %s", film_id, e)
            conn.rollback()
            stats["failed"] += 1

    log.info("done: %s", stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
