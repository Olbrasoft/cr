#!/usr/bin/env python3
"""Re-generate Gemma descriptions for tv_shows that still hold raw TMDB text.

Issue #565: until today's fix, `tv_show_enricher.process_tv_show_episode`
wrote TMDB's cs-CZ / en-US overview directly into `tv_shows.description`,
which Google flags as duplicate content vs every other Czech site fed
from the same TMDB row. This one-shot script re-runs Gemma for each
existing TV show so the historical 50 rows on prod don't keep poisoning
SEO.

Strategy:
- Pull cs-CZ + en-US overviews from TMDB for every tv_show with tmdb_id.
- Skip rows whose current description is NOT byte-identical to either
  TMDB overview — that means an admin already curated it, or some prior
  run already wrote Gemma text. The acceptance criterion in #565 is
  exactly this: after the backfill no row equals TMDB overview verbatim.
- For the rest, call `generate_unique_cs(title, first_air_year, sources,
  is_series=True)` (matching the in-flight enricher path) and overwrite.

Usage:
  DATABASE_URL=...  TMDB_API_KEY=...  GEMINI_API_KEY=... \\
      python3 scripts/generate-tv-show-descriptions.py [--dry-run] [--limit N]
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

log = logging.getLogger("gen-tv-desc")


def fetch_tv_overview(http: requests.Session, key: str, tmdb_id: int,
                      lang: str) -> str | None:
    """Fetch TMDB TV overview. Returns None on any transient failure."""
    try:
        r = http.get(
            f"{TMDB_BASE}/tv/{tmdb_id}",
            params={"api_key": key, "language": lang}, timeout=30,
        )
    except requests.RequestException as e:
        log.warning("TMDB request failed tmdb=%d lang=%s: %s", tmdb_id, lang, e)
        return None
    if r.status_code != 200:
        log.warning("TMDB %d for tmdb=%d lang=%s", r.status_code, tmdb_id, lang)
        return None
    try:
        body = r.json()
    except ValueError as e:
        log.warning("TMDB JSON decode failed tmdb=%d lang=%s: %s", tmdb_id, lang, e)
        return None
    return (body.get("overview") or "").strip() or None


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

    has_gemma_key = (os.environ.get("GEMINI_API_KEY", "").strip() or
                     any(os.environ.get(f"GEMINI_API_KEY_{i}", "").strip()
                         for i in range(1, 5)))
    if not has_gemma_key:
        log.error("GEMINI_API_KEY (or GEMINI_API_KEY_1..4) required — "
                  "without it Gemma returns None for every row and the run "
                  "would issue many TMDB requests with nothing to update")
        return 2

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    sql = (
        "SELECT id, title, first_air_year, tmdb_id, description "
        "FROM tv_shows "
        "WHERE tmdb_id IS NOT NULL "
        "ORDER BY id"
    )
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    cur.execute(sql)
    shows = cur.fetchall()
    log.info("processing %d tv_shows", len(shows))

    http = requests.Session()
    http.headers.update({"User-Agent": "ceskarepublika.wiki tv-desc-backfill"})

    stats = {"updated_gemma": 0, "already_unique": 0,
             "skipped_no_overview": 0, "failed": 0}

    for i, (tv_show_id, title, year, tmdb_id, current_desc) in enumerate(shows, 1):
        cs = fetch_tv_overview(http, tmdb_key, tmdb_id, "cs-CZ")
        time.sleep(TMDB_SLEEP)
        en = fetch_tv_overview(http, tmdb_key, tmdb_id, "en-US")
        time.sleep(TMDB_SLEEP)

        if not cs and not en:
            stats["skipped_no_overview"] += 1
            log.info("[%d/%d] id=%d %r — TMDB has no overview, skipping",
                     i, len(shows), tv_show_id, title)
            continue

        # If the current description isn't byte-identical to either TMDB
        # overview, an admin already curated it (or a previous Gemma run
        # touched it). Leave it alone — the #565 acceptance criterion is
        # exactly "no row equals TMDB overview verbatim".
        if current_desc and current_desc != cs and current_desc != en:
            stats["already_unique"] += 1
            log.info("[%d/%d] id=%d %r — description already unique, skipping",
                     i, len(shows), tv_show_id, title)
            continue

        sources: list[tuple[str, str]] = []
        if cs:
            sources.append(("TMDB CS", cs))
        if en:
            sources.append(("TMDB EN", en))

        try:
            generated = generate_unique_cs(title, year, sources, is_series=True)
        except Exception as e:
            log.warning("Gemma error id=%d: %s", tv_show_id, e)
            generated = None
        time.sleep(GEMMA_SLEEP)

        if not generated:
            stats["failed"] += 1
            log.info("[%d/%d] id=%d %r — Gemma returned None, leaving as-is",
                     i, len(shows), tv_show_id, title)
            continue

        if args.dry_run:
            log.info("[%d/%d] DRY id=%d %r → %s",
                     i, len(shows), tv_show_id, title, generated[:120])
            continue

        # Guard against concurrent edits the same way the films backfill
        # does: only update if the row is STILL in the suspect state
        # (description still equals one of the TMDB overviews).
        try:
            cur.execute(
                "UPDATE tv_shows SET description = %s "
                "WHERE id = %s "
                "  AND (description = %s OR description = %s OR description IS NULL)",
                (generated, tv_show_id, cs, en),
            )
            if cur.rowcount == 0:
                stats["already_unique"] += 1
                log.info("[%d/%d] id=%d — no longer matches filter, skipping",
                         i, len(shows), tv_show_id)
            else:
                stats["updated_gemma"] += 1
                log.info("[%d/%d] id=%d %r ← gemma (%d chars)",
                         i, len(shows), tv_show_id, title, len(generated))
            conn.commit()
        except Exception as e:
            log.error("DB error id=%d: %s", tv_show_id, e)
            conn.rollback()
            stats["failed"] += 1

    log.info("done: %s", stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
