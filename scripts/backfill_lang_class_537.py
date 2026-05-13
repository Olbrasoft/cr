#!/usr/bin/env python3
"""Re-classify film_prehrajto_uploads.lang_class with current detect_lang.

Issue #537 — after the hyphen-tolerant regex fix landed in
import-prehrajto-uploads.py / import-prehrajto-new-films.py, rows
already in `film_prehrajto_uploads` still carry the pre-fix
classifications. This script idempotently re-runs detect_lang against
every row and UPDATEs lang_class where the new result differs.

Behaviour (strict #537 scope):
  - Default is dry-run; pass --apply to actually UPDATE.
  - detect_lang is loaded from scripts/auto_import/lang_detect.py
    (dependency-free; both importer scripts re-export it).
  - ONLY rows currently classified as UNKNOWN and now resolving to a
    DUB/SUB class are touched. Everything else is left alone:
      * UNKNOWN -> CZ_NATIVE flips are large in prod (~35 k) and come
        from rows imported before the CZ_NATIVE diacritic heuristic
        existed, not from the #537 regex change. Mixing them in
        would conflate the PR — backfill them in their own ticket.
      * Existing named classes (CZ_DUB / CZ_SUB / SK_DUB / SK_SUB /
        CZ_NATIVE) are never overwritten. Even cross-class flips like
        CZ_DUB -> CZ_NATIVE that the new code would assert lose
        specific information (the original CZ_DUB row may have come
        from a human-verified import); don't silently downgrade.
      * EN rows are left alone — outside the #537 scope.

Memory + scope hardening (Copilot review on PR #715):
  - The candidate SELECT is filtered server-side to
    `lang_class = 'UNKNOWN'` and streamed via a named cursor +
    fetchmany(); we never materialize the full table.
  - The video_sources sync UPDATE is restricted to the exact
    (film_id, upload_id, lang_class) triples promoted in this run —
    NOT "every row where legacy is DUB/SUB and unified is UNKNOWN",
    which would also sweep up pre-existing drift outside #537's scope.
  - audio_lang / audio_detected_by are filled with COALESCE so any
    value populated by another pipeline survives. The title-regex
    detector is only credited for rows that previously had NULL.

Usage:
  DATABASE_URL=postgres://... python3 scripts/backfill_lang_class_537.py
  DATABASE_URL=postgres://... python3 scripts/backfill_lang_class_537.py --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from pathlib import Path

import psycopg2
import psycopg2.extras

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from auto_import.lang_detect import detect_lang  # noqa: E402

_DUB_SUB = {"CZ_DUB", "CZ_SUB", "SK_DUB", "SK_SUB"}
_FETCH_BATCH = 5000  # Streamed batch size for fetchmany().


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Commit UPDATEs (default is dry-run)")
    ap.add_argument("--limit", type=int, help="Cap processed rows")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        print("DATABASE_URL required", file=sys.stderr)
        return 2

    conn = psycopg2.connect(dsn)

    # Server-side cursor + lang_class filter so we never materialize the
    # whole `film_prehrajto_uploads` table. The script only promotes
    # UNKNOWN -> DUB/SUB; rows with any other current class are no-ops
    # by design and filtering them in SQL skips a full-table scan worth
    # of Python iteration.
    select_cur = conn.cursor(name="backfill_537_scan")
    sql = ("SELECT film_id, upload_id, title "
           "FROM film_prehrajto_uploads "
           "WHERE lang_class = 'UNKNOWN' "
           "ORDER BY film_id, upload_id")
    params: tuple = ()
    if args.limit:
        sql += " LIMIT %s"
        params = (int(args.limit),)
    select_cur.execute(sql, params)

    transitions: Counter[tuple[str, str]] = Counter()
    # Triples of (film_id, upload_id, new_class) that this run promoted.
    # Drives the video_sources sync below with strict scope.
    promoted: list[tuple[int, str, str]] = []
    updated = 0
    scanned = 0

    upd = conn.cursor()
    while True:
        batch = select_cur.fetchmany(_FETCH_BATCH)
        if not batch:
            break
        scanned += len(batch)
        for film_id, upload_id, title in batch:
            new = detect_lang(title or "")
            if new not in _DUB_SUB:
                continue
            transitions[("UNKNOWN", new)] += 1
            promoted.append((film_id, upload_id, new))
            if args.apply:
                upd.execute(
                    "UPDATE film_prehrajto_uploads SET lang_class = %s "
                    "WHERE film_id = %s AND upload_id = %s "
                    "  AND lang_class = 'UNKNOWN'",
                    (new, film_id, upload_id),
                )
                updated += upd.rowcount

    select_cur.close()

    print(f"scanned {scanned:,} UNKNOWN rows "
          f"({'apply' if args.apply else 'dry-run'})")

    if args.apply:
        conn.commit()

    # Sync video_sources for the same rows. film_prehrajto_uploads is
    # the legacy table; the unified video_sources schema (#607/#610)
    # carries its own lang_class + audio_lang per source, and the
    # rollup trigger on video_sources is what populates
    # films.audio_langs / subtitle_langs. Without this step the regex
    # fix only updates the legacy column and the user-visible filter
    # on /filmy-online/ doesn't move.
    synced_count = 0
    sub_inserts = 0
    if args.apply and promoted:
        sync_cur = conn.cursor()

        # Strict scope: only the triples we just promoted. Pre-existing
        # drift in other rows is out of #537's scope (Copilot review).
        # COALESCE preserves any audio_lang / audio_detected_by already
        # populated by another pipeline; the title-regex pass only
        # writes when the column is NULL.
        sync_rows = psycopg2.extras.execute_values(
            sync_cur,
            """
            UPDATE video_sources vs SET
                lang_class = u.lang_class,
                audio_lang = COALESCE(vs.audio_lang, CASE u.lang_class
                    WHEN 'CZ_DUB' THEN 'cs'
                    WHEN 'SK_DUB' THEN 'sk'
                    ELSE NULL
                END),
                audio_detected_by = COALESCE(vs.audio_detected_by, 'title_regex'),
                updated_at = now()
            FROM (VALUES %s) AS u(film_id, upload_id, lang_class)
            WHERE vs.provider_id = (SELECT id FROM video_providers WHERE slug = 'prehrajto')
              AND vs.external_id = u.upload_id
              AND vs.film_id = u.film_id
              AND vs.lang_class = 'UNKNOWN'
            RETURNING vs.id, vs.lang_class
            """,
            promoted,
            fetch=True,
        )
        synced_count = len(sync_rows)

        # For CZ_SUB / SK_SUB rows, the subtitle track lives in
        # video_source_subtitles, not in any column on video_sources.
        # Insert one row per (video_source, subtitle_lang); the
        # ON CONFLICT clause keeps re-runs no-op.
        for vs_id, lc in sync_rows:
            sub_lang = {"CZ_SUB": "cs", "SK_SUB": "sk"}.get(lc)
            if sub_lang is None:
                continue
            sync_cur.execute(
                "INSERT INTO video_source_subtitles (source_id, lang) "
                "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (vs_id, sub_lang),
            )
            sub_inserts += sync_cur.rowcount
        conn.commit()

    print(f"\nApplied transitions ({sum(transitions.values()):,} rows):")
    for (o, n), count in transitions.most_common():
        print(f"  {o:14s} -> {n:14s} {count:>6,}")
    if args.apply:
        print(f"\nUPDATEs committed:                {updated:,}")
        print(f"Synced video_sources rows:        {synced_count:,}")
        print(f"Inserted video_source_subtitles:  {sub_inserts:,}")
    else:
        print(f"\n(dry-run — pass --apply to commit)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
