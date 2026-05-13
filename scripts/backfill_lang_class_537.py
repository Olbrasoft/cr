#!/usr/bin/env python3
"""Re-classify film_prehrajto_uploads.lang_class with current detect_lang.

Issue #537 — after the hyphen-tolerant regex fix landed in
import-prehrajto-uploads.py / import-prehrajto-new-films.py, rows
already in `film_prehrajto_uploads` still carry the pre-fix
classifications. This script idempotently re-runs detect_lang against
every row and UPDATEs lang_class where the new result differs.

Behaviour (strict #537 scope):
  - Default is dry-run; pass --apply to actually UPDATE.
  - The two importers carry the same detect_lang code 1:1; we load it
    from import-prehrajto-uploads.py.
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

Usage:
  DATABASE_URL=postgres://... python3 scripts/backfill_lang_class_537.py
  DATABASE_URL=postgres://... python3 scripts/backfill_lang_class_537.py --apply
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from collections import Counter
from pathlib import Path

import psycopg2

_HERE = Path(__file__).resolve().parent
_DUB_SUB = {"CZ_DUB", "CZ_SUB", "SK_DUB", "SK_SUB"}


def _load_detect_lang():
    spec = importlib.util.spec_from_file_location(
        "_uploads_for_backfill", _HERE / "import-prehrajto-uploads.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.detect_lang


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

    detect_lang = _load_detect_lang()
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    # film_prehrajto_uploads PK is composite (film_id, upload_id) —
    # no synthetic `id`. We need both for the WHERE clause on UPDATE.
    sql = ("SELECT film_id, upload_id, title, lang_class "
           "FROM film_prehrajto_uploads ORDER BY film_id, upload_id")
    params: tuple = ()
    if args.limit:
        sql += " LIMIT %s"
        params = (int(args.limit),)
    cur.execute(sql, params)
    rows = cur.fetchall()
    print(f"scanning {len(rows):,} rows ({'apply' if args.apply else 'dry-run'})")

    transitions: Counter[tuple[str, str]] = Counter()
    out_of_scope: Counter[tuple[str, str]] = Counter()
    updated = 0

    upd = conn.cursor()
    for film_id, upload_id, title, old in rows:
        new = detect_lang(title or "")
        if new == old:
            continue

        # Strict #537 scope: only promote UNKNOWN -> DUB/SUB. Anything
        # else (UNKNOWN -> CZ_NATIVE, CZ_DUB -> CZ_NATIVE, EN -> *, etc.)
        # is tallied for visibility but skipped — see module docstring.
        if old == "UNKNOWN" and new in _DUB_SUB:
            transitions[(old, new)] += 1
            if args.apply:
                upd.execute(
                    "UPDATE film_prehrajto_uploads SET lang_class = %s "
                    "WHERE film_id = %s AND upload_id = %s AND lang_class = %s",
                    (new, film_id, upload_id, old),
                )
                updated += upd.rowcount
        else:
            out_of_scope[(old, new)] += 1

    if args.apply:
        conn.commit()

    # Sync video_sources for the same rows. film_prehrajto_uploads is
    # the legacy table; the unified video_sources schema (#607/#610)
    # carries its own lang_class + audio_lang per source, and the
    # rollup trigger on video_sources is what populates
    # films.audio_langs / subtitle_langs. Without this step the regex
    # fix only updates the legacy column and the user-visible filter
    # on /filmy-online/ doesn't move.
    #
    # Scope mirrors the lang_class backfill above: only sync where
    # legacy was UNKNOWN and is now a DUB/SUB class, AND video_sources
    # still shows UNKNOWN. Pre-existing drift in other rows is out of
    # scope for #537.
    if args.apply:
        sync_cur = conn.cursor()
        sync_cur.execute("""
            UPDATE video_sources vs SET
                lang_class = u.lang_class,
                audio_lang = CASE u.lang_class
                    WHEN 'CZ_DUB' THEN 'cs'
                    WHEN 'SK_DUB' THEN 'sk'
                    ELSE NULL
                END,
                audio_detected_by = 'title_regex',
                updated_at = now()
            FROM film_prehrajto_uploads u
            WHERE vs.provider_id = (SELECT id FROM video_providers WHERE slug = 'prehrajto')
              AND vs.external_id = u.upload_id
              AND vs.film_id = u.film_id
              AND vs.lang_class = 'UNKNOWN'
              AND u.lang_class IN ('CZ_DUB', 'CZ_SUB', 'SK_DUB', 'SK_SUB')
            RETURNING vs.id, u.lang_class
        """)
        synced_rows = sync_cur.fetchall()
        synced_count = len(synced_rows)

        # For CZ_SUB / SK_SUB rows, the subtitle track lives in
        # video_source_subtitles, not in any column on video_sources.
        # Insert one row per (video_source, subtitle_lang); the
        # ON CONFLICT clause keeps re-runs no-op.
        sub_inserts = 0
        for vs_id, lc in synced_rows:
            sub_lang = {"CZ_SUB": "cs", "SK_SUB": "sk"}.get(lc)
            if sub_lang is None:
                continue
            sync_cur.execute("""
                INSERT INTO video_source_subtitles (source_id, lang)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (vs_id, sub_lang))
            sub_inserts += sync_cur.rowcount
        conn.commit()
        print(f"\nSynced video_sources rows:        {synced_count:,}")
        print(f"Inserted video_source_subtitles:  {sub_inserts:,}")

    print(f"\nApplied transitions ({sum(transitions.values()):,} rows):")
    for (o, n), count in transitions.most_common():
        print(f"  {o:14s} -> {n:14s} {count:>6,}")
    if out_of_scope:
        print(f"\nSkipped — out of #537 scope "
              f"({sum(out_of_scope.values()):,} rows):")
        for (o, n), count in out_of_scope.most_common():
            print(f"  {o:14s} -> {n:14s} {count:>6,}")
    if args.apply:
        print(f"\nUPDATEs committed: {updated:,}")
    else:
        print(f"\n(dry-run — pass --apply to commit)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
