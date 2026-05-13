#!/usr/bin/env python3
"""Re-classify film_prehrajto_uploads.lang_class with current detect_lang.

Issue #537 — after the hyphen-tolerant regex fix landed in
import-prehrajto-uploads.py / import-prehrajto-new-films.py, rows
already in `film_prehrajto_uploads` still carry the pre-fix
classifications. This script idempotently re-runs detect_lang against
every row and UPDATEs lang_class where the new result differs.

Behaviour:
  - Default is dry-run; pass --apply to actually UPDATE.
  - The two importers carry the same detect_lang code 1:1; we load it
    from import-prehrajto-uploads.py.
  - We never downgrade a named class (CZ_DUB / CZ_SUB / SK_DUB /
    SK_SUB / CZ_NATIVE) to UNKNOWN or EN. If detect_lang now returns
    something less specific than what's stored, we leave the row
    alone — this preserves work from a future smarter detect_lang
    that we don't want to silently undo.
  - Cross-class flips (CZ_NATIVE -> CZ_DUB, CZ_NATIVE -> SK_DUB,
    UNKNOWN -> *) are applied: those are the targeted precision
    wins of #537 (a title with an explicit `cz-dabing` marker should
    be CZ_DUB, not CZ_NATIVE).

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
_NAMED = {"CZ_DUB", "CZ_SUB", "SK_DUB", "SK_SUB", "CZ_NATIVE"}


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
    updated = 0
    skipped_downgrade = 0

    upd = conn.cursor()
    for film_id, upload_id, title, old in rows:
        new = detect_lang(title or "")
        if new == old:
            continue

        # Refuse to downgrade. detect_lang may legitimately weaken
        # in future versions (e.g. if a regex is tightened to drop a
        # false-positive) — that should be a deliberate migration, not
        # a side effect of this one-shot backfill.
        if old in _NAMED and new not in _NAMED:
            skipped_downgrade += 1
            continue

        transitions[(old, new)] += 1
        if args.apply:
            upd.execute(
                "UPDATE film_prehrajto_uploads SET lang_class = %s "
                "WHERE film_id = %s AND upload_id = %s AND lang_class = %s",
                (new, film_id, upload_id, old),
            )
            updated += upd.rowcount

    if args.apply:
        conn.commit()

    print(f"\nTransitions ({sum(transitions.values()):,} rows):")
    for (o, n), count in transitions.most_common():
        print(f"  {o:14s} -> {n:14s} {count:>6,}")
    if skipped_downgrade:
        print(f"\nSkipped downgrades (named -> UNKNOWN/EN): {skipped_downgrade}")
    if args.apply:
        print(f"\nUPDATEs committed: {updated:,}")
    else:
        print(f"\n(dry-run — pass --apply to commit)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
