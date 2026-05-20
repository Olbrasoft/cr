#!/usr/bin/env python3
"""One-shot migration #757: sledujteto video_sources.external_id from
URL slug → global numeric upload_id.

Background: sledujteto.cz REUSES URL slugs (e.g. `/file/49648/...`)
across unrelated uploads after the original file is deleted. The
series importer (`import-sledujteto-series.py`) was writing
`video_sources.external_id = slug`, so re-imports collide with rows
written from older scrapes and silently drop the new attach (92
conflicts observed in the 2026-05-20 prod run, see issue #757).

This migration:

  1. Loads the latest sledujteto raw scrape and builds a
     `slug → global numeric id` map.
  2. For every video_sources row with `provider=sledujteto` AND
     `episode_id IS NOT NULL` (films use upload_id already, no work),
     looks up the row's external_id in the map.
  3. If found → UPDATE external_id = global numeric id.
  4. If NOT found → the underlying upload has been deleted on
     sledujteto. Mark `is_alive=false` and leave external_id alone
     (anything we could put there is guesswork).

Two-pass strategy to avoid within-migration UNIQUE collisions: many
rows are legitimately mapped to upload_ids that ANOTHER yet-to-be-
migrated row currently holds (slug 52828 → upload_id 24236, but
another row has slug 24236 → upload_id 20137 — both must be renamed,
and the order in which we visit them can falsely look like a
collision). To avoid that we:

  Pass 1: UPDATE every target row to a guaranteed-unused placeholder
          (`__mig:<vs_id>`), clearing the historical external_id name
          space entirely.
  Pass 2: UPDATE each row from its placeholder to the target
          upload_id. Now the only possible collision is with a FILM
          row that already legitimately holds the upload_id (one
          physical file double-indexed as both film and episode) —
          in that case we DELETE the episode row, since the films
          pipeline has always used upload_id and is authoritative.

Idempotent: re-running on a fully-migrated DB is a no-op (slug-keyed
lookups miss because external_id is already numeric).

Usage on prod:
    DATABASE_URL=postgres://... \\
        python3 scripts/migrate-sledujteto-external-ids.py \\
            --dump data/sledujteto/sledujteto-series-raw-2026-05-20.json \\
            [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

try:
    import psycopg2
except ImportError as e:
    print(f"ERROR: missing dependency ({e}). pip install psycopg2-binary",
          file=sys.stderr)
    sys.exit(2)

log = logging.getLogger("migrate-sledujteto-external-ids")


def load_slug_to_upload_id(dump_path: Path) -> dict[str, str]:
    """Return {slug: str(upload_id)} from a sledujteto raw scrape.

    The dump's top-level key is the URL slug; each value's `id` field is
    the global numeric upload_id. Both are normalized to str so DB
    comparisons stay text-typed (video_sources.external_id is TEXT).
    """
    raw = json.loads(dump_path.read_text())
    out: dict[str, str] = {}
    for slug, v in raw.items():
        upload_id = v.get("id")
        if upload_id is None:
            continue
        out[str(slug)] = str(upload_id)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", type=Path, required=True,
                    help="Latest sledujteto raw scrape JSON")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would happen, don't touch DB")
    ns = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                         format="%(asctime)s %(levelname)s: %(message)s")

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL not set")
        return 2

    log.info("loading dump: %s", ns.dump)
    slug_to_upload = load_slug_to_upload_id(ns.dump)
    log.info("  %d slug→upload_id mappings loaded", len(slug_to_upload))

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT id FROM video_providers WHERE slug='sledujteto'")
    row = cur.fetchone()
    if not row:
        log.error("no 'sledujteto' provider in video_providers")
        return 2
    provider_id = row[0]
    log.info("sledujteto provider_id = %d", provider_id)

    # All sledujteto episode rows whose external_id is non-numeric OR a
    # known slug (numeric slugs and numeric upload_ids overlap, so we
    # widen the candidate set to ALL rows and let the dump lookup
    # decide). Films aren't touched — they've always used upload_id.
    cur.execute(
        """SELECT vs.id, vs.external_id, vs.episode_id, vs.is_alive
             FROM video_sources vs
            WHERE vs.provider_id = %s
              AND vs.episode_id IS NOT NULL
            ORDER BY vs.id""",
        (provider_id,),
    )
    rows = cur.fetchall()
    log.info("episode-attached sledujteto rows: %d", len(rows))

    # Classify rows up front:
    #   plan[vs_id] = (kind, payload)
    #   kind="dead"   payload=None        — slug not in dump, mark dead
    #   kind="noop"   payload=None        — already on upload_id
    #   kind="rename" payload=upload_id   — needs slug→upload_id rename
    plan: dict[int, tuple[str, str | None]] = {}
    n_noop = 0
    n_dead = 0
    n_rename = 0
    for vs_id, ext_id, episode_id, is_alive in rows:
        upload_id = slug_to_upload.get(str(ext_id))
        if upload_id is None:
            plan[vs_id] = ("dead", None)
            n_dead += 1
        elif str(ext_id) == upload_id:
            plan[vs_id] = ("noop", None)
            n_noop += 1
        else:
            plan[vs_id] = ("rename", upload_id)
            n_rename += 1

    log.info("plan: %d noop, %d dead, %d to rename", n_noop, n_dead, n_rename)

    # Set of external_ids belonging to rows we will NOT touch (films +
    # episode noops). After we clear all rename-targets to placeholders
    # in pass 1, the only remaining collisions in pass 2 are against
    # this set — those are genuine film-vs-episode duplicates.
    cur.execute(
        "SELECT external_id FROM video_sources WHERE provider_id = %s",
        (provider_id,),
    )
    all_ext = {r[0] for r in cur.fetchall()}
    rename_old_ext = {str(r[1]) for r in rows if plan[r[0]][0] == "rename"}
    untouched_ext = all_ext - rename_old_ext

    n_updated = 0
    n_marked_dead = 0
    n_collision_deleted = 0

    # ---------------- Pass 1: clear rename targets to placeholders -----------
    if not ns.dry_run:
        for vs_id, (kind, _) in plan.items():
            if kind == "rename":
                cur.execute(
                    "UPDATE video_sources SET external_id = %s WHERE id = %s",
                    (f"__mig:{vs_id}", vs_id),
                )
        log.info("pass 1: %d rows parked under __mig:<vs_id>", n_rename)
    else:
        log.info("pass 1 (dry run): would park %d rows under __mig:<vs_id>",
                  n_rename)

    # ---------------- Pass 2: rename to upload_id, handling collisions -------
    for vs_id, (kind, upload_id) in plan.items():
        if kind == "noop":
            continue
        if kind == "dead":
            if rows_by_id := {r[0]: r for r in rows}:
                _, _, episode_id, is_alive = rows_by_id[vs_id]
                if is_alive:
                    if not ns.dry_run:
                        cur.execute(
                            "UPDATE video_sources SET is_alive=false WHERE id=%s",
                            (vs_id,),
                        )
                    n_marked_dead += 1
            continue

        if upload_id in untouched_ext:
            # Genuine collision: a film (or an already-correct row)
            # holds this upload_id. Films pipeline is authoritative —
            # drop the episode row.
            log.warning("row vs_id=%d collides with existing film/correct row "
                         "upload_id=%s — deleting episode row",
                         vs_id, upload_id)
            if not ns.dry_run:
                cur.execute("DELETE FROM video_sources WHERE id = %s",
                             (vs_id,))
            n_collision_deleted += 1
            continue

        if not ns.dry_run:
            cur.execute(
                "UPDATE video_sources SET external_id = %s WHERE id = %s",
                (upload_id, vs_id),
            )
        n_updated += 1
        untouched_ext.add(upload_id)

    if ns.dry_run:
        conn.rollback()
        log.info("DRY RUN — rolled back")
    else:
        conn.commit()
        log.info("committed")

    print("\n=== summary ===", file=sys.stderr)
    print(f"  rows examined:       {len(rows)}", file=sys.stderr)
    print(f"  already correct:     {n_noop}", file=sys.stderr)
    print(f"  updated to upload_id:{n_updated}", file=sys.stderr)
    print(f"  marked dead (gone):  {n_marked_dead}", file=sys.stderr)
    print(f"  deleted (collision): {n_collision_deleted}", file=sys.stderr)
    if ns.dry_run:
        print("  *** DRY RUN — no changes committed ***", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
