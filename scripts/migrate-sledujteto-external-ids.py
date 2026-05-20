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


def load_slug_to_upload_id(
    dump_path: Path,
) -> tuple[dict[str, tuple[str, str]], set[str]]:
    """Return ({slug: (upload_id, name)}, set of known upload_ids).

    The dump's top-level key is the URL slug; each value's `id` field is
    the global numeric upload_id; `name` is the human title. All strings
    are normalized to str so DB comparisons stay text-typed
    (video_sources.external_id is TEXT). `name` lets the migration
    detect slug recycling: if the row's stored title doesn't match the
    dump's current name for the slug, the underlying upload has been
    deleted and the slug rebound to an unrelated file.

    The second return is the set of every upload_id present in the dump.
    Re-running the migration on a fully-migrated DB needs this to
    recognise rows whose `external_id` is already the numeric upload_id
    (rather than mis-classifying them as "dead" because they aren't slug
    keys).

    Raises if a numeric `id` appears under two different slugs. If
    sledujteto ever returned two slugs pointing at the same physical
    file, pass 2 of the migration would treat the second rename as a
    collision and could delete a valid row — fail loud at load time
    instead of silently corrupting data.
    """
    raw = json.loads(dump_path.read_text())
    out: dict[str, tuple[str, str]] = {}
    upload_id_seen: dict[str, str] = {}  # upload_id → first slug we saw
    for slug, v in raw.items():
        upload_id = v.get("id")
        if upload_id is None:
            continue
        uid = str(upload_id)
        prior = upload_id_seen.get(uid)
        if prior is not None and prior != str(slug):
            raise RuntimeError(
                f"dump invariant violated: upload_id={uid} appears under "
                f"two slugs ({prior!r} and {slug!r}); migration cannot "
                "safely proceed because pass 2 would treat the second "
                "rename as a collision."
            )
        upload_id_seen[uid] = str(slug)
        name = (v.get("name") or "").strip()
        out[str(slug)] = (uid, name)
    return out, set(upload_id_seen.keys())


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
    slug_to_upload, known_upload_ids = load_slug_to_upload_id(ns.dump)
    log.info("  %d slug→upload_id mappings loaded (%d unique upload_ids)",
              len(slug_to_upload), len(known_upload_ids))

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

    # All sledujteto episode rows. Read `title` too so we can detect slug
    # recycling (row's title doesn't match the dump's current name for
    # the slug → underlying upload was deleted and the slug was rebound
    # to an unrelated file).
    cur.execute(
        """SELECT vs.id, vs.external_id, vs.episode_id, vs.is_alive, vs.title
             FROM video_sources vs
            WHERE vs.provider_id = %s
              AND vs.episode_id IS NOT NULL
            ORDER BY vs.id""",
        (provider_id,),
    )
    rows = cur.fetchall()
    log.info("episode-attached sledujteto rows: %d", len(rows))

    # Build vs_id → row map ONCE; pass 2's "dead" branch needs is_alive.
    rows_by_id: dict[int, tuple] = {r[0]: r for r in rows}

    # Classify rows up front:
    #   plan[vs_id] = (kind, payload)
    #   kind="dead"   payload=None        — underlying upload is gone:
    #                                       ext_id isn't a known slug or
    #                                       upload_id, OR ext_id is a slug
    #                                       whose dump entry has a title
    #                                       different from this row (= the
    #                                       slug was recycled).
    #   kind="noop"   payload=None        — ext_id already equals a known
    #                                       upload_id (rerun-safe).
    #   kind="rename" payload=upload_id   — slug → upload_id rename, dump
    #                                       title matches the row's title.
    plan: dict[int, tuple[str, str | None]] = {}
    n_noop = 0
    n_dead = 0
    n_rename = 0
    for vs_id, ext_id, episode_id, is_alive, row_title in rows:
        ext_str = str(ext_id)
        dump_entry = slug_to_upload.get(ext_str)
        if dump_entry is None:
            # ext_id is not a known slug. If it IS a known upload_id, the
            # row was already migrated by a prior run — leave it alone.
            # Otherwise the underlying upload has been deleted on
            # sledujteto and the row is dead.
            if ext_str in known_upload_ids:
                plan[vs_id] = ("noop", None)
                n_noop += 1
            else:
                plan[vs_id] = ("dead", None)
                n_dead += 1
            continue

        upload_id, dump_name = dump_entry
        if ext_str == upload_id:
            plan[vs_id] = ("noop", None)
            n_noop += 1
            continue

        # ext_id is a slug in the dump but its current upload_id differs
        # from the stored external_id. Two scenarios:
        #   1. Row's title matches the dump's name → simple slug→upload_id
        #      rename of a row that hasn't been migrated yet.
        #   2. Row's title differs from the dump's name → the slug was
        #      recycled after the original upload was deleted; this row
        #      references a vanished file. Mark dead — renaming would
        #      silently re-point it to an unrelated upload (the original
        #      bug this migration fixes).
        if dump_name and row_title and row_title.strip() != dump_name:
            plan[vs_id] = ("dead", None)
            n_dead += 1
        else:
            plan[vs_id] = ("rename", upload_id)
            n_rename += 1

    log.info("plan: %d noop, %d dead, %d to rename", n_noop, n_dead, n_rename)

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
            _, _, _episode_id, is_alive, _ = rows_by_id[vs_id]
            if is_alive:
                if not ns.dry_run:
                    cur.execute(
                        "UPDATE video_sources SET is_alive=false WHERE id=%s",
                        (vs_id,),
                    )
                n_marked_dead += 1
            continue

        # Look up the colliding row at write time. After pass 1, every
        # rename source has been parked under `__mig:<vs_id>`, so the only
        # rows still holding non-placeholder external_ids are: films,
        # episode noops, and dead-episode rows (whose external_id is left
        # untouched on purpose). Decide what to do based on which:
        #   - film row              → episode row loses (films are authoritative
        #                             and have always used upload_id correctly)
        #   - alive episode row     → shouldn't happen post-pass-1 (would
        #                             require slug==upload_id on a noop), but
        #                             defensively skip and log
        #   - dead episode row      → DELETE the dead row, let the rename land
        cur.execute(
            "SELECT id, film_id, episode_id, is_alive FROM video_sources "
            "WHERE provider_id = %s AND external_id = %s",
            (provider_id, upload_id),
        )
        collider = cur.fetchone()
        if collider is not None:
            col_id, col_film, col_ep, col_alive = collider
            if col_film is not None:
                # Film holds this upload_id — drop the episode row.
                log.warning("row vs_id=%d rename to upload_id=%s collides "
                             "with film vs_id=%d — deleting episode row",
                             vs_id, upload_id, col_id)
                if not ns.dry_run:
                    cur.execute("DELETE FROM video_sources WHERE id = %s",
                                 (vs_id,))
                n_collision_deleted += 1
                continue
            if col_ep is not None and not col_alive:
                # Dead episode row blocking the upload_id — delete the
                # stale row, let our rename land.
                log.warning("row vs_id=%d rename to upload_id=%s blocked by "
                             "dead episode vs_id=%d — deleting dead row",
                             vs_id, upload_id, col_id)
                if not ns.dry_run:
                    cur.execute("DELETE FROM video_sources WHERE id = %s",
                                 (col_id,))
            else:
                # Live episode row already on upload_id — same physical
                # upload claimed by two different episode rows (likely a
                # parser-side cross-attach). The existing alive row wins;
                # delete our placeholder so it doesn't linger in the table
                # with a `__mig:<id>` external_id forever. Importer will
                # re-create the lost attach next run if the upload genuinely
                # belongs to two different shows.
                log.warning("row vs_id=%d rename to upload_id=%s collides "
                             "with alive episode vs_id=%d — deleting our "
                             "placeholder row (the alive row wins)",
                             vs_id, upload_id, col_id)
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
