#!/usr/bin/env python3
"""One-shot backfill — set films.has_dub / has_subtitles for every film and
episode that the auto-import pipeline created BEFORE we started passing
language flags (issue #423, runs 3–8).

Reads every row in `import_items` that references a real target_film_id /
target_episode_id, reparses its `sktorrent_title` with the shared
`title_parser`, and ORs the derived dub/subs flags onto the target row.

Idempotent: we use `has_dub OR %s` so rerunning can only turn flags ON.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import psycopg2

from scripts.auto_import.title_parser import parse_sktorrent_title
from scripts.video_sources_helper import (
    dual_write_sktorrent,
    get_provider_ids,
)


def _flags(title: str) -> tuple[bool, bool]:
    p = parse_sktorrent_title(title)
    has_dub = any(l in ("DUB_CZ", "DUB_SK", "CZ", "SK") for l in p.langs)
    has_subs = any(l in ("SUBS_CZ", "SUBS_SK") for l in p.langs)
    return has_dub, has_subs


def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        raise SystemExit("DATABASE_URL required")
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    providers = get_provider_ids(cur)

    updated_films = 0
    cur.execute(
        "SELECT DISTINCT ON (target_film_id) target_film_id, sktorrent_title "
        "FROM import_items WHERE target_film_id IS NOT NULL "
        "ORDER BY target_film_id, id DESC"
    )
    for film_id, title in cur.fetchall():
        has_dub, has_subs = _flags(title)
        if not has_dub and not has_subs:
            continue
        cur.execute(
            "UPDATE films SET "
            "has_dub = has_dub OR %s, "
            "has_subtitles = has_subtitles OR %s "
            "WHERE id = %s",
            (has_dub, has_subs, film_id),
        )
        if cur.rowcount:
            updated_films += 1
            # Dual-write (#607 / #610): refresh the video_sources row for
            # this film's sktorrent source so the lang classification on
            # the new schema matches the flags we just ORed into `films`.
            cur.execute(
                "SELECT sktorrent_video_id, sktorrent_cdn, sktorrent_qualities "
                "FROM films WHERE id = %s",
                (film_id,),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                dual_write_sktorrent(
                    cur,
                    providers=providers,
                    film_id=film_id,
                    sktorrent_video_id=row[0],
                    sktorrent_cdn=row[1],
                    sktorrent_qualities=row[2],
                    has_dub=has_dub,
                    has_subtitles=has_subs,
                )

    updated_episodes = 0
    cur.execute(
        "SELECT DISTINCT ON (target_episode_id) target_episode_id, sktorrent_title "
        "FROM import_items WHERE target_episode_id IS NOT NULL "
        "ORDER BY target_episode_id, id DESC"
    )
    for ep_id, title in cur.fetchall():
        has_dub, has_subs = _flags(title)
        if not has_dub and not has_subs:
            continue
        cur.execute(
            "UPDATE episodes SET "
            "has_dub = has_dub OR %s, "
            "has_subtitles = has_subtitles OR %s "
            "WHERE id = %s",
            (has_dub, has_subs, ep_id),
        )
        if cur.rowcount:
            updated_episodes += 1
            cur.execute(
                "SELECT sktorrent_video_id, sktorrent_cdn, sktorrent_qualities "
                "FROM episodes WHERE id = %s",
                (ep_id,),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                dual_write_sktorrent(
                    cur,
                    providers=providers,
                    episode_id=ep_id,
                    sktorrent_video_id=row[0],
                    sktorrent_cdn=row[1],
                    sktorrent_qualities=row[2],
                    has_dub=has_dub,
                    has_subtitles=has_subs,
                )

    conn.commit()
    print(f"backfilled {updated_films} films, {updated_episodes} episodes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
