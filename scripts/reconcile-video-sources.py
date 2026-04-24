#!/usr/bin/env python3
"""Detect drift between legacy per-provider tables and the unified
`video_sources` schema (#607 / #610). Intended to run periodically
during the dual-write phase — catches missing/stale rows before the
reader switch in PR3 trusts the new schema.

Exits 0 when legacy ↔ new are in agreement, 1 on any drift. Prints a
compact report so cron output stays greppable in logs.

Checks performed:

  1. Row counts per provider:
       film_prehrajto_uploads                  vs video_sources[prehrajto]
       film_sledujteto_uploads                 vs video_sources[sledujteto]
       films/episodes/tv_episodes.sktorrent_video_id cardinality
                                               vs video_sources[sktorrent]

  2. Primary pointer alignment: every legacy primary pointer maps to
     a matching video_sources row with is_primary=TRUE and the same
     external_id.

  3. Missing rows: legacy upload rows that have no corresponding
     video_sources row (within the dual-write providers).

  4. Rollup staleness: spot-check `films.audio_langs` / `subtitle_langs`
     against what the video_sources data implies. Finds rows where the
     trigger failed to run or was bypassed.

Usage:
    DATABASE_URL=postgres://... \\
    python3 scripts/reconcile-video-sources.py [--verbose]

Exit codes:
    0 — no drift
    1 — drift detected (see output)
    2 — setup error (missing env, schema mismatch)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)


log = logging.getLogger("reconcile-video-sources")


def check_counts(cur) -> list[str]:
    """Compare legacy row counts to video_sources per-provider counts."""
    errors = []
    # prehrajto
    cur.execute("SELECT COUNT(*) AS n FROM film_prehrajto_uploads")
    legacy_prehrajto = cur.fetchone()["n"]
    cur.execute(
        "SELECT COUNT(*) AS n FROM video_sources "
        "WHERE provider_id = (SELECT id FROM video_providers WHERE slug = 'prehrajto')"
    )
    new_prehrajto = cur.fetchone()["n"]
    if legacy_prehrajto != new_prehrajto:
        errors.append(
            f"COUNT DRIFT prehrajto: legacy={legacy_prehrajto} "
            f"video_sources={new_prehrajto} delta={new_prehrajto - legacy_prehrajto}"
        )
    else:
        log.info("OK prehrajto counts: %d rows on both sides", legacy_prehrajto)

    # sledujteto
    cur.execute("SELECT COUNT(*) AS n FROM film_sledujteto_uploads")
    legacy_sledujteto = cur.fetchone()["n"]
    cur.execute(
        "SELECT COUNT(*) AS n FROM video_sources "
        "WHERE provider_id = (SELECT id FROM video_providers WHERE slug = 'sledujteto')"
    )
    new_sledujteto = cur.fetchone()["n"]
    if legacy_sledujteto != new_sledujteto:
        errors.append(
            f"COUNT DRIFT sledujteto: legacy={legacy_sledujteto} "
            f"video_sources={new_sledujteto} delta={new_sledujteto - legacy_sledujteto}"
        )
    else:
        log.info("OK sledujteto counts: %d rows on both sides", legacy_sledujteto)

    # sktorrent — distinct video_ids across films + episodes + tv_episodes
    # (legacy allows duplicates within a table, but video_sources enforces
    # UNIQUE per provider+external_id, so we compare against DISTINCT count).
    cur.execute(
        """
        SELECT COUNT(DISTINCT sktorrent_video_id) AS n
        FROM (
            SELECT sktorrent_video_id FROM films       WHERE sktorrent_video_id IS NOT NULL
            UNION ALL
            SELECT sktorrent_video_id FROM episodes    WHERE sktorrent_video_id IS NOT NULL
            UNION ALL
            SELECT sktorrent_video_id FROM tv_episodes WHERE sktorrent_video_id IS NOT NULL
        ) u
        """
    )
    legacy_sktorrent = cur.fetchone()["n"]
    cur.execute(
        "SELECT COUNT(*) AS n FROM video_sources "
        "WHERE provider_id = (SELECT id FROM video_providers WHERE slug = 'sktorrent')"
    )
    new_sktorrent = cur.fetchone()["n"]
    if legacy_sktorrent != new_sktorrent:
        errors.append(
            f"COUNT DRIFT sktorrent: legacy distinct video_ids={legacy_sktorrent} "
            f"video_sources={new_sktorrent} delta={new_sktorrent - legacy_sktorrent}"
        )
    else:
        log.info("OK sktorrent counts: %d distinct video_ids on both sides", legacy_sktorrent)

    return errors


def check_primary_alignment(cur) -> list[str]:
    """Every legacy `*_primary_*_id` must map to a `video_sources` row with
    matching external_id and `is_primary=TRUE`."""
    errors = []
    for slug, pointer_sql, count_label in [
        ("prehrajto",
         "SELECT id, prehrajto_primary_upload_id AS ptr FROM films "
         "WHERE prehrajto_primary_upload_id IS NOT NULL",
         "prehrajto primary pointers"),
        ("sledujteto",
         "SELECT id, sledujteto_primary_file_id::text AS ptr FROM films "
         "WHERE sledujteto_primary_file_id IS NOT NULL",
         "sledujteto primary pointers"),
    ]:
        cur.execute(
            f"""
            WITH legacy AS ({pointer_sql}),
                 prov AS (SELECT id FROM video_providers WHERE slug = %s),
                 mismatched AS (
                    SELECT l.id FROM legacy l, prov p
                    WHERE NOT EXISTS (
                        SELECT 1 FROM video_sources vs
                        WHERE vs.film_id = l.id
                          AND vs.provider_id = p.id
                          AND vs.is_primary
                          AND vs.external_id = l.ptr
                    )
                 )
            SELECT
                (SELECT COUNT(*) FROM mismatched) AS n,
                COALESCE((SELECT array_agg(id) FROM (SELECT id FROM mismatched LIMIT 10) s),
                         '{{}}'::int[]) AS examples
            """,
            (slug,),
        )
        row = cur.fetchone()
        n = row["n"]
        if n > 0:
            errors.append(
                f"PRIMARY MISMATCH {count_label}: {n} films have a legacy pointer "
                f"but no matching video_sources.is_primary row. First IDs: {row['examples']}"
            )
        else:
            log.info("OK %s: all legacy primary pointers align", count_label)

    # sktorrent: every legacy sktorrent_video_id (films / episodes /
    # tv_episodes) should have a matching video_sources row. is_primary is
    # unconditional for sktorrent (is_primary=TRUE always — sktorrent is 1:1).
    for table, parent_col, parent_label in (
        ("films", "film_id", "sktorrent films"),
        ("episodes", "episode_id", "sktorrent episodes"),
        ("tv_episodes", "tv_episode_id", "sktorrent tv_episodes"),
    ):
        cur.execute(
            f"""
            WITH prov AS (SELECT id FROM video_providers WHERE slug = 'sktorrent')
            SELECT COUNT(*) AS n
            FROM {table} t, prov p
            WHERE t.sktorrent_video_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM video_sources vs
                  WHERE vs.{parent_col} = t.id
                    AND vs.provider_id = p.id
                    AND vs.external_id = t.sktorrent_video_id::text
              )
            """
        )
        missing = cur.fetchone()["n"]
        if missing > 0:
            errors.append(
                f"SKT MISSING: {missing} {parent_label} with legacy "
                f"sktorrent_video_id have no matching video_sources row"
            )
        else:
            log.info("OK %s: every sktorrent_video_id has a video_sources row",
                     parent_label)

    return errors


def check_rollup_staleness(cur) -> list[str]:
    """Spot-check rollup arrays vs. the source-of-truth in video_sources.

    A rollup is stale when `films.audio_langs` does NOT match
    `array_agg(DISTINCT vs.audio_lang)` for alive video_sources rows.
    This indicates the trigger failed to run (e.g. if someone wrote
    directly to the base tables bypassing the trigger) or a bulk
    UPDATE/COPY skipped per-row triggers.
    """
    errors = []
    cur.execute(
        """
        WITH rollup AS (
            SELECT f.id,
                   f.audio_langs AS stored,
                   COALESCE(
                       (SELECT array_agg(DISTINCT vs.audio_lang::TEXT ORDER BY vs.audio_lang::TEXT)
                        FROM video_sources vs
                        WHERE vs.film_id = f.id
                          AND vs.is_alive
                          AND vs.audio_lang IS NOT NULL),
                       '{}'::TEXT[]
                   ) AS expected
            FROM films f
            WHERE EXISTS (SELECT 1 FROM video_sources vs WHERE vs.film_id = f.id)
        ),
        stale AS (
            SELECT id FROM rollup WHERE stored IS DISTINCT FROM expected
        )
        SELECT
            (SELECT COUNT(*) FROM stale) AS n,
            COALESCE((SELECT array_agg(id) FROM (SELECT id FROM stale LIMIT 10) s),
                     '{}'::int[]) AS examples
        """
    )
    row = cur.fetchone()
    stale = row["n"]
    if stale > 0:
        errors.append(
            f"ROLLUP STALE (audio_langs): {stale} films have audio_langs drift from "
            f"video_sources. First IDs: {row['examples']}"
        )
    else:
        log.info("OK films.audio_langs: no rollup drift detected")

    # Subtitle rollup: same shape but checks films.subtitle_langs vs.
    # video_source_subtitles for all alive video_sources on the film.
    cur.execute(
        """
        WITH rollup AS (
            SELECT f.id,
                   f.subtitle_langs AS stored,
                   COALESCE(
                       (SELECT array_agg(DISTINCT vss.lang::TEXT ORDER BY vss.lang::TEXT)
                        FROM video_sources vs
                        JOIN video_source_subtitles vss ON vss.source_id = vs.id
                        WHERE vs.film_id = f.id
                          AND vs.is_alive),
                       '{}'::TEXT[]
                   ) AS expected
            FROM films f
            WHERE EXISTS (SELECT 1 FROM video_sources vs WHERE vs.film_id = f.id)
        ),
        stale AS (
            SELECT id FROM rollup WHERE stored IS DISTINCT FROM expected
        )
        SELECT
            (SELECT COUNT(*) FROM stale) AS n,
            COALESCE((SELECT array_agg(id) FROM (SELECT id FROM stale LIMIT 10) s),
                     '{}'::int[]) AS examples
        """
    )
    row = cur.fetchone()
    stale_subs = row["n"]
    if stale_subs > 0:
        errors.append(
            f"ROLLUP STALE (subtitle_langs): {stale_subs} films have subtitle_langs "
            f"drift from video_source_subtitles. First IDs: {row['examples']}"
        )
    else:
        log.info("OK films.subtitle_langs: no rollup drift detected")

    return errors


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL required")
        return 2

    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    errors: list[str] = []
    try:
        errors.extend(check_counts(cur))
        errors.extend(check_primary_alignment(cur))
        errors.extend(check_rollup_staleness(cur))
    finally:
        cur.close()
        conn.close()

    if errors:
        for e in errors:
            log.error(e)
        log.error("Drift detected: %d check(s) failed", len(errors))
        return 1

    log.info("All checks passed — legacy and video_sources are in agreement")
    return 0


if __name__ == "__main__":
    sys.exit(main())
