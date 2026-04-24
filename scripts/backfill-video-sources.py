#!/usr/bin/env python3
"""Backfill `video_sources` + `video_source_subtitles` from the legacy
per-provider tables and denormalized columns (issue #607 / sub-issue #609).

After this runs, the new unified schema has parity with the legacy data;
the legacy tables + columns remain the source of truth until the reader
switch (#611) ships. The script is idempotent — re-running it is a no-op
modulo `last_seen` / `updated_at` column refreshes.

Data sources → target rows:

  1. sktorrent: for every (films | episodes | tv_episodes) row with
     `sktorrent_video_id IS NOT NULL`, insert one `video_sources` row with
       provider = sktorrent
       external_id = sktorrent_video_id::text
       cdn = sktorrent_cdn::text
       is_primary = true  (sktorrent is always 1:1 in legacy)
       audio_lang/lang_class derived from has_dub + has_subtitles
     Subtitle row inserted when has_subtitles = true (assumes CZ subs).

  2. prehrajto: copy every `film_prehrajto_uploads` row →
       provider = prehrajto
       external_id = upload_id
       is_primary = (upload_id = films.prehrajto_primary_upload_id)
       lang_class copied verbatim
       audio_lang/subtitle rows derived from lang_class

  3. sledujteto: copy every `film_sledujteto_uploads` row →
       provider = sledujteto
       external_id = file_id::text
       cdn = cdn
       is_primary = (file_id = films.sledujteto_primary_file_id)
       lang_class copied verbatim

Idempotence is guaranteed by `ON CONFLICT (provider_id, external_id) DO UPDATE`
in every INSERT.

Invariants verified at end:
  - no (owner, provider) pair has more than one is_primary row
  - every legacy primary pointer maps to a matching video_sources primary row

Usage:
    DATABASE_URL=postgres://... \\
    python3 scripts/backfill-video-sources.py [--dry-run] [--limit N] \\
        [--skip-sktorrent] [--skip-prehrajto] [--skip-sledujteto]

Flags:
  --dry-run    Wrap everything in a transaction and ROLLBACK at the end.
               Prints final summary counts but leaves the DB untouched.
  --limit N    Process at most N source rows from each provider path
               (ordered by id). For sanity-check runs on dev.
  --verbose    Log every insert, not just summaries.
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
    sys.exit(1)


log = logging.getLogger("backfill-video-sources")


# video_providers.slug → (audio_lang fallback, lang_class fallback). We cache
# the IDs looked up in provider_ids() after the first call.
PROVIDER_SLUGS = ("sktorrent", "prehrajto", "sledujteto")


def provider_ids(cur) -> dict[str, int]:
    cur.execute(
        "SELECT slug, id FROM video_providers WHERE slug = ANY(%s)",
        (list(PROVIDER_SLUGS),),
    )
    return {row["slug"]: row["id"] for row in cur.fetchall()}


def lang_class_to_audio_and_subs(lang_class: str | None,
                                 has_dub: bool = False,
                                 has_subtitles: bool = False
                                 ) -> tuple[str | None, str, list[str]]:
    """Map legacy lang signals to (audio_lang, lang_class, subtitle_langs).

    `lang_class` is the legacy-table enum
    (CZ_DUB|CZ_NATIVE|CZ_SUB|SK_DUB|SK_SUB|EN|UNKNOWN) when known, else None.
    `has_dub` / `has_subtitles` are the sktorrent-side booleans used as a
    fallback when no lang_class is available.

    Returns a triple:
      audio_lang  — 2/3-char ISO code or None (must satisfy the CHECK
                    constraint `^[a-z]{2,3}$`).
      lang_class  — normalized enum value. The DB CHECK
                    video_sources_lang_class_audio_consistency_check
                    enforces audio_lang ↔ lang_class consistency, so this
                    function is the single place where those two fields
                    are derived together.
      sub_langs   — list of subtitle language codes to insert as
                    video_source_subtitles rows (often empty).
    """
    if lang_class == "CZ_DUB":
        return "cs", "CZ_DUB", []
    if lang_class == "CZ_NATIVE":
        return "cs", "CZ_NATIVE", []
    if lang_class == "CZ_SUB":
        # Audio is original (often en), unknown from legacy data.
        return None, "CZ_SUB", ["cs"]
    if lang_class == "SK_DUB":
        return "sk", "SK_DUB", []
    if lang_class == "SK_SUB":
        return None, "SK_SUB", ["sk"]
    if lang_class == "EN":
        return "en", "EN", []

    # Fallback path for sktorrent (only has_dub / has_subtitles available).
    if has_dub and has_subtitles:
        # Both flags → dub is primary audio signal, subtitles coexist.
        return "cs", "CZ_DUB", ["cs"]
    if has_dub:
        return "cs", "CZ_DUB", []
    if has_subtitles:
        return None, "CZ_SUB", ["cs"]
    return None, "UNKNOWN", []


def upsert_video_source(cur, *,
                        provider_id: int,
                        external_id: str,
                        film_id: int | None = None,
                        episode_id: int | None = None,
                        tv_episode_id: int | None = None,
                        title: str | None = None,
                        duration_sec: int | None = None,
                        resolution_hint: str | None = None,
                        filesize_bytes: int | None = None,
                        view_count: int | None = None,
                        lang_class: str = "UNKNOWN",
                        audio_lang: str | None = None,
                        audio_detected_by: str | None = None,
                        cdn: str | None = None,
                        is_primary: bool = False,
                        is_alive: bool = True,
                        last_seen = None,
                        metadata = None,
                        ) -> int:
    """UPSERT a video_sources row. Returns the id of the (new or existing) row.

    Idempotence via `ON CONFLICT (provider_id, external_id)`: a re-run updates
    the mutable fields (is_alive, cdn, lang_class, …) but keeps the row id
    stable, so `video_source_subtitles.source_id` stays valid across re-runs.
    """
    cur.execute(
        """
        INSERT INTO video_sources (
            provider_id, film_id, episode_id, tv_episode_id,
            external_id, title, duration_sec, resolution_hint,
            filesize_bytes, view_count, lang_class, audio_lang,
            audio_detected_by, cdn, is_primary, is_alive,
            last_seen, metadata, updated_at
        ) VALUES (
            %(provider_id)s, %(film_id)s, %(episode_id)s, %(tv_episode_id)s,
            %(external_id)s, %(title)s, %(duration_sec)s, %(resolution_hint)s,
            %(filesize_bytes)s, %(view_count)s, %(lang_class)s, %(audio_lang)s,
            %(audio_detected_by)s, %(cdn)s, %(is_primary)s, %(is_alive)s,
            %(last_seen)s, %(metadata)s, NOW()
        )
        ON CONFLICT (provider_id, external_id) DO UPDATE SET
            film_id           = EXCLUDED.film_id,
            episode_id        = EXCLUDED.episode_id,
            tv_episode_id     = EXCLUDED.tv_episode_id,
            title             = COALESCE(EXCLUDED.title, video_sources.title),
            duration_sec      = COALESCE(EXCLUDED.duration_sec, video_sources.duration_sec),
            resolution_hint   = COALESCE(EXCLUDED.resolution_hint, video_sources.resolution_hint),
            filesize_bytes    = COALESCE(EXCLUDED.filesize_bytes, video_sources.filesize_bytes),
            view_count        = COALESCE(EXCLUDED.view_count, video_sources.view_count),
            lang_class        = EXCLUDED.lang_class,
            audio_lang        = EXCLUDED.audio_lang,
            audio_detected_by = EXCLUDED.audio_detected_by,
            cdn               = EXCLUDED.cdn,
            is_primary        = EXCLUDED.is_primary,
            is_alive          = EXCLUDED.is_alive,
            last_seen         = COALESCE(EXCLUDED.last_seen, video_sources.last_seen),
            metadata          = COALESCE(EXCLUDED.metadata, video_sources.metadata),
            updated_at        = NOW()
        RETURNING id
        """,
        dict(
            provider_id=provider_id,
            film_id=film_id,
            episode_id=episode_id,
            tv_episode_id=tv_episode_id,
            external_id=external_id,
            title=title,
            duration_sec=duration_sec,
            resolution_hint=resolution_hint,
            filesize_bytes=filesize_bytes,
            view_count=view_count,
            lang_class=lang_class,
            audio_lang=audio_lang,
            audio_detected_by=audio_detected_by,
            cdn=cdn,
            is_primary=is_primary,
            is_alive=is_alive,
            last_seen=last_seen,
            metadata=psycopg2.extras.Json(metadata) if metadata else None,
        ),
    )
    return cur.fetchone()["id"]


def upsert_subtitle(cur, source_id: int, lang: str) -> None:
    """Insert a subtitle row if absent. URL + format stay NULL (filled by
    the live resolver at play-time for sledujteto / prehrajto)."""
    cur.execute(
        """
        INSERT INTO video_source_subtitles (source_id, lang)
        VALUES (%s, %s)
        ON CONFLICT (source_id, lang, is_forced, COALESCE(format, ''))
        DO NOTHING
        """,
        (source_id, lang),
    )


def backfill_sktorrent(cur, providers: dict[str, int], limit: int | None
                       ) -> tuple[int, int]:
    """Backfill sktorrent sources for films / episodes / tv_episodes.

    Returns (inserted_rows, skipped_rows) counts.
    """
    provider_id = providers["sktorrent"]
    inserted = 0
    skipped = 0

    for table, parent_col in (("films", "film_id"),
                              ("episodes", "episode_id"),
                              ("tv_episodes", "tv_episode_id")):
        qualities_col = "sktorrent_qualities" if table != "episodes" else "sktorrent_qualities"
        cdn_col = "sktorrent_cdn"
        # `has_dub` / `has_subtitles` exist on all three tables for sktorrent
        # legacy detection.
        limit_clause = f"LIMIT {int(limit)}" if limit else ""
        cur.execute(
            f"""
            SELECT id, sktorrent_video_id, {cdn_col} AS cdn,
                   {qualities_col} AS qualities, has_dub, has_subtitles
            FROM {table}
            WHERE sktorrent_video_id IS NOT NULL
            ORDER BY id
            {limit_clause}
            """
        )
        rows = cur.fetchall()
        log.info("sktorrent/%s: %d candidates", table, len(rows))

        for row in rows:
            audio_lang, lang_class, sub_langs = lang_class_to_audio_and_subs(
                None, has_dub=row["has_dub"], has_subtitles=row["has_subtitles"])
            metadata = {"qualities": row["qualities"]} if row["qualities"] else None

            parent_kwargs = {parent_col: row["id"]}
            try:
                source_id = upsert_video_source(
                    cur,
                    provider_id=provider_id,
                    external_id=str(row["sktorrent_video_id"]),
                    **parent_kwargs,
                    lang_class=lang_class,
                    audio_lang=audio_lang,
                    audio_detected_by="title_regex" if lang_class != "UNKNOWN" else None,
                    cdn=str(row["cdn"]) if row["cdn"] is not None else None,
                    is_primary=True,  # sktorrent is 1:1 in legacy
                    is_alive=True,
                    metadata=metadata,
                )
                for lang in sub_langs:
                    upsert_subtitle(cur, source_id, lang)
                inserted += 1
            except psycopg2.Error as e:
                log.warning("sktorrent/%s id=%d video_id=%s: %s",
                            table, row["id"], row["sktorrent_video_id"], e)
                skipped += 1

    return inserted, skipped


def backfill_prehrajto(cur, providers: dict[str, int], limit: int | None
                       ) -> tuple[int, int]:
    """Backfill prehrajto sources from film_prehrajto_uploads."""
    provider_id = providers["prehrajto"]
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    cur.execute(
        f"""
        SELECT u.film_id, u.upload_id, u.url, u.title, u.duration_sec,
               u.view_count, u.lang_class, u.resolution_hint,
               u.discovered_at, u.last_seen_at, u.is_alive, u.is_direct,
               f.prehrajto_primary_upload_id
        FROM film_prehrajto_uploads u
        JOIN films f ON f.id = u.film_id
        ORDER BY u.film_id, u.upload_id
        {limit_clause}
        """
    )
    rows = cur.fetchall()
    log.info("prehrajto: %d uploads", len(rows))

    inserted = 0
    skipped = 0
    for row in rows:
        audio_lang, lang_class, sub_langs = lang_class_to_audio_and_subs(row["lang_class"])
        is_primary = row["upload_id"] == row["prehrajto_primary_upload_id"]
        metadata = {
            "url": row["url"],
            "is_direct": row["is_direct"],
            "discovered_at": row["discovered_at"].isoformat() if row["discovered_at"] else None,
        }
        try:
            source_id = upsert_video_source(
                cur,
                provider_id=provider_id,
                external_id=row["upload_id"],
                film_id=row["film_id"],
                title=row["title"],
                duration_sec=row["duration_sec"],
                resolution_hint=row["resolution_hint"],
                view_count=row["view_count"],
                lang_class=lang_class,
                audio_lang=audio_lang,
                audio_detected_by="title_regex" if lang_class != "UNKNOWN" else None,
                is_primary=is_primary,
                is_alive=row["is_alive"],
                last_seen=row["last_seen_at"],
                metadata=metadata,
            )
            for lang in sub_langs:
                upsert_subtitle(cur, source_id, lang)
            inserted += 1
        except psycopg2.Error as e:
            log.warning("prehrajto film_id=%d upload_id=%s: %s",
                        row["film_id"], row["upload_id"], e)
            skipped += 1
    return inserted, skipped


def backfill_sledujteto(cur, providers: dict[str, int], limit: int | None
                        ) -> tuple[int, int]:
    """Backfill sledujteto sources from film_sledujteto_uploads."""
    provider_id = providers["sledujteto"]
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    cur.execute(
        f"""
        SELECT u.film_id, u.file_id, u.title, u.duration_sec,
               u.lang_class, u.resolution_hint, u.filesize_bytes, u.cdn,
               u.is_alive, u.last_seen,
               f.sledujteto_primary_file_id
        FROM film_sledujteto_uploads u
        JOIN films f ON f.id = u.film_id
        ORDER BY u.film_id, u.file_id
        {limit_clause}
        """
    )
    rows = cur.fetchall()
    log.info("sledujteto: %d uploads", len(rows))

    inserted = 0
    skipped = 0
    for row in rows:
        audio_lang, lang_class, sub_langs = lang_class_to_audio_and_subs(row["lang_class"])
        is_primary = row["file_id"] == row["sledujteto_primary_file_id"]
        try:
            source_id = upsert_video_source(
                cur,
                provider_id=provider_id,
                external_id=str(row["file_id"]),
                film_id=row["film_id"],
                title=row["title"],
                duration_sec=row["duration_sec"],
                resolution_hint=row["resolution_hint"],
                filesize_bytes=row["filesize_bytes"],
                lang_class=lang_class,
                audio_lang=audio_lang,
                audio_detected_by="title_regex" if lang_class != "UNKNOWN" else None,
                cdn=row["cdn"],
                is_primary=is_primary,
                is_alive=row["is_alive"],
                last_seen=row["last_seen"],
            )
            for lang in sub_langs:
                upsert_subtitle(cur, source_id, lang)
            inserted += 1
        except psycopg2.Error as e:
            log.warning("sledujteto film_id=%d file_id=%d: %s",
                        row["film_id"], row["file_id"], e)
            skipped += 1
    return inserted, skipped


def assert_invariants(cur) -> bool:
    """Post-backfill sanity checks. Returns True when all invariants hold.

    1. No (provider, parent) pair has >1 is_primary row.
       (Partial unique indexes enforce this at DB level, so a violation here
       would indicate an index was dropped or a concurrent writer bypassed
       the constraint.)
    2. Every legacy primary pointer maps to a matching video_sources primary.
    """
    ok = True

    # Invariant 1 — should always pass if the partial unique indexes are in
    # place. Sanity net in case we run this against a DB where the migration
    # is partially applied.
    cur.execute(
        """
        SELECT provider_id,
               COALESCE(film_id::text, 'E'||episode_id::text, 'T'||tv_episode_id::text) AS parent,
               COUNT(*) AS n
        FROM video_sources
        WHERE is_primary
        GROUP BY provider_id, parent
        HAVING COUNT(*) > 1
        """
    )
    dup_primaries = cur.fetchall()
    if dup_primaries:
        log.error("INVARIANT 1 FAILED: %d (provider, parent) pairs have "
                  "multiple primary rows", len(dup_primaries))
        ok = False

    # Invariant 2 — legacy primary ↔ video_sources.is_primary alignment.
    # Run per-provider so the error messages point at the right legacy
    # column when something is off.
    legacy_checks = [
        ("prehrajto",
         "SELECT f.id, f.prehrajto_primary_upload_id AS pointer FROM films f "
         "WHERE f.prehrajto_primary_upload_id IS NOT NULL"),
        ("sledujteto",
         "SELECT f.id, f.sledujteto_primary_file_id::text AS pointer FROM films f "
         "WHERE f.sledujteto_primary_file_id IS NOT NULL"),
    ]
    for slug, legacy_sql in legacy_checks:
        cur.execute(
            f"""
            WITH legacy AS ({legacy_sql}),
                 provider AS (SELECT id FROM video_providers WHERE slug = %s)
            SELECT l.id, l.pointer
            FROM legacy l, provider p
            WHERE NOT EXISTS (
                SELECT 1 FROM video_sources vs
                WHERE vs.film_id = l.id
                  AND vs.provider_id = p.id
                  AND vs.is_primary
                  AND vs.external_id = l.pointer
            )
            """,
            (slug,),
        )
        mismatched = cur.fetchall()
        if mismatched:
            log.error("INVARIANT 2 FAILED (%s): %d films have legacy primary "
                      "pointer but no matching video_sources primary. "
                      "First 5: %s", slug, len(mismatched),
                      [(r["id"], r["pointer"]) for r in mismatched[:5]])
            ok = False

    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--skip-sktorrent", action="store_true")
    ap.add_argument("--skip-prehrajto", action="store_true")
    ap.add_argument("--skip-sledujteto", action="store_true")
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
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    totals = {"inserted": 0, "skipped": 0}
    try:
        providers = provider_ids(cur)
        log.info("Providers: %s", providers)

        if not args.skip_sktorrent:
            i, s = backfill_sktorrent(cur, providers, args.limit)
            totals["inserted"] += i
            totals["skipped"] += s
            log.info("sktorrent done: inserted=%d skipped=%d", i, s)

        if not args.skip_prehrajto:
            i, s = backfill_prehrajto(cur, providers, args.limit)
            totals["inserted"] += i
            totals["skipped"] += s
            log.info("prehrajto done: inserted=%d skipped=%d", i, s)

        if not args.skip_sledujteto:
            i, s = backfill_sledujteto(cur, providers, args.limit)
            totals["inserted"] += i
            totals["skipped"] += s
            log.info("sledujteto done: inserted=%d skipped=%d", i, s)

        log.info("Totals: inserted=%d skipped=%d", totals["inserted"], totals["skipped"])

        # Invariants only meaningful on a FULL run (no --limit), otherwise
        # "legacy pointer with no video_sources match" is expected.
        if args.limit is None:
            ok = assert_invariants(cur)
            if not ok:
                log.error("Post-backfill invariants failed; rolling back.")
                conn.rollback()
                return 1

        if args.dry_run:
            conn.rollback()
            log.info("DRY RUN: rolled back.")
        else:
            conn.commit()
            log.info("Committed.")
    except Exception:
        conn.rollback()
        log.exception("Error during backfill — rolled back")
        return 1
    finally:
        cur.close()
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
