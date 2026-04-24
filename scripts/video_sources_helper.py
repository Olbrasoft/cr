"""Shared dual-write helper for the unified `video_sources` schema (#607).

All importer scripts (sktorrent / prehrajto / sledujteto) call into this
module after their legacy INSERT/UPSERT, in the SAME transaction, so the
new schema stays in lock-step with the legacy per-provider tables.

Usage pattern (in every importer, inside a psycopg2 transaction):

    from video_sources_helper import (
        get_provider_ids, upsert_video_source, upsert_subtitle,
        lang_class_to_audio_and_subs,
    )

    providers = get_provider_ids(cur)   # {slug: id}, cache per-cursor

    # --- legacy write stays unchanged ---
    cur.execute("INSERT INTO film_prehrajto_uploads ...", ...)

    # --- dual-write into video_sources ---
    audio_lang, lang_class, sub_langs = lang_class_to_audio_and_subs(
        lang_class=row["lang_class"])
    source_id = upsert_video_source(
        cur,
        provider_id=providers["prehrajto"],
        external_id=upload_id,
        film_id=film_id,
        title=title, duration_sec=duration_sec, view_count=view_count,
        resolution_hint=resolution_hint,
        lang_class=lang_class, audio_lang=audio_lang,
        audio_detected_by="title_regex" if lang_class != "UNKNOWN" else None,
        is_primary=(upload_id == primary_upload_id),
        is_alive=True,
    )
    for sub_lang in sub_langs:
        upsert_subtitle(cur, source_id, sub_lang)

The helper enforces:
  - ON CONFLICT (provider_id, external_id) updates the existing row, so
    re-runs are no-ops modulo `updated_at`. This plays well with the
    schema's partial-unique constraints on `is_primary`.
  - `lang_class`/`audio_lang` pair derived by `lang_class_to_audio_and_subs`
    always satisfies the DB CHECK constraint.
  - Sub rows are upserted via COALESCE(format,'') to tolerate format=NULL
    during the window between discovery and first resolve.

Schema reference: cr-infra/migrations/20260529_058_video_sources_unified.sql
"""
from __future__ import annotations

import logging

try:
    import psycopg2.extras
except ImportError:
    raise


PROVIDER_SLUGS = ("sktorrent", "prehrajto", "sledujteto")

# Cache the provider ids per cursor object so we don't re-query the lookup
# table on every upsert. Keyed by id(cur) because psycopg2 cursors are not
# hashable across DB reconnects — but within a single run the same cursor
# is reused thousands of times.
_PROVIDER_CACHE: dict[int, dict[str, int]] = {}


def get_provider_ids(cur) -> dict[str, int]:
    """Return {'sktorrent': 1, 'prehrajto': 2, 'sledujteto': 3} (ids vary by DB)."""
    key = id(cur)
    cached = _PROVIDER_CACHE.get(key)
    if cached is not None:
        return cached
    cur.execute(
        "SELECT slug, id FROM video_providers WHERE slug = ANY(%s)",
        (list(PROVIDER_SLUGS),),
    )
    rows = cur.fetchall()
    # cur may be a DictCursor or a tuple cursor — handle both.
    mapping = {row[0] if not hasattr(row, "keys") else row["slug"]:
               row[1] if not hasattr(row, "keys") else row["id"]
               for row in rows}
    # Fail fast on missing seed rows: callers index with providers["sktorrent"]
    # etc. and a missing slug would surface as an opaque KeyError elsewhere.
    missing = [s for s in PROVIDER_SLUGS if s not in mapping]
    if missing:
        raise RuntimeError(
            f"video_providers missing seed rows for: {missing}. "
            f"Run migration 058 before dual-write.")
    _PROVIDER_CACHE[key] = mapping
    return mapping


def lang_class_to_audio_and_subs(lang_class: str | None = None,
                                 has_dub: bool = False,
                                 has_subtitles: bool = False
                                 ) -> tuple[str | None, str, list[str]]:
    """Map legacy language signals → (audio_lang, lang_class, subtitle_langs).

    Inputs (in priority order):
      1. `lang_class` — the enum from the legacy upload tables
         (CZ_DUB|CZ_NATIVE|CZ_SUB|SK_DUB|SK_SUB|EN|UNKNOWN). When set, it
         determines both the audio and the subtitles.
      2. `has_dub` / `has_subtitles` — sktorrent-style booleans, used ONLY
         when lang_class is None. Assumes CZ (the dominant audience).

    Returns a triple:
      audio_lang  — 2/3-char ISO code or None (must satisfy the CHECK
                    constraint `^[a-z]{2,3}$`).
      lang_class  — normalized enum value. The DB CHECK
                    `video_sources_lang_class_audio_consistency_check`
                    enforces audio_lang ↔ lang_class consistency, so this
                    function is the single place where those two fields
                    are derived together.
      sub_langs   — list of subtitle language codes to insert as
                    video_source_subtitles rows (often empty).

    Example mappings:
      CZ_DUB       → ('cs', 'CZ_DUB',    [])
      CZ_SUB       → (None, 'CZ_SUB',    ['cs'])
      SK_DUB       → ('sk', 'SK_DUB',    [])
      has_dub=T    → ('cs', 'CZ_DUB',    [])         (fallback)
      has_subs=T   → (None, 'CZ_SUB',    ['cs'])     (fallback)
      both T       → ('cs', 'CZ_DUB',    ['cs'])     (fallback)
      neither      → (None, 'UNKNOWN',   [])
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
        # Both flags — dub is primary audio signal, subtitles coexist.
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
                        audio_confidence: float | None = None,
                        audio_detected_by: str | None = None,
                        cdn: str | None = None,
                        is_primary: bool = False,
                        is_alive: bool = True,
                        last_seen=None,
                        metadata=None,
                        ) -> int:
    """UPSERT a `video_sources` row. Returns the row's id.

    Idempotence via `ON CONFLICT (provider_id, external_id)`. A re-run
    updates mutable fields (is_alive, cdn, lang_class, …) but keeps the
    row id stable, so `video_source_subtitles.source_id` stays valid
    across re-runs.

    IMPORTANT: exactly one of `film_id` / `episode_id` / `tv_episode_id`
    must be non-None. The DB enforces this via
    `video_sources_one_parent_check`; passing two or zero here is a bug
    in the caller, not something to work around silently.

    `metadata` may be a Python dict; it's wrapped in psycopg2.extras.Json
    so psycopg2 serializes it correctly for the JSONB column.
    """
    cur.execute(
        """
        INSERT INTO video_sources (
            provider_id, film_id, episode_id, tv_episode_id,
            external_id, title, duration_sec, resolution_hint,
            filesize_bytes, view_count, lang_class, audio_lang,
            audio_confidence, audio_detected_by, cdn,
            is_primary, is_alive, last_seen, metadata, updated_at
        ) VALUES (
            %(provider_id)s, %(film_id)s, %(episode_id)s, %(tv_episode_id)s,
            %(external_id)s, %(title)s, %(duration_sec)s, %(resolution_hint)s,
            %(filesize_bytes)s, %(view_count)s, %(lang_class)s, %(audio_lang)s,
            %(audio_confidence)s, %(audio_detected_by)s, %(cdn)s,
            %(is_primary)s, %(is_alive)s, %(last_seen)s, %(metadata)s, NOW()
        )
        -- Safety: never silently move a source row to a different parent.
        -- Same rule as backfill — legacy sktorrent has known duplicate
        -- video_ids across films, and any importer that encounters such a
        -- collision should preserve the existing parent binding rather than
        -- silently re-point the row (which would corrupt rollups on the old
        -- parent via the subtitles trigger cascade). The caller compares
        -- the returned parent IDs against the incoming ones and logs the
        -- mismatch instead.
        ON CONFLICT (provider_id, external_id) DO UPDATE SET
            title             = COALESCE(EXCLUDED.title, video_sources.title),
            duration_sec      = COALESCE(EXCLUDED.duration_sec, video_sources.duration_sec),
            resolution_hint   = COALESCE(EXCLUDED.resolution_hint, video_sources.resolution_hint),
            filesize_bytes    = COALESCE(EXCLUDED.filesize_bytes, video_sources.filesize_bytes),
            view_count        = COALESCE(EXCLUDED.view_count, video_sources.view_count),
            lang_class        = EXCLUDED.lang_class,
            audio_lang        = EXCLUDED.audio_lang,
            audio_confidence  = COALESCE(EXCLUDED.audio_confidence, video_sources.audio_confidence),
            audio_detected_by = COALESCE(EXCLUDED.audio_detected_by, video_sources.audio_detected_by),
            cdn               = EXCLUDED.cdn,
            is_primary        = EXCLUDED.is_primary,
            is_alive          = EXCLUDED.is_alive,
            last_seen         = COALESCE(EXCLUDED.last_seen, video_sources.last_seen),
            metadata          = COALESCE(EXCLUDED.metadata, video_sources.metadata),
            updated_at        = NOW()
        RETURNING id, film_id, episode_id, tv_episode_id
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
            audio_confidence=audio_confidence,
            audio_detected_by=audio_detected_by,
            cdn=cdn,
            is_primary=is_primary,
            is_alive=is_alive,
            last_seen=last_seen,
            metadata=psycopg2.extras.Json(metadata) if metadata is not None else None,
        ),
    )
    result = cur.fetchone()
    # Handle both DictCursor (row["id"]) and plain cursor (row[0]).
    if hasattr(result, "keys"):
        got = (result["id"], result["film_id"], result["episode_id"],
               result["tv_episode_id"])
    else:
        got = (result[0], result[1], result[2], result[3])
    got_id, got_film, got_ep, got_tv = got
    # Detect the "moved between parents" case — we preserved the original
    # parent binding; the caller's incoming parent was different. Log it so
    # the operator can clean the legacy duplicate. This is the same check
    # as in backfill-video-sources.py.
    if (got_film != film_id or got_ep != episode_id
            or got_tv != tv_episode_id):
        logging.getLogger(__name__).warning(
            "video_sources row id=%d kept on original parent "
            "(film=%s episode=%s tv_ep=%s); incoming (film=%s episode=%s "
            "tv_ep=%s) not re-pointed",
            got_id, got_film, got_ep, got_tv,
            film_id, episode_id, tv_episode_id,
        )
    return got_id


def upsert_subtitle(cur, source_id: int, lang: str,
                    *, format: str | None = None,
                    is_forced: bool = False,
                    label: str | None = None,
                    url: str | None = None,
                    is_default: bool = False) -> None:
    """Insert a subtitle row if absent (no-op on duplicate).

    URL + format are allowed to be NULL (sledujteto subtitles are
    resolved at play-time, so we persist the existence of the track
    without the URL). The uniqueness key includes COALESCE(format, '')
    so a (source, lang, forced) tuple can legitimately carry .srt + .ass
    side by side.
    """
    cur.execute(
        """
        INSERT INTO video_source_subtitles
            (source_id, lang, format, url, is_default, is_forced, label)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_id, lang, is_forced, COALESCE(format, ''))
        DO NOTHING
        """,
        (source_id, lang, format, url, is_default, is_forced, label),
    )


def dual_write_prehrajto_upload(cur, *, providers, film_id, upload_row,
                                primary_upload_id=None) -> int:
    """One-shot wrapper for prehrajto importers.

    `upload_row` is a dict (or DictRow) with keys:
      upload_id, url, title, duration_sec, view_count,
      lang_class, resolution_hint, is_alive (default True)

    Equivalent to the explicit `lang_class_to_audio_and_subs` + `upsert_video_source`
    + `upsert_subtitle` sequence, but collapsed into a single call so
    importer code stays short.
    """
    audio_lang, lang_class, sub_langs = lang_class_to_audio_and_subs(
        upload_row.get("lang_class"))
    metadata = {
        "url": upload_row.get("url"),
        "is_direct": upload_row.get("is_direct"),
    }
    source_id = upsert_video_source(
        cur,
        provider_id=providers["prehrajto"],
        external_id=upload_row["upload_id"],
        film_id=film_id,
        title=upload_row.get("title"),
        duration_sec=upload_row.get("duration_sec"),
        view_count=upload_row.get("view_count"),
        resolution_hint=upload_row.get("resolution_hint"),
        lang_class=lang_class,
        audio_lang=audio_lang,
        audio_detected_by="title_regex" if lang_class != "UNKNOWN" else None,
        is_primary=(primary_upload_id is not None
                    and upload_row["upload_id"] == primary_upload_id),
        is_alive=upload_row.get("is_alive", True),
        metadata=metadata,
    )
    for sub_lang in sub_langs:
        upsert_subtitle(cur, source_id, sub_lang)
    return source_id


def dual_write_sledujteto_upload(cur, *, providers, film_id, upload_row,
                                 primary_file_id=None) -> int:
    """One-shot wrapper for the sledujteto importer.

    `upload_row` keys:
      file_id, title, duration_sec, resolution_hint, filesize_bytes,
      lang_class, cdn, is_alive (default True)
    """
    audio_lang, lang_class, sub_langs = lang_class_to_audio_and_subs(
        upload_row.get("lang_class"))
    source_id = upsert_video_source(
        cur,
        provider_id=providers["sledujteto"],
        external_id=str(upload_row["file_id"]),
        film_id=film_id,
        title=upload_row.get("title"),
        duration_sec=upload_row.get("duration_sec"),
        resolution_hint=upload_row.get("resolution_hint"),
        filesize_bytes=upload_row.get("filesize_bytes"),
        lang_class=lang_class,
        audio_lang=audio_lang,
        audio_detected_by="title_regex" if lang_class != "UNKNOWN" else None,
        cdn=upload_row.get("cdn"),
        is_primary=(primary_file_id is not None
                    and upload_row["file_id"] == primary_file_id),
        is_alive=upload_row.get("is_alive", True),
    )
    for sub_lang in sub_langs:
        upsert_subtitle(cur, source_id, sub_lang)
    return source_id


def dual_write_sktorrent(cur, *, providers,
                         film_id=None, episode_id=None, tv_episode_id=None,
                         sktorrent_video_id: int,
                         sktorrent_cdn: int | None = None,
                         sktorrent_qualities: str | None = None,
                         has_dub: bool = False,
                         has_subtitles: bool = False) -> int:
    """One-shot wrapper for sktorrent importers (auto-import + friends).

    sktorrent is always 1:1 in legacy (one `sktorrent_video_id` per
    parent row), so `is_primary` is unconditionally True.
    """
    audio_lang, lang_class, sub_langs = lang_class_to_audio_and_subs(
        has_dub=has_dub, has_subtitles=has_subtitles)
    metadata = {"qualities": sktorrent_qualities} if sktorrent_qualities else None
    source_id = upsert_video_source(
        cur,
        provider_id=providers["sktorrent"],
        external_id=str(sktorrent_video_id),
        film_id=film_id,
        episode_id=episode_id,
        tv_episode_id=tv_episode_id,
        cdn=str(sktorrent_cdn) if sktorrent_cdn is not None else None,
        lang_class=lang_class,
        audio_lang=audio_lang,
        audio_detected_by="title_regex" if lang_class != "UNKNOWN" else None,
        is_primary=True,
        is_alive=True,
        metadata=metadata,
    )
    for sub_lang in sub_langs:
        upsert_subtitle(cur, source_id, sub_lang)
    return source_id
