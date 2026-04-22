"""TV pořad enricher — find-or-create tv_shows + INSERT tv_episodes row.

Called from auto-import.py for every SK Torrent video tagged
`section='tv-porady'`. Shape matches `series_enricher.process_series_batch`
enough that the caller can thread counters through the same way.

Intentional scope cut:
- No ČSFD fallback here (#485 owns that). If TMDB doesn't have the show,
  the caller marks the item as failed/skipped — we don't create partial rows.
- No genre / cast / crew enrichment for now. Reality pořady rarely need it
  and we don't want a partial dataset polluting /tv-porady/ pages.

Idempotency caveats:
- tv_episodes insert carries `ON CONFLICT (tv_show_id, season, episode,
  sktorrent_video_id) DO NOTHING`, so duplicate runs against the same SK
  Torrent video are safe.
- tv_shows is NOT a true SQL UPSERT — tmdb_id has no UNIQUE constraint yet,
  so we do SELECT-then-INSERT. That's race-y if two workers import the
  same show concurrently (the second INSERT would duplicate). The auto-
  import pipeline runs single-threaded, so this is acceptable for now.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class UpsertResult:
    action: str                   # "added_tv_show+added_tv_episode" | "added_tv_episode" | "skipped" | "failed"
    tv_show_id: int | None
    tv_episode_id: int | None


def _slugify(text: str) -> str:
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _unique_slug(cur, base: str) -> str:
    """Find a tv_shows slug not used by films/series/genres/tv_shows."""
    if not base:
        base = "tv-porad"
    candidate = base
    n = 1
    while True:
        cur.execute(
            """SELECT 1 FROM tv_shows WHERE slug = %s
               UNION ALL SELECT 1 FROM films WHERE slug = %s
               UNION ALL SELECT 1 FROM series WHERE slug = %s
               UNION ALL SELECT 1 FROM genres WHERE slug = %s
               LIMIT 1""",
            (candidate, candidate, candidate, candidate),
        )
        if not cur.fetchone():
            return candidate
        n += 1
        candidate = f"{base}-{n}"


def process_tv_show_episode(
    conn,
    *,
    tv,                     # TvResolution (tmdb_resolver.resolve_tv()) — name_cs/name_en, first_air_year, overview_cs/overview_en
    season: int,
    episode: int,
    sktorrent_video_id: int,
    sktorrent_cdn: int | None,
    sktorrent_qualities: list[str],
    has_dub: bool,
    has_subtitles: bool,
) -> UpsertResult:
    """Find-or-create tv_show + insert tv_episode. Returns (action, ids)."""
    # Prefer Czech localisation — /tv-porady/ is a cs-CZ catalog, fall back
    # only when TMDB has no CZ entry for this show.
    title = (tv.name_cs or tv.name_en or tv.original_name or "").strip()
    description = tv.overview_cs or tv.overview_en

    qualities_str = ",".join(sktorrent_qualities) if sktorrent_qualities else None

    cur = conn.cursor()

    # Find-or-create tv_show by tmdb_id. NOT a true UPSERT — tmdb_id has no
    # UNIQUE constraint yet; see module docstring for the race-y implications.
    cur.execute(
        "SELECT id, slug FROM tv_shows WHERE tmdb_id = %s LIMIT 1",
        (tv.tmdb_id,),
    )
    row = cur.fetchone()
    created_show = False
    if row:
        tv_show_id, _slug = row
    else:
        base_slug = _slugify(title) or f"tv-porad-{tv.tmdb_id}"
        slug = _unique_slug(cur, base_slug)
        try:
            cur.execute(
                """INSERT INTO tv_shows (title, slug, tmdb_id, imdb_id,
                       first_air_year, description,
                       tmdb_poster_path, added_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                   RETURNING id""",
                (
                    title[:255],
                    slug,
                    tv.tmdb_id,
                    tv.imdb_id,
                    tv.first_air_year,
                    description,
                    tv.poster_path,
                ),
            )
            tv_show_id = cur.fetchone()[0]
            created_show = True
            log.info("created tv_show id=%d slug='%s' (tmdb=%d)",
                     tv_show_id, slug, tv.tmdb_id)
        except Exception:
            conn.rollback()
            log.exception("tv_show insert failed for tmdb=%d", tv.tmdb_id)
            return UpsertResult(action="failed", tv_show_id=None, tv_episode_id=None)

    # Episode.
    ep_slug = f"s{season:02d}e{episode:02d}"
    try:
        cur.execute(
            """INSERT INTO tv_episodes (
                   tv_show_id, season, episode, slug,
                   sktorrent_video_id, sktorrent_cdn, sktorrent_qualities,
                   has_dub, has_subtitles, sktorrent_added_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
               ON CONFLICT (tv_show_id, season, episode, sktorrent_video_id)
               DO NOTHING
               RETURNING id""",
            (
                tv_show_id, season, episode, ep_slug,
                sktorrent_video_id, sktorrent_cdn, qualities_str,
                has_dub, has_subtitles,
            ),
        )
        row = cur.fetchone()
    except Exception:
        conn.rollback()
        log.exception("tv_episode insert failed for tmdb=%d s%de%d",
                      tv.tmdb_id, season, episode)
        return UpsertResult(action="failed",
                            tv_show_id=tv_show_id, tv_episode_id=None)

    if row is None:
        # Episode row already existed (same sktorrent_video_id). No-op.
        return UpsertResult(
            action="skipped",
            tv_show_id=tv_show_id,
            tv_episode_id=None,
        )

    tv_episode_id = row[0]
    action = (
        "added_tv_show+added_tv_episode" if created_show else "added_tv_episode"
    )
    return UpsertResult(
        action=action,
        tv_show_id=tv_show_id,
        tv_episode_id=tv_episode_id,
    )
