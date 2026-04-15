"""Add or update series + episodes in the production DB.

Symmetric counterpart to enricher.upsert_film, but with one extra concern:
when a brand-new series shows up on SK Torrent with several new episodes in
the same scanner batch, we want exactly ONE INSERT into `series` (not one per
episode). The batch helper `process_series_batch` groups scanner items by
TMDB tv_id and runs a single series-creation per group.

Single-episode flow (`upsert_episode`):
  - episode exists with imdb/tmdb match in DB → if no SKT, UPDATE; else skip
  - episode doesn't exist → INSERT episode (assumes series row already exists)

Series creation (`ensure_series`):
  - looks up existing by imdb_id or tmdb_id; returns existing series_id if found
  - otherwise: download cover, Gemma generate, INSERT series + series_genres
"""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import psycopg2

from scripts.auto_import.cover_downloader import download_cover
from scripts.auto_import.gemma_writer import generate_unique_cs
from scripts.auto_import.tmdb_resolver import (
    EpisodeResolution,
    TvResolution,
    resolve_episode,
)

log = logging.getLogger(__name__)

# Same TMDB→our slug mapping as movies, plus TV-only ids.
TMDB_TV_GENRE_MAP: dict[int, str | None] = {
    10759: "akcni",         # Action & Adventure (TV)
    16:    "animovany",     # Animation
    35:    "komedie",       # Comedy
    80:    "krimi",         # Crime
    99:    "dokumentarni",  # Documentary
    18:    "drama",         # Drama
    10751: "rodinny",       # Family
    10762: "rodinny",       # Kids
    9648:  "mysteriozni",   # Mystery
    10763: "dokumentarni",  # News
    10764: None,            # Reality — no clean mapping
    10765: "sci-fi",        # Sci-Fi & Fantasy
    10766: "drama",         # Soap
    10767: None,            # Talk — skip
    10768: "valecny",       # War & Politics
    37:    "western",       # Western
}


def _slugify(text: str) -> str:
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text)
    s = s.encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _unique_series_slug(cur, base: str, year: int | None) -> str:
    if not base:
        base = "series"
    cur.execute("SELECT 1 FROM series WHERE slug = %s", (base,))
    if not cur.fetchone():
        return base
    if year:
        candidate = f"{base}-{year}"
        cur.execute("SELECT 1 FROM series WHERE slug = %s", (candidate,))
        if not cur.fetchone():
            return candidate
    counter = 2
    while True:
        candidate = f"{base}-{counter}"
        cur.execute("SELECT 1 FROM series WHERE slug = %s", (candidate,))
        if not cur.fetchone():
            return candidate
        counter += 1


def _genre_id_lookup(cur) -> dict[str, int]:
    cur.execute("SELECT slug, id FROM genres")
    return dict(cur.fetchall())


def ensure_series(
    conn: psycopg2.extensions.connection,
    tv: TvResolution,
    cover_dir: Path,
) -> tuple[bool, int | None]:
    """Find existing series or create a new one.

    Returns (was_created, series_id). On TMDB lookup failure, series_id None.
    """
    if not (tv.imdb_id or tv.tmdb_id):
        log.warning("ensure_series: missing both imdb_id and tmdb_id")
        return False, None

    cur = conn.cursor()
    if tv.imdb_id:
        cur.execute("SELECT id FROM series WHERE imdb_id = %s", (tv.imdb_id,))
        row = cur.fetchone()
        if row:
            return False, row[0]
    if tv.tmdb_id:
        cur.execute("SELECT id FROM series WHERE tmdb_id = %s", (tv.tmdb_id,))
        row = cur.fetchone()
        if row:
            return False, row[0]

    # New series — create row
    name_cs = tv.name_cs or tv.name_en or tv.original_name or "Seriál"
    name_en = tv.name_en
    base_slug = _slugify(name_cs)
    slug = _unique_series_slug(cur, base_slug, tv.first_air_year)

    cover_filename: str | None = None
    if tv.poster_path:
        result = download_cover(tv.poster_path, slug, cover_dir)
        if result is not None:
            cover_filename = slug

    sources = []
    if tv.overview_cs:
        sources.append(("TMDB CS", tv.overview_cs))
    if tv.overview_en:
        sources.append(("TMDB EN", tv.overview_en))
    generated = generate_unique_cs(name_cs, tv.first_air_year, sources, is_series=True)
    description = generated or tv.overview_cs or tv.overview_en

    cur.execute(
        """INSERT INTO series
           (title, original_title, slug, first_air_year, last_air_year,
            description, generated_description, imdb_id, tmdb_id,
            season_count, episode_count, cover_filename, added_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
           RETURNING id""",
        (
            name_cs, name_en if name_en != name_cs else None, slug,
            tv.first_air_year, tv.last_air_year,
            description, generated,
            tv.imdb_id, tv.tmdb_id,
            tv.season_count, tv.episode_count,
            cover_filename,
        ),
    )
    series_id = cur.fetchone()[0]

    # Series genres
    if tv.genre_ids:
        slug_to_id = _genre_id_lookup(cur)
        for tmdb_gid in tv.genre_ids:
            slug = TMDB_TV_GENRE_MAP.get(tmdb_gid)
            if not slug or slug not in slug_to_id:
                continue
            cur.execute(
                "INSERT INTO series_genres (series_id, genre_id) "
                "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (series_id, slug_to_id[slug]),
            )

    log.info("created series %d (imdb=%s, tmdb=%d, slug=%s)",
             series_id, tv.imdb_id, tv.tmdb_id, slug)
    return True, series_id


def upsert_episode(
    conn: psycopg2.extensions.connection,
    *,
    series_id: int,
    season: int,
    episode_num: int,
    sktorrent_video_id: int,
    sktorrent_cdn: int | None,
    sktorrent_qualities: list[str],
    ep_meta: EpisodeResolution | None = None,
) -> tuple[str, int | None]:
    """Decide updated_episode / added_episode / skipped for a single episode."""
    cur = conn.cursor()
    qualities_str = ",".join(sktorrent_qualities) if sktorrent_qualities else None

    # Existing episode?
    cur.execute(
        "SELECT id, sktorrent_video_id FROM episodes "
        "WHERE series_id = %s AND season = %s AND episode = %s LIMIT 1",
        (series_id, season, episode_num),
    )
    row = cur.fetchone()
    if row is not None:
        ep_id, existing_skt = row
        if existing_skt is not None:
            return "skipped", ep_id
        cur.execute(
            "UPDATE episodes SET sktorrent_video_id = %s, sktorrent_cdn = %s, "
            "sktorrent_qualities = %s WHERE id = %s",
            (sktorrent_video_id, sktorrent_cdn, qualities_str, ep_id),
        )
        log.info("updated episode %d S%dE%d (added SKT %d)",
                 ep_id, season, episode_num, sktorrent_video_id)
        return "updated_episode", ep_id

    # New episode — INSERT with whatever metadata we have
    name = ep_meta.name if ep_meta else None
    overview = ep_meta.overview if ep_meta else None
    air_date = ep_meta.air_date if ep_meta else None
    runtime = ep_meta.runtime_min if ep_meta else None

    cur.execute(
        """INSERT INTO episodes
           (series_id, season, episode, sktorrent_video_id, sktorrent_cdn,
            sktorrent_qualities, episode_name, overview, air_date, runtime)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING id""",
        (
            series_id, season, episode_num,
            sktorrent_video_id, sktorrent_cdn, qualities_str,
            name, overview, air_date, runtime,
        ),
    )
    ep_id = cur.fetchone()[0]
    log.info("added episode %d S%dE%d (series_id=%d)", ep_id, season, episode_num, series_id)
    return "added_episode", ep_id


def process_series_batch(
    conn: psycopg2.extensions.connection,
    *,
    tv: TvResolution,
    episodes_to_add: list[tuple[int, int, int, int | None, list[str]]],
    cover_dir: Path,
) -> list[tuple[str, int | None, int, int]]:
    """Single series + multiple new episodes — exactly ONE series creation.

    Args:
        episodes_to_add: list of (season, episode_num, sktorrent_video_id,
                                  sktorrent_cdn, sktorrent_qualities)

    Returns:
        list of (action, episode_id, season, episode_num) per processed episode.
        First action will be "added_series+added_episode" if series was newly
        created in this call, otherwise plain "added_episode" / "updated_episode".
    """
    was_created, series_id = ensure_series(conn, tv, cover_dir)
    if series_id is None:
        return [("failed", None, s, e) for s, e, _, _, _ in episodes_to_add]

    out: list[tuple[str, int | None, int, int]] = []
    for season, ep_num, skt_id, skt_cdn, skt_q in episodes_to_add:
        ep_meta = resolve_episode(tv.tmdb_id, season, ep_num)
        action, ep_id = upsert_episode(
            conn,
            series_id=series_id,
            season=season,
            episode_num=ep_num,
            sktorrent_video_id=skt_id,
            sktorrent_cdn=skt_cdn,
            sktorrent_qualities=skt_q,
            ep_meta=ep_meta,
        )
        # Tag the FIRST episode of a newly-created series so the dashboard can
        # show "added series + N episodes" cleanly.
        if was_created and not out and action == "added_episode":
            action = "added_series+added_episode"
            was_created = False  # only flag the first one
        out.append((action, ep_id, season, ep_num))
    return out
