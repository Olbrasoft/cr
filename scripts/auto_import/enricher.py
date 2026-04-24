"""Decide and execute the right DB action for each scanned video.

upsert_film orchestrates Path A (existing film, just attach SK Torrent) vs
Path B (brand-new film, full INSERT with cover + Gemma + genres). Returns the
action label and target film_id for logging into import_items.

Series + episode handling lives in series_enricher (sub-issue #420) — kept
separate because the batching logic for new series + multiple episodes is
non-trivial.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import psycopg2

from scripts.auto_import.cover_downloader import download_cover, download_sktorrent_thumb
from scripts.auto_import.gemma_writer import generate_unique_cs
from scripts.auto_import.tmdb_resolver import MovieResolution
from scripts.video_sources_helper import (
    dual_write_sktorrent,
    get_provider_ids,
)

log = logging.getLogger(__name__)

# TMDB genre id → our genres.slug. Mirror of GENRE_MAP in populate-films.py
# but using TMDB's numeric IDs (which is what /movie/{id} returns).
TMDB_MOVIE_GENRE_MAP: dict[int, str | None] = {
    28:    "akcni",         # Action
    12:    "dobrodruzny",   # Adventure
    16:    "animovany",     # Animation
    35:    "komedie",       # Comedy
    80:    "krimi",         # Crime
    99:    "dokumentarni",  # Documentary
    18:    "drama",         # Drama
    10751: "rodinny",       # Family
    14:    "fantasy",       # Fantasy
    36:    "historicky",    # History
    27:    "horor",         # Horror
    10402: "hudebni",       # Music
    9648:  "mysteriozni",   # Mystery
    10749: "romanticky",    # Romance
    878:   "sci-fi",        # Science Fiction
    10770: None,            # TV Movie — skip
    53:    "thriller",      # Thriller
    10752: "valecny",       # War
    37:    "western",       # Western
}


def _slugify(text: str) -> str:
    """Czech-aware slug generator (mirror of slug_from_title in populate-films.py)."""
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _unique_slug(cur, base: str, year: int | None) -> str:
    """Find a free slug — first try base, then base-{year}, then base-2, base-3..."""
    if not base:
        base = "film"
    cur.execute("SELECT 1 FROM films WHERE slug = %s", (base,))
    if not cur.fetchone():
        return base
    if year:
        candidate = f"{base}-{year}"
        cur.execute("SELECT 1 FROM films WHERE slug = %s", (candidate,))
        if not cur.fetchone():
            return candidate
    counter = 2
    while True:
        candidate = f"{base}-{counter}"
        cur.execute("SELECT 1 FROM films WHERE slug = %s", (candidate,))
        if not cur.fetchone():
            return candidate
        counter += 1


def _genre_id_lookup(cur) -> dict[str, int]:
    cur.execute("SELECT slug, id FROM genres")
    return dict(cur.fetchall())


def upsert_film(
    conn: psycopg2.extensions.connection,
    *,
    sktorrent_video_id: int,
    sktorrent_cdn: int | None,
    sktorrent_qualities: list[str],
    movie: MovieResolution,
    cover_dir: str,
    has_dub: bool = False,
    has_subtitles: bool = False,
    csfd_rating: int | None = None,
) -> tuple[str, int | None]:
    """Decide between updated_film / added_film / skipped and execute it.

    Args:
        conn: psycopg2 connection (caller manages commit/rollback)
        sktorrent_video_id: SK Torrent video id we're attaching
        sktorrent_cdn: 1-9 (online{N})
        sktorrent_qualities: ["720p", "480p", ...]
        movie: TMDB resolution (must have imdb_id)
        cover_dir: R2 prefix (no trailing slash), e.g. "films" or "series"

    Returns:
        (action, film_id) — action is one of "updated_film", "added_film", "skipped"
    """
    if not movie.imdb_id:
        log.warning("upsert_film: TMDB resolution missing imdb_id (tmdb=%d)", movie.tmdb_id)
        return "skipped", None

    cur = conn.cursor()
    qualities_str = ",".join(sktorrent_qualities) if sktorrent_qualities else None

    # Clamp csfd_rating to 0..100 — _detect_csfd accepts any 1–3 digit number
    # so a malformed title like "CSFD 999%" would otherwise land in DB and
    # corrupt sorting / card rendering.
    if csfd_rating is not None and not (0 <= csfd_rating <= 100):
        log.warning("csfd_rating=%d out of range, dropping", csfd_rating)
        csfd_rating = None

    # --- Path A: film already in DB? ---
    cur.execute(
        "SELECT id, sktorrent_video_id FROM films WHERE imdb_id = %s",
        (movie.imdb_id,),
    )
    row = cur.fetchone()
    if row is not None:
        film_id, existing_skt = row
        if existing_skt is not None:
            log.info("film %d (imdb=%s) already has SKT %d — skipping",
                     film_id, movie.imdb_id, existing_skt)
            return "skipped", film_id
        # Preserve existing has_dub/has_subtitles when updating — the DB value
        # reflects any previously linked source (e.g. Bombuj) and we only want
        # to OR-in the new signal from SK Torrent, not downgrade to False.
        # Backfill ratings via COALESCE — only fill if DB value is NULL, so
        # manually-curated numbers are never overwritten.
        cur.execute(
            "UPDATE films SET sktorrent_video_id = %s, sktorrent_cdn = %s, "
            "sktorrent_qualities = %s, "
            "has_dub = has_dub OR %s, "
            "has_subtitles = has_subtitles OR %s, "
            "imdb_rating = COALESCE(imdb_rating, %s), "
            "csfd_rating = COALESCE(csfd_rating, %s), "
            "tmdb_poster_path = COALESCE(tmdb_poster_path, %s), "
            "sktorrent_added_at = now() "
            "WHERE id = %s",
            (sktorrent_video_id, sktorrent_cdn, qualities_str,
             has_dub, has_subtitles,
             movie.vote_average, csfd_rating,
             movie.poster_path,
             film_id),
        )
        # Dual-write into the unified video_sources schema (#607 / #610).
        # has_dub/has_subtitles here reflect only THIS run's signal (regex over
        # the sktorrent title); the legacy UPDATE OR-ed them into the films
        # column so historical signals are preserved. The video_sources row
        # carries only the current detection, which is the right semantic for
        # a per-source record (a source either has CZ audio or doesn't).
        dual_write_sktorrent(
            cur,
            providers=get_provider_ids(cur),
            film_id=film_id,
            sktorrent_video_id=sktorrent_video_id,
            sktorrent_cdn=sktorrent_cdn,
            sktorrent_qualities=qualities_str,
            has_dub=has_dub,
            has_subtitles=has_subtitles,
        )
        log.info("upserted SKT into existing film %d (imdb=%s)", film_id, movie.imdb_id)
        return "updated_film", film_id

    # --- Path B: brand new film ---
    title_cs = movie.title_cs or movie.title_en or movie.original_title or "Film"
    title_en = movie.title_en
    base_slug = _slugify(title_cs)
    slug = _unique_slug(cur, base_slug, movie.year)

    # Gemma 4 unique CS text
    sources = []
    if movie.overview_cs:
        sources.append(("TMDB CS", movie.overview_cs))
    if movie.overview_en:
        sources.append(("TMDB EN", movie.overview_en))
    generated = generate_unique_cs(title_cs, movie.year, sources, is_series=False)
    description = generated or movie.overview_cs or movie.overview_en

    # imdb_rating is seeded from TMDB's vote_average (acceptable proxy —
    # same 0–10 scale, usually ≤0.5 apart for films with votes). csfd_rating
    # comes straight from the SK Torrent title when present ("= CSFD 82%").
    # Both NULL for new-release films and obscure CZ titles; the list page
    # already handles missing ratings gracefully.
    imdb_rating = movie.vote_average
    cur.execute(
        """INSERT INTO films
           (title, original_title, slug, year, description,
            imdb_id, tmdb_id, runtime_min,
            imdb_rating, csfd_rating,
            sktorrent_video_id, sktorrent_cdn, sktorrent_qualities,
            has_dub, has_subtitles,
            tmdb_poster_path,
            added_at, sktorrent_added_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
           RETURNING id""",
        (
            title_cs, title_en if title_en != title_cs else None, slug, movie.year,
            description,
            movie.imdb_id, movie.tmdb_id, movie.runtime_min,
            imdb_rating, csfd_rating,
            sktorrent_video_id, sktorrent_cdn, qualities_str,
            has_dub, has_subtitles,
            movie.poster_path,
        ),
    )
    film_id = cur.fetchone()[0]

    # Dual-write into the unified video_sources schema (#607 / #610). Same
    # transaction as the films INSERT above.
    dual_write_sktorrent(
        cur,
        providers=get_provider_ids(cur),
        film_id=film_id,
        sktorrent_video_id=sktorrent_video_id,
        sktorrent_cdn=sktorrent_cdn,
        sktorrent_qualities=qualities_str,
        has_dub=has_dub,
        has_subtitles=has_subtitles,
    )

    # Cover (best-effort, id-keyed layout). TMDB first, then SK Torrent
    # thumbnail as a low-res fallback for obscure CZ titles without a TMDB
    # poster (e.g. 53-min ČT dramas). Better a 200×300 thumbnail than a
    # placeholder. Failures are non-fatal — the handler serves a 1×1
    # placeholder WebP when the R2 key is missing.
    cover_result = "failed"
    if movie.poster_path:
        cover_result = download_cover(movie.poster_path, film_id, cover_dir)
    if cover_result == "failed":
        download_sktorrent_thumb(sktorrent_video_id, film_id, cover_dir)

    # Genre links
    if movie.genre_ids:
        slug_to_id = _genre_id_lookup(cur)
        for tmdb_gid in movie.genre_ids:
            slug = TMDB_MOVIE_GENRE_MAP.get(tmdb_gid)
            if not slug or slug not in slug_to_id:
                continue
            cur.execute(
                "INSERT INTO film_genres (film_id, genre_id) "
                "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (film_id, slug_to_id[slug]),
            )

    log.info("added film %d (imdb=%s, slug=%s)", film_id, movie.imdb_id, slug)
    return "added_film", film_id
