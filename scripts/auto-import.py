#!/usr/bin/env python3
"""Auto-import entry-point — SK Torrent → TMDB → films/series DB.

One-shot orchestrator invoked by:
  - POST /admin/import/run (manual, via admin dashboard)
  - systemd cr-auto-import.timer (daily cron at 05:00 UTC)

Pipeline per run:
  1. Open DB, INSERT into import_runs (status='running').
  2. Read checkpoint from import_checkpoint.
  3. Call scanner.scan_new_videos() with checkpoint + --max-new.
  4. For each scanned video:
      a. fetch_detail() (SK Torrent detail page for qualities/cdn)
      b. parse_sktorrent_title() (→ ParsedTitle with CZ/EN/year/SxxExx)
      c. Route to film (upsert_film) or episode (process_series_batch).
      d. INSERT into import_items with action + target ids.
  5. Advance checkpoint to max sktorrent_video_id processed.
  6. UPDATE import_runs (status, counters, finished_at).

Env vars:
  DATABASE_URL           — Postgres DSN (required)
  TMDB_API_KEY           — TMDB v3 API key (query-string `api_key=`, not a bearer token)
  GEMINI_API_KEY         — single prod key (cron); GEMINI_API_KEY_1..4 for dev
  MOVIES_COVERS_DIR      — default data/movies/covers-webp
  SERIES_COVERS_DIR      — default data/series/covers-webp
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import traceback
from dataclasses import asdict
from pathlib import Path

# Allow `python3 scripts/auto-import.py` from any cwd. Modules in scripts/
# auto_import/*.py import via `scripts.auto_import.foo`, so we add the project
# root (parent of scripts/) to sys.path. We also add scripts/ itself so the
# bare-name import of `video_sources_helper` (used both here AND from inside
# `prehrajto_search.py`) resolves — it MUST happen before any auto_import
# package import or transitive `from video_sources_helper import …` will fail.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(_PROJECT_ROOT))

import psycopg2  # noqa: E402
import requests  # noqa: E402

from scripts.auto_import.sktorrent_scanner import (  # noqa: E402
    scan_new_videos, ScannedVideo, ScannerError,
)
from scripts.auto_import.sktorrent_detail import fetch_detail, DetailFetchError  # noqa: E402
from scripts.auto_import.title_parser import parse_sktorrent_title, ParsedTitle  # noqa: E402
from scripts.auto_import.tmdb_resolver import resolve_movie, resolve_tv  # noqa: E402
from scripts.auto_import.enricher import upsert_film  # noqa: E402
from scripts.auto_import.series_enricher import process_series_batch  # noqa: E402
from scripts.auto_import.tv_show_enricher import process_tv_show_episode  # noqa: E402
from scripts.auto_import.prehrajto_search import (  # noqa: E402
    BlockedError as PrehrajtoBlockedError,
    try_prehrajto_match,
)
from video_sources_helper import get_provider_ids  # noqa: E402

log = logging.getLogger("auto-import")


def _parse_pg_dsn(url: str) -> str:
    # psycopg2 accepts both URL and key=value forms directly — just return.
    return url


def _db_connect() -> psycopg2.extensions.connection:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        raise SystemExit("DATABASE_URL is required")
    conn = psycopg2.connect(_parse_pg_dsn(dsn))
    conn.autocommit = False
    return conn


def _open_run(conn, trigger: str) -> tuple[int, int, int]:
    """Return (run_id, generic_checkpoint, tv_porady_checkpoint)."""
    cur = conn.cursor()
    cur.execute(
        """SELECT last_sktorrent_video_id, last_sktorrent_video_id_tv_porady
           FROM import_checkpoint WHERE id = 1"""
    )
    row = cur.fetchone()
    checkpoint = row[0] if row else 0
    checkpoint_tv = row[1] if row else 0

    # Bootstrap on very first run: seed checkpoint from the maximum
    # sktorrent_video_id already present in films/episodes so we don't
    # re-crawl the entire listing. The migration seeds checkpoint=0, so
    # 0 means "never ran before".
    if checkpoint == 0:
        cur.execute(
            """SELECT GREATEST(
                   COALESCE((SELECT MAX(sktorrent_video_id) FROM films), 0),
                   COALESCE((SELECT MAX(sktorrent_video_id) FROM episodes), 0)
               )"""
        )
        bootstrap = cur.fetchone()[0] or 0
        if bootstrap > 0:
            log.info("bootstrap checkpoint from DB: %d", bootstrap)
            checkpoint = bootstrap
            cur.execute(
                """UPDATE import_checkpoint
                   SET last_sktorrent_video_id = %s, updated_at = now()
                   WHERE id = 1""",
                (checkpoint,),
            )

    # Bootstrap tv-porady checkpoint from tv_episodes the first time round.
    if checkpoint_tv == 0:
        cur.execute(
            "SELECT COALESCE((SELECT MAX(sktorrent_video_id) FROM tv_episodes), 0)"
        )
        bootstrap_tv = cur.fetchone()[0] or 0
        if bootstrap_tv > 0:
            log.info("bootstrap tv-porady checkpoint from DB: %d", bootstrap_tv)
            checkpoint_tv = bootstrap_tv
            cur.execute(
                """UPDATE import_checkpoint
                   SET last_sktorrent_video_id_tv_porady = %s, updated_at = now()
                   WHERE id = 1""",
                (checkpoint_tv,),
            )

    cur.execute(
        """INSERT INTO import_runs (trigger, checkpoint_before)
           VALUES (%s, %s) RETURNING id""",
        (trigger, checkpoint),
    )
    run_id = cur.fetchone()[0]
    conn.commit()
    return run_id, checkpoint, checkpoint_tv


def _close_run(conn, run_id: int, status: str,
               checkpoint_after: int, checkpoint_after_tv: int,
               counters: dict, error_message: str | None = None) -> None:
    cur = conn.cursor()
    cur.execute(
        """UPDATE import_runs SET
               finished_at = now(),
               status = %s,
               scanned_pages = %s,
               scanned_videos = %s,
               checkpoint_after = %s,
               added_films = %s,
               added_series = %s,
               added_episodes = %s,
               added_tv_shows = %s,
               added_tv_episodes = %s,
               updated_films = %s,
               updated_episodes = %s,
               failed_count = %s,
               skipped_count = %s,
               prehrajto_attempted = %s,
               prehrajto_matched = %s,
               prehrajto_rows_written = %s,
               error_message = %s
           WHERE id = %s""",
        (
            status,
            counters["scanned_pages"],
            counters["scanned_videos"],
            checkpoint_after,
            counters["added_films"],
            counters["added_series"],
            counters["added_episodes"],
            counters["added_tv_shows"],
            counters["added_tv_episodes"],
            counters["updated_films"],
            counters["updated_episodes"],
            counters["failed_count"],
            counters["skipped_count"],
            counters.get("prehrajto_attempted", 0),
            counters.get("prehrajto_matched", 0),
            counters.get("prehrajto_rows_written", 0),
            error_message,
            run_id,
        ),
    )
    cur.execute(
        """UPDATE import_checkpoint
           SET last_sktorrent_video_id = %s,
               last_sktorrent_video_id_tv_porady = %s,
               updated_at = now()
           WHERE id = 1""",
        (checkpoint_after, checkpoint_after_tv),
    )
    conn.commit()


def _insert_item(conn, *, run_id: int, video: ScannedVideo,
                 parsed: ParsedTitle | None,
                 detected_type: str,
                 imdb_id: str | None,
                 tmdb_id: int | None,
                 action: str,
                 target_film_id: int | None = None,
                 target_series_id: int | None = None,
                 target_episode_id: int | None = None,
                 target_tv_show_id: int | None = None,
                 target_tv_episode_id: int | None = None,
                 failure_step: str | None = None,
                 failure_message: str | None = None,
                 raw_log: dict | None = None,
                 prehrajto_status: str | None = None,
                 prehrajto_rows_written: int = 0) -> None:
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO import_items
           (run_id, sktorrent_video_id, sktorrent_url, sktorrent_title,
            detected_type, imdb_id, tmdb_id, season, episode, action,
            target_film_id, target_series_id, target_episode_id,
            target_tv_show_id, target_tv_episode_id,
            failure_step, failure_message, raw_log,
            prehrajto_status, prehrajto_rows_written)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            run_id, video.video_id, video.url, video.title,
            detected_type, imdb_id, tmdb_id,
            parsed.season if parsed else None,
            parsed.episode if parsed else None,
            action,
            target_film_id, target_series_id, target_episode_id,
            target_tv_show_id, target_tv_episode_id,
            failure_step, failure_message,
            json.dumps(raw_log, ensure_ascii=False) if raw_log else None,
            prehrajto_status, prehrajto_rows_written,
        ),
    )


def _mark_skipped(conn, sktorrent_video_id: int, reason: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO import_skipped_videos (sktorrent_video_id, reason)
           VALUES (%s, %s)
           ON CONFLICT (sktorrent_video_id) DO UPDATE SET
               reason = EXCLUDED.reason,
               last_tried_at = now(),
               try_count = import_skipped_videos.try_count + 1""",
        (sktorrent_video_id, reason),
    )


def _is_skipped(conn, sktorrent_video_id: int) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM import_skipped_videos WHERE sktorrent_video_id = %s",
        (sktorrent_video_id,),
    )
    return cur.fetchone() is not None


def _langs_to_flags(langs: list[str]) -> tuple[bool, bool]:
    """Map ParsedTitle.langs to (has_dub, has_subtitles) booleans.

    DUB_CZ/DUB_SK → dabing, SUBS_CZ/SUBS_SK → titulky. Bare "CZ" / "SK" tags
    on SK Torrent titles mean "Czech/Slovak dub present" (that's how they
    label dubbed releases), so they count as dub too. Pure "EN" does not
    toggle either flag.
    """
    has_dub = any(l in ("DUB_CZ", "DUB_SK", "CZ", "SK") for l in langs)
    has_subs = any(l in ("SUBS_CZ", "SUBS_SK") for l in langs)
    return has_dub, has_subs


def _try_prehrajto_for_film(conn, *, film_id: int, providers: dict,
                            prh_session: requests.Session,
                            counters: dict) -> tuple[str, int]:
    """Search prehraj.to for the just-added film and write hits.

    Returns (status, rows_written) for storage on the import_items row.
    Status is one of: matched, no_results, no_acceptable, error, blocked.
    BlockedError is re-raised so the run aborts (we share the proxy with
    the SK Torrent scanner — losing it kills both pipelines).
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT title, original_title, year, runtime_min "
        "FROM films WHERE id = %s",
        (film_id,),
    )
    row = cur.fetchone()
    if not row:
        return "error", 0
    title, original_title, year, runtime_min = row
    try:
        result = try_prehrajto_match(
            cur, providers, film_id,
            title=title, original_title=original_title,
            year=year, runtime_min=runtime_min, sess=prh_session,
        )
    except PrehrajtoBlockedError:
        conn.rollback()
        counters["prehrajto_attempted"] += 1
        raise
    except Exception as e:
        log.warning("prehrajto match failed for film_id=%d: %s", film_id, e)
        conn.rollback()
        counters["prehrajto_attempted"] += 1
        return "error", 0
    counters["prehrajto_attempted"] += 1
    attached = result["written"] + result["repointed"] + result.get("refreshed", 0)
    if attached:
        counters["prehrajto_matched"] += 1
        # Run-level row counter only counts genuinely new state (insert
        # or re-point), not pure refreshes — otherwise re-runs of the
        # daily importer would inflate it indefinitely.
        counters["prehrajto_rows_written"] += result["written"] + result["repointed"]
        log.info("  prehrajto match film_id=%d: written=%d repointed=%d "
                 "refreshed=%d (query=%r)", film_id, result["written"],
                 result["repointed"], result.get("refreshed", 0),
                 result["query"])
        return "matched", result["written"] + result["repointed"]
    if result["hits"] == 0:
        return "no_results", 0
    if result["accepted"] == 0:
        return "no_acceptable", 0
    # Hits accepted but nothing attached to our film → all collisions
    # with other films' existing rows where the title-evidence wasn't
    # strong enough to re-point. Surface as no_acceptable in the admin UI.
    return "no_acceptable", 0


def _process_film(conn, *, run_id: int, video: ScannedVideo,
                  parsed: ParsedTitle, detail, movies_covers: Path,
                  counters: dict, tmdb_session: requests.Session,
                  providers: dict, prh_session: requests.Session) -> None:
    movie = resolve_movie(parsed, session=tmdb_session)
    if movie is None or not movie.imdb_id:
        _mark_skipped(conn, video.video_id, "tmdb_resolve_failed")
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="film", imdb_id=None, tmdb_id=None,
                     action="skipped",
                     failure_step="tmdb_resolve",
                     failure_message="no match or missing imdb_id",
                     raw_log={"detail": asdict(detail) if detail else None})
        counters["skipped_count"] += 1
        return

    film_has_dub, film_has_subs = _langs_to_flags(parsed.langs)
    try:
        action, film_id = upsert_film(
            conn,
            sktorrent_video_id=video.video_id,
            sktorrent_cdn=detail.cdn if detail else None,
            sktorrent_qualities=detail.qualities if detail else [],
            movie=movie,
            cover_dir=movies_covers,
            has_dub=film_has_dub,
            has_subtitles=film_has_subs,
            csfd_rating=parsed.csfd_rating,
        )
    except Exception as e:
        conn.rollback()
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="film",
                     imdb_id=movie.imdb_id, tmdb_id=movie.tmdb_id,
                     action="failed",
                     failure_step="upsert_film",
                     failure_message=str(e),
                     raw_log={"trace": traceback.format_exc()[-2000:]})
        counters["failed_count"] += 1
        conn.commit()
        return

    # When upsert_film returns "skipped" it means the film already has this
    # exact SK Torrent video id linked — record that as the reason so the
    # admin dashboard doesn't show a reason-less row.
    if action == "skipped":
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="film",
                     imdb_id=movie.imdb_id, tmdb_id=movie.tmdb_id,
                     action="skipped", target_film_id=film_id,
                     failure_step="already_imported",
                     failure_message="film already linked to this SK Torrent video")
        counters["skipped_count"] += 1
        conn.commit()
        return

    # Persist the upsert outcome before we touch the network — if the
    # prehraj.to search fails, the import_items row already exists and
    # surfaces the SK Torrent video correctly in the admin dashboard.
    conn.commit()
    if action == "added_film":
        counters["added_films"] += 1
    elif action == "updated_film":
        counters["updated_films"] += 1

    # Now try prehraj.to. Two reasons it runs even on `updated_film`:
    # 1) updated_film means a film row matched an existing TMDB id but
    #    the prehraj.to importer might still have missed it.
    # 2) The check inside try_prehrajto_match is cheap; if the film
    #    already has a prehrajto source, write_hits will simply refresh
    #    the existing rows.
    prh_status: str | None = None
    prh_rows = 0
    try:
        prh_status, prh_rows = _try_prehrajto_for_film(
            conn, film_id=film_id, providers=providers,
            prh_session=prh_session, counters=counters,
        )
        conn.commit()
    except PrehrajtoBlockedError as e:
        # Record the import_items row anyway, then bubble up to abort run.
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="film",
                     imdb_id=movie.imdb_id, tmdb_id=movie.tmdb_id,
                     action=action, target_film_id=film_id,
                     prehrajto_status="blocked",
                     prehrajto_rows_written=0)
        conn.commit()
        raise

    _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                 detected_type="film",
                 imdb_id=movie.imdb_id, tmdb_id=movie.tmdb_id,
                 action=action, target_film_id=film_id,
                 prehrajto_status=prh_status,
                 prehrajto_rows_written=prh_rows)
    conn.commit()


def _process_episode(conn, *, run_id: int, video: ScannedVideo,
                     parsed: ParsedTitle, detail, series_covers: Path,
                     counters: dict, tmdb_session: requests.Session) -> None:
    # SK Torrent sometimes lists a TV pořad video (e.g. Královny Brna,
    # Asia Express) in BOTH /videos/ (generic) and /videos/tv-porady/.
    # If the tv-porady scan already created a tv_episode for this id,
    # silently skip the duplicate from the generic scan — don't touch
    # the blacklist and don't flag it as a failure in the admin log.
    cur = conn.cursor()
    cur.execute(
        "SELECT tv_show_id FROM tv_episodes WHERE sktorrent_video_id = %s LIMIT 1",
        (video.video_id,),
    )
    existing_tv_ep = cur.fetchone()
    if existing_tv_ep is not None:
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_episode", imdb_id=None, tmdb_id=None,
                     action="skipped",
                     failure_step="duplicate_tv_porad",
                     failure_message="already imported via /videos/tv-porady scan",
                     target_tv_show_id=existing_tv_ep[0])
        counters["skipped_count"] += 1
        conn.commit()
        return

    tv = resolve_tv(parsed, session=tmdb_session)
    if tv is None:
        _mark_skipped(conn, video.video_id, "tmdb_tv_resolve_failed")
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="episode", imdb_id=None, tmdb_id=None,
                     action="skipped",
                     failure_step="tmdb_tv_resolve",
                     failure_message="no TMDB match",
                     raw_log={"detail": asdict(detail) if detail else None})
        counters["skipped_count"] += 1
        return
    if not tv.imdb_id:
        # TMDB has a TV match but no IMDB link — hallmark of a CZ/SK-only
        # production (Královny Brna, Asia Express, Superlov). Route to the
        # tv_show flow instead of failing here; `series` table needs an
        # imdb_id but `tv_shows` doesn't.
        _process_tv_show(conn, run_id=run_id, video=video, parsed=parsed,
                         detail=detail, counters=counters,
                         tmdb_session=tmdb_session)
        return

    ep_has_dub, ep_has_subs = _langs_to_flags(parsed.langs)
    episodes = [(
        parsed.season,
        parsed.episode,
        video.video_id,
        detail.cdn if detail else None,
        detail.qualities if detail else [],
        ep_has_dub,
        ep_has_subs,
    )]
    try:
        results = process_series_batch(
            conn, tv=tv, episodes_to_add=episodes, cover_dir=series_covers,
        )
    except Exception as e:
        conn.rollback()
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="episode",
                     imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                     action="failed",
                     failure_step="process_series_batch",
                     failure_message=str(e),
                     raw_log={"trace": traceback.format_exc()[-2000:]})
        counters["failed_count"] += 1
        conn.commit()
        return

    series_row_written = False
    for action, ep_id, _season, _ep_num in results:
        if action == "added_series+added_episode":
            counters["added_series"] += 1
            counters["added_episodes"] += 1
            # Two rows: one for the series creation (so the dashboard's
            # "Added series" filter sees it) and one for the episode.
            if not series_row_written:
                series_cur = conn.cursor()
                series_cur.execute(
                    "SELECT id FROM series WHERE imdb_id = %s",
                    (tv.imdb_id,),
                )
                series_row = series_cur.fetchone()
                _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                             detected_type="series",
                             imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                             action="added_series",
                             target_series_id=series_row[0] if series_row else None)
                series_row_written = True
            store_action = "added_episode"
        elif action == "added_episode":
            counters["added_episodes"] += 1
            store_action = action
        elif action == "updated_episode":
            counters["updated_episodes"] += 1
            store_action = action
        elif action == "failed":
            counters["failed_count"] += 1
            _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                         detected_type="episode",
                         imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                         action=action,
                         target_episode_id=ep_id)
        else:
            counters["skipped_count"] += 1
            # Episode already had this sktorrent_video_id linked — record a
            # reason so the admin "Skipped" tab never shows a blank why.
            _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                         detected_type="episode",
                         imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                         action=action,
                         target_episode_id=ep_id,
                         failure_step="already_imported",
                         failure_message="episode already linked to this SK Torrent video")
    conn.commit()


def _process_tv_show(conn, *, run_id: int, video: ScannedVideo,
                     parsed: ParsedTitle, detail,
                     counters: dict, tmdb_session: requests.Session) -> None:
    """Route an SK Torrent video from /videos/tv-porady to tv_shows/tv_episodes.

    Scope: TMDB-matched only. ČSFD fallback is #485.
    """
    season = parsed.season if parsed.is_episode and parsed.season else 1
    episode = parsed.episode if parsed.is_episode and parsed.episode else None
    if episode is None:
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_show", imdb_id=None, tmdb_id=None,
                     action="skipped",
                     failure_step="parse_title",
                     failure_message="TV pořad video without parseable episode number")
        counters["skipped_count"] += 1
        conn.commit()
        return

    tv = resolve_tv(parsed, session=tmdb_session)
    if tv is None:
        # TMDB miss — without ČSFD fallback (#485) we skip for now.
        _mark_skipped(conn, video.video_id, "tmdb_tv_porady_resolve_failed")
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_show", imdb_id=None, tmdb_id=None,
                     action="skipped",
                     failure_step="tmdb_tv_resolve",
                     failure_message="no TMDB match (ČSFD fallback pending — #485)")
        counters["skipped_count"] += 1
        conn.commit()
        return

    ep_has_dub, ep_has_subs = _langs_to_flags(parsed.langs)
    try:
        result = process_tv_show_episode(
            conn,
            tv=tv,
            season=season,
            episode=episode,
            sktorrent_video_id=video.video_id,
            sktorrent_cdn=detail.cdn if detail else None,
            sktorrent_qualities=detail.qualities if detail else [],
            has_dub=ep_has_dub,
            has_subtitles=ep_has_subs,
        )
    except Exception as e:
        conn.rollback()
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_show",
                     imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                     action="failed",
                     failure_step="tv_show_enricher",
                     failure_message=str(e),
                     raw_log={"trace": traceback.format_exc()[-2000:]})
        counters["failed_count"] += 1
        conn.commit()
        return

    # tv_porady scanner has its own counters so /admin/import/ can show
    # the TV branch separately from the series scanner. Issue #566.
    if result.action == "added_tv_show+added_tv_episode":
        counters["added_tv_shows"] += 1
        counters["added_tv_episodes"] += 1
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_show",
                     imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                     action="added_tv_show",
                     target_tv_show_id=result.tv_show_id)
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_episode",
                     imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                     action="added_tv_episode",
                     target_tv_show_id=result.tv_show_id,
                     target_tv_episode_id=result.tv_episode_id)
    elif result.action == "added_tv_episode":
        counters["added_tv_episodes"] += 1
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_episode",
                     imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                     action="added_tv_episode",
                     target_tv_show_id=result.tv_show_id,
                     target_tv_episode_id=result.tv_episode_id)
    elif result.action == "skipped":
        counters["skipped_count"] += 1
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_episode",
                     imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                     action="skipped",
                     target_tv_show_id=result.tv_show_id,
                     failure_step="tv_episode_insert",
                     failure_message="already imported (sktorrent_video_id conflict)")
    else:  # "failed"
        counters["failed_count"] += 1
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="tv_show",
                     imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                     action="failed",
                     target_tv_show_id=result.tv_show_id,
                     failure_step="tv_show_enricher",
                     failure_message="insert aborted")
    conn.commit()


def run(trigger: str, max_new: int) -> int:
    movies_covers = Path(
        os.environ.get("MOVIES_COVERS_DIR", "data/movies/covers-webp")
    )
    series_covers = Path(
        os.environ.get("SERIES_COVERS_DIR", "data/series/covers-webp")
    )
    movies_covers.mkdir(parents=True, exist_ok=True)
    series_covers.mkdir(parents=True, exist_ok=True)

    conn = _db_connect()
    run_id, checkpoint, checkpoint_tv = _open_run(conn, trigger)
    log.info("run %d started (trigger=%s, cp=%d, cp_tv=%d, max_new=%d)",
             run_id, trigger, checkpoint, checkpoint_tv, max_new)

    counters = {
        "scanned_pages": 0, "scanned_videos": 0,
        "added_films": 0, "added_series": 0, "added_episodes": 0,
        "added_tv_shows": 0, "added_tv_episodes": 0,
        "updated_films": 0, "updated_episodes": 0,
        "failed_count": 0, "skipped_count": 0,
        "prehrajto_attempted": 0, "prehrajto_matched": 0,
        "prehrajto_rows_written": 0,
    }
    checkpoint_after = checkpoint
    checkpoint_after_tv = checkpoint_tv
    status = "ok"
    error_message: str | None = None

    skt_session = requests.Session()
    tmdb_session = requests.Session()
    prh_session = requests.Session()
    providers = get_provider_ids(conn.cursor())

    try:
        scan_generic = scan_new_videos(
            checkpoint=checkpoint, max_new=max_new, session=skt_session,
            section="generic",
        )
        scan_tv = scan_new_videos(
            checkpoint=checkpoint_tv, max_new=max_new, session=skt_session,
            section="tv-porady",
        )
        # Process oldest-first globally so batch grouping of adjacent
        # episode numbers stays coherent even when the two sections
        # overlap in SK Torrent video_id space.
        videos = sorted(
            [*scan_generic.videos, *scan_tv.videos],
            key=lambda v: v.video_id,
        )
        # max_new is a GLOBAL cap — each section was already capped when
        # crawling to stop runaway pagination, but without this second
        # slice a manual run would quietly process up to 2*max_new items.
        if max_new and len(videos) > max_new:
            videos = videos[:max_new]
        counters["scanned_pages"] = scan_generic.pages_scanned + scan_tv.pages_scanned
        counters["scanned_videos"] = len(videos)

        for video in videos:
            if _is_skipped(conn, video.video_id):
                log.info("video %d is blacklisted — skipping", video.video_id)
                _insert_item(conn, run_id=run_id, video=video, parsed=None,
                             detected_type="unknown",
                             imdb_id=None, tmdb_id=None,
                             action="skipped",
                             failure_step="blacklist",
                             failure_message="video present in import_skipped_videos")
                counters["skipped_count"] += 1
                conn.commit()
                continue

            try:
                detail = fetch_detail(video.video_id, session=skt_session)
            except DetailFetchError as e:
                _insert_item(conn, run_id=run_id, video=video, parsed=None,
                             detected_type="unknown",
                             imdb_id=None, tmdb_id=None,
                             action="failed",
                             failure_step="fetch_detail",
                             failure_message=str(e))
                counters["failed_count"] += 1
                conn.commit()
                continue

            if detail is None:
                _mark_skipped(conn, video.video_id, "detail_404")
                _insert_item(conn, run_id=run_id, video=video, parsed=None,
                             detected_type="unknown",
                             imdb_id=None, tmdb_id=None,
                             action="skipped",
                             failure_step="fetch_detail",
                             failure_message="HTTP 404 (deleted)")
                counters["skipped_count"] += 1
                conn.commit()
                continue

            parsed = parse_sktorrent_title(video.title)

            if video.section == "tv-porady":
                _process_tv_show(conn, run_id=run_id, video=video,
                                 parsed=parsed, detail=detail,
                                 counters=counters, tmdb_session=tmdb_session)
            elif parsed.is_episode and parsed.season and parsed.episode:
                _process_episode(conn, run_id=run_id, video=video,
                                 parsed=parsed, detail=detail,
                                 series_covers=series_covers,
                                 counters=counters, tmdb_session=tmdb_session)
            elif parsed.cz_title or parsed.en_title:
                try:
                    _process_film(conn, run_id=run_id, video=video,
                                  parsed=parsed, detail=detail,
                                  movies_covers=movies_covers,
                                  counters=counters,
                                  tmdb_session=tmdb_session,
                                  providers=providers,
                                  prh_session=prh_session)
                except PrehrajtoBlockedError as e:
                    log.error("prehraj.to blocked — aborting run after "
                              "current film: %s", e)
                    status = "partial"
                    error_message = f"prehrajto blocked: {e}"
                    break
            else:
                _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                             detected_type="unknown",
                             imdb_id=None, tmdb_id=None,
                             action="skipped",
                             failure_step="parse_title",
                             failure_message="no CZ/EN title extracted")
                counters["skipped_count"] += 1
                conn.commit()

            # Each section has its own checkpoint — bump only the relevant one
            # so a gap in one section can't hide new items in the other.
            if video.section == "tv-porady":
                if video.video_id > checkpoint_after_tv:
                    checkpoint_after_tv = video.video_id
            else:
                if video.video_id > checkpoint_after:
                    checkpoint_after = video.video_id
            time.sleep(1.5)  # polite throttle between videos

        if counters["failed_count"] > 0:
            status = "partial" if (counters["added_films"] or
                                   counters["added_episodes"] or
                                   counters["updated_films"] or
                                   counters["updated_episodes"]) else "error"

    except ScannerError as e:
        status = "error"
        error_message = f"scanner: {e}"
        log.exception("scanner failed")
    except KeyboardInterrupt:
        status = "error"
        error_message = "interrupted"
        raise
    except Exception as e:
        status = "error"
        error_message = f"{type(e).__name__}: {e}"
        log.exception("run %d crashed", run_id)
    finally:
        try:
            _close_run(conn, run_id, status, checkpoint_after,
                       checkpoint_after_tv, counters, error_message)
        finally:
            skt_session.close()
            tmdb_session.close()
            prh_session.close()
            conn.close()

    log.info("run %d finished: status=%s counters=%s", run_id, status, counters)
    return 0 if status in ("ok", "partial") else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--trigger", choices=["cron", "manual"], default="manual")
    ap.add_argument("--max-new", type=int, default=5,
                    help="Max new videos per run (0 = unlimited, for cron)")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    return run(trigger=args.trigger, max_new=args.max_new)


if __name__ == "__main__":
    sys.exit(main())
