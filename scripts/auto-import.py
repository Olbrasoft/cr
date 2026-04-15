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
  TMDB_API_KEY           — TMDB v3 bearer key (required)
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
# root (parent of scripts/) to sys.path.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import psycopg2
import psycopg2.extras
import requests

from scripts.auto_import.sktorrent_scanner import (
    scan_new_videos, ScannedVideo, ScannerError,
)
from scripts.auto_import.sktorrent_detail import fetch_detail, DetailFetchError
from scripts.auto_import.title_parser import parse_sktorrent_title, ParsedTitle
from scripts.auto_import.tmdb_resolver import resolve_movie, resolve_tv
from scripts.auto_import.enricher import upsert_film
from scripts.auto_import.series_enricher import process_series_batch

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


def _open_run(conn, trigger: str) -> tuple[int, int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT last_sktorrent_video_id FROM import_checkpoint WHERE id = 1"
    )
    row = cur.fetchone()
    checkpoint = row[0] if row else 0

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

    cur.execute(
        """INSERT INTO import_runs (trigger, checkpoint_before)
           VALUES (%s, %s) RETURNING id""",
        (trigger, checkpoint),
    )
    run_id = cur.fetchone()[0]
    conn.commit()
    return run_id, checkpoint


def _close_run(conn, run_id: int, status: str, checkpoint_after: int,
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
               updated_films = %s,
               updated_episodes = %s,
               failed_count = %s,
               skipped_count = %s,
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
            counters["updated_films"],
            counters["updated_episodes"],
            counters["failed_count"],
            counters["skipped_count"],
            error_message,
            run_id,
        ),
    )
    cur.execute(
        """UPDATE import_checkpoint
           SET last_sktorrent_video_id = %s, updated_at = now()
           WHERE id = 1""",
        (checkpoint_after,),
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
                 failure_step: str | None = None,
                 failure_message: str | None = None,
                 raw_log: dict | None = None) -> None:
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO import_items
           (run_id, sktorrent_video_id, sktorrent_url, sktorrent_title,
            detected_type, imdb_id, tmdb_id, season, episode, action,
            target_film_id, target_series_id, target_episode_id,
            failure_step, failure_message, raw_log)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s)""",
        (
            run_id, video.video_id, video.url, video.title,
            detected_type, imdb_id, tmdb_id,
            parsed.season if parsed else None,
            parsed.episode if parsed else None,
            action,
            target_film_id, target_series_id, target_episode_id,
            failure_step, failure_message,
            json.dumps(raw_log, ensure_ascii=False) if raw_log else None,
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


def _process_film(conn, *, run_id: int, video: ScannedVideo,
                  parsed: ParsedTitle, detail, movies_covers: Path,
                  counters: dict, tmdb_session: requests.Session) -> None:
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

    try:
        action, film_id = upsert_film(
            conn,
            sktorrent_video_id=video.video_id,
            sktorrent_cdn=detail.cdn if detail else None,
            sktorrent_qualities=detail.qualities if detail else [],
            movie=movie,
            cover_dir=movies_covers,
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

    _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                 detected_type="film",
                 imdb_id=movie.imdb_id, tmdb_id=movie.tmdb_id,
                 action=action, target_film_id=film_id)
    if action == "added_film":
        counters["added_films"] += 1
    elif action == "updated_film":
        counters["updated_films"] += 1
    elif action == "skipped":
        counters["skipped_count"] += 1
    conn.commit()


def _process_episode(conn, *, run_id: int, video: ScannedVideo,
                     parsed: ParsedTitle, detail, series_covers: Path,
                     counters: dict, tmdb_session: requests.Session) -> None:
    tv = resolve_tv(parsed, session=tmdb_session)
    if tv is None or not tv.imdb_id:
        _mark_skipped(conn, video.video_id, "tmdb_tv_resolve_failed")
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="episode", imdb_id=None, tmdb_id=None,
                     action="skipped",
                     failure_step="tmdb_tv_resolve",
                     failure_message="no TV match or missing imdb_id",
                     raw_log={"detail": asdict(detail) if detail else None})
        counters["skipped_count"] += 1
        return

    episodes = [(
        parsed.season,
        parsed.episode,
        video.video_id,
        detail.cdn if detail else None,
        detail.qualities if detail else [],
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

    for action, ep_id, _season, _ep_num in results:
        if action == "added_series+added_episode":
            counters["added_series"] += 1
            counters["added_episodes"] += 1
            store_action = "added_episode"
        elif action == "added_episode":
            counters["added_episodes"] += 1
            store_action = action
        elif action == "updated_episode":
            counters["updated_episodes"] += 1
            store_action = action
        elif action == "failed":
            counters["failed_count"] += 1
            store_action = action
        else:
            counters["skipped_count"] += 1
            store_action = action
        _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                     detected_type="episode",
                     imdb_id=tv.imdb_id, tmdb_id=tv.tmdb_id,
                     action=store_action,
                     target_episode_id=ep_id)
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
    run_id, checkpoint = _open_run(conn, trigger)
    log.info("run %d started (trigger=%s, checkpoint=%d, max_new=%d)",
             run_id, trigger, checkpoint, max_new)

    counters = {
        "scanned_pages": 0, "scanned_videos": 0,
        "added_films": 0, "added_series": 0, "added_episodes": 0,
        "updated_films": 0, "updated_episodes": 0,
        "failed_count": 0, "skipped_count": 0,
    }
    checkpoint_after = checkpoint
    status = "ok"
    error_message: str | None = None

    skt_session = requests.Session()
    tmdb_session = requests.Session()

    try:
        videos = scan_new_videos(
            checkpoint=checkpoint, max_new=max_new, session=skt_session,
        )
        counters["scanned_videos"] = len(videos)

        for video in videos:
            if _is_skipped(conn, video.video_id):
                log.info("video %d is blacklisted — skipping", video.video_id)
                counters["skipped_count"] += 1
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

            if parsed.is_episode and parsed.season and parsed.episode:
                _process_episode(conn, run_id=run_id, video=video,
                                 parsed=parsed, detail=detail,
                                 series_covers=series_covers,
                                 counters=counters, tmdb_session=tmdb_session)
            elif parsed.cz_title or parsed.en_title:
                _process_film(conn, run_id=run_id, video=video,
                              parsed=parsed, detail=detail,
                              movies_covers=movies_covers,
                              counters=counters, tmdb_session=tmdb_session)
            else:
                _insert_item(conn, run_id=run_id, video=video, parsed=parsed,
                             detected_type="unknown",
                             imdb_id=None, tmdb_id=None,
                             action="skipped",
                             failure_step="parse_title",
                             failure_message="no CZ/EN title extracted")
                counters["skipped_count"] += 1
                conn.commit()

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
            _close_run(conn, run_id, status, checkpoint_after, counters,
                       error_message)
        finally:
            skt_session.close()
            tmdb_session.close()
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
