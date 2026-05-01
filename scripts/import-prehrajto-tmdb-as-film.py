#!/usr/bin/env python3
"""Auto-import NEW_TMDB candidates from `prehrajto_unmatched_clusters`
into the `films` table — second stage of the prehraj.to → TMDB
resolution pipeline (#652).

Pipeline overview:
  Stage 1 (resolve-unmatched-via-llm.py): Gemma extracts a canonical
    title from the messy upload string, TMDB API resolves to a stable
    `tmdb_id`. Cluster gets either `resolved_film_id` (existing
    films.tmdb_id) or `resolved_tmdb_id` (TMDB hit but no films row).
  Stage 2 (this script): for every "TMDB-known but no films row" row,
    fetch full TMDB metadata and INSERT INTO films. After insertion
    the cluster's `resolved_film_id` is filled — closing the loop.

Why a separate script (not a hook in the resolver):
  - Keeps the resolver's responsibility narrow (extract → identify).
  - Auto-import can be skipped / rate-limited independently from the
    Gemma cycle — useful when TMDB is flaky but Gemma quota is fresh.
  - Different failure modes (slug collision, INSERT race) deserve
    their own retry policy.

Why not extend `scripts/auto_import/enricher.upsert_film()`:
  - upsert_film is tightly coupled to SK Torrent (`sktorrent_video_id`,
    `sktorrent_cdn`, `sktorrent_qualities`, `dual_write_sktorrent` for
    the video_sources row). For prehraj.to-derived films we have NONE
    of those signals — the prehraj.to importer (`import-prehrajto-
    uploads.py`) attaches uploads on a separate code path, keyed by
    cluster_key. Inserting via upsert_film would write a stub
    video_sources row tagged sktorrent that doesn't reflect reality.

Usage:
  python3 scripts/import-prehrajto-tmdb-as-film.py [--limit N] [--dry-run]

Environment:
  DATABASE_URL    Postgres DSN
  TMDB_API_KEY    TMDB v3 API key

Per-row commit; safe to interrupt mid-run.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import unicodedata
from typing import Optional

try:
    import psycopg2
    import requests
except ImportError as e:
    print(f"ERROR: missing dep ({e}). apt install python3-psycopg2 python3-requests",
          file=sys.stderr)
    sys.exit(2)

# Local genre map duplicated from enricher.TMDB_MOVIE_GENRE_MAP — keeps
# this script importable on hosts that have only `psycopg2` + `requests`
# installed (the full `scripts.auto_import` package would pull in
# Pillow / rclone via `cover_downloader`). The cost of duplication is
# 20 lines of integer mappings that change about once a year.
TMDB_MOVIE_GENRE_MAP: dict[int, Optional[str]] = {
    28: "akcni", 12: "dobrodruzny", 16: "animovany", 35: "komedie",
    80: "krimi", 99: "dokumentarni", 18: "drama", 10751: "rodinny",
    14: "fantasy", 36: "historicky", 27: "horor", 10402: "hudebni",
    9648: "mysteriozni", 10749: "romanticky", 878: "sci-fi",
    10770: None,  # TV Movie — skip
    53: "thriller", 10752: "valecny", 37: "western",
}

TMDB_URL = "https://api.themoviedb.org/3"
# TMDB allows 40 requests / 10 s = 0.25 s minimum between calls. Each
# cluster needs 2 TMDB calls (cs-CZ + en-US), so at 0.25 s we spend
# ~0.5 s of TMDB latency per cluster — well within the daily import
# budget. Earlier 0.05 s value was a stale comment vs. quota mismatch
# called out in code review.
TMDB_RATE_DELAY_S = 0.25

log = logging.getLogger("prehrajto-tmdb-import")


def _slugify(text: str) -> str:
    """ASCII-folded slug — mirror of enricher._slugify."""
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _slug_taken(cur, slug: str) -> bool:
    """A slug is unavailable if it conflicts with an existing film OR
    with a genre — `trg_films_slug_not_genre` rejects films.slug
    values that match `genres.slug`, so probing only `films` lets a
    title like "Drama" or "Horor" pass the SELECT and then crash at
    INSERT. Mirror tv_show_enricher's UNION probe.
    """
    cur.execute(
        "SELECT 1 FROM films WHERE slug = %s "
        "UNION ALL "
        "SELECT 1 FROM genres WHERE slug = %s LIMIT 1",
        (slug, slug),
    )
    return cur.fetchone() is not None


def _unique_slug(cur, base: str, year: Optional[int]) -> str:
    """Find a free slug — base, base-{year}, then base-2, base-3...
    Probes both `films.slug` AND `genres.slug` (see `_slug_taken`)."""
    if not base:
        base = "film"
    if not _slug_taken(cur, base):
        return base
    if year:
        candidate = f"{base}-{year}"
        if not _slug_taken(cur, candidate):
            return candidate
    counter = 2
    while True:
        candidate = f"{base}-{counter}"
        if not _slug_taken(cur, candidate):
            return candidate
        counter += 1


def fetch_tmdb_movie(session: requests.Session, api_key: str,
                     tmdb_id: int) -> Optional[dict]:
    """Fetch /movie/{id} for cs-CZ + en-US, merge into a single dict.
    Returns None on HTTP failure or malformed payload.
    """
    out = {"tmdb_id": tmdb_id}
    for lang_code, prefix in (("cs-CZ", "cs"), ("en-US", "en")):
        try:
            r = session.get(
                f"{TMDB_URL}/movie/{tmdb_id}",
                params={"api_key": api_key, "language": lang_code},
                timeout=15,
            )
        except requests.RequestException as e:
            log.warning("tmdb fetch tmdb=%d lang=%s: %s",
                        tmdb_id, lang_code, type(e).__name__)
            return None
        if r.status_code == 404:
            log.warning("tmdb tmdb=%d not found (404)", tmdb_id)
            return None
        if r.status_code != 200:
            log.warning("tmdb http=%d tmdb=%d lang=%s",
                        r.status_code, tmdb_id, lang_code)
            return None
        try:
            d = r.json()
        except ValueError:
            return None
        out[prefix] = d
        time.sleep(TMDB_RATE_DELAY_S)
    return out


def _build_film_row(merged: dict) -> Optional[dict]:
    """Project the cs+en TMDB payload onto the films-table row shape.
    Returns None if the payload is missing the required title (TMDB
    occasionally returns a stub for newly-created entries)."""
    cs = merged.get("cs") or {}
    en = merged.get("en") or {}
    tmdb_id = merged["tmdb_id"]
    title_cs = (cs.get("title") or "").strip() or None
    title_en = (en.get("title") or "").strip() or None
    original_title = (cs.get("original_title")
                      or en.get("original_title")
                      or "").strip() or None
    if not (title_cs or title_en or original_title):
        return None

    rd = (cs.get("release_date") or en.get("release_date") or "")
    year = int(rd[:4]) if len(rd) >= 4 and rd[:4].isdigit() else None

    runtime = cs.get("runtime") or en.get("runtime")
    overview_cs = (cs.get("overview") or "").strip() or None
    overview_en = (en.get("overview") or "").strip() or None
    # `films.description` is reserved for the Gemma-rewritten unique CS
    # text (see migration 20260522_051_consolidate_description.sql).
    # Persist NULL here — templates already render fine when missing —
    # and let a follow-up Gemma pass populate it with original text
    # written from the TMDB overview as a source. Storing the raw
    # TMDB overview directly would violate the "unique CS" invariant
    # the consolidation migration enforces project-wide.
    _ = (overview_cs, overview_en)  # documented intent; not persisted

    # vote_average=0 means "no votes yet" — store as NULL so the list
    # page doesn't render a bogus 0/10 rating.
    va_raw = cs.get("vote_average") or en.get("vote_average")
    imdb_rating = float(va_raw) if va_raw else None

    poster_path = cs.get("poster_path") or en.get("poster_path")
    imdb_id = cs.get("imdb_id") or en.get("imdb_id")

    genre_ids = [g["id"] for g in (cs.get("genres") or en.get("genres") or [])
                 if g.get("id")]

    return {
        "tmdb_id": tmdb_id,
        "imdb_id": imdb_id,
        "title": title_cs or title_en or original_title,
        "original_title": (original_title
                           if original_title and original_title != (title_cs or title_en)
                           else None),
        "year": year,
        "runtime_min": int(runtime) if runtime else None,
        "imdb_rating": imdb_rating,
        "tmdb_poster_path": poster_path,
        "genre_ids": genre_ids,
    }


def _insert_film(cur, row: dict) -> int:
    """INSERT a films row from the projected TMDB payload, return film_id.
    NB: no sktorrent_video_id / sktorrent_* / video_sources dual-write —
    those belong to the SK Torrent flow. The prehraj.to importer
    (`import-prehrajto-uploads.py`) attaches uploads on its own.
    """
    base_slug = _slugify(row["title"])
    slug = _unique_slug(cur, base_slug, row.get("year"))
    # `description` left out → defaults to NULL. See `_build_film_row`
    # for why we do not persist the raw TMDB overview.
    cur.execute(
        """INSERT INTO films
               (title, original_title, slug, year,
                imdb_id, tmdb_id, runtime_min,
                imdb_rating,
                tmdb_poster_path,
                added_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
           RETURNING id""",
        (
            row["title"], row.get("original_title"), slug,
            row.get("year"),
            row.get("imdb_id"), row["tmdb_id"], row.get("runtime_min"),
            row.get("imdb_rating"),
            row.get("tmdb_poster_path"),
        ),
    )
    return cur.fetchone()[0]


def _link_genres(cur, film_id: int, genre_ids: list[int]) -> int:
    """Link film to genres via film_genres. Returns count linked."""
    if not genre_ids:
        return 0
    cur.execute("SELECT slug, id FROM genres")
    slug_to_id = dict(cur.fetchall())
    linked = 0
    for tmdb_gid in genre_ids:
        slug = TMDB_MOVIE_GENRE_MAP.get(tmdb_gid)
        if not slug or slug not in slug_to_id:
            continue
        cur.execute(
            "INSERT INTO film_genres (film_id, genre_id) "
            "VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (film_id, slug_to_id[slug]),
        )
        linked += 1
    return linked


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=20,
                    help="Max NEW_TMDB candidates to import per run (default 20)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would be imported, do not commit")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    dsn = os.environ.get("DATABASE_URL", "").strip()
    tmdb_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not (dsn and tmdb_key):
        print("ERROR: DATABASE_URL and TMDB_API_KEY are both required",
              file=sys.stderr)
        return 2

    http = requests.Session()
    http.headers.update({"User-Agent": "ceskarepublika.wiki tmdb-film-import"})

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    # `resolved_at IS NULL` mirrors the UPDATE preconditions further
    # below — a cluster with `resolved_at` set but `resolved_film_id`
    # still NULL would otherwise be re-selected on every run while the
    # UPDATEs silently no-op. Manual edits / partial earlier writes
    # are the only way to land in that state, but better to fail
    # closed than spin on it.
    #
    # `FOR UPDATE SKIP LOCKED` claims rows so two concurrent runs
    # (manual + cron, or two manual triggers) can't import the same
    # cluster twice. Combined with the per-row commit at the end, the
    # claim is released as soon as one row is fully processed.
    cur.execute("""
        SELECT id, sample_title, resolved_tmdb_id, upload_count
          FROM prehrajto_unmatched_clusters
         WHERE resolved_tmdb_id IS NOT NULL
           AND resolved_film_id IS NULL
           AND resolved_at IS NULL
         ORDER BY upload_count DESC, id ASC
         LIMIT %s
         FOR UPDATE SKIP LOCKED
    """, (args.limit,))
    rows = cur.fetchall()
    print(f"Loaded {len(rows)} NEW_TMDB candidates "
          f"(resolved_tmdb_id IS NOT NULL AND resolved_film_id IS NULL "
          f"AND resolved_at IS NULL)",
          flush=True)

    counters = {"added": 0, "skipped_existing": 0,
                "skipped_no_tmdb": 0, "failed": 0}

    for i, (rid, sample_title, tmdb_id, upload_count) in enumerate(rows, 1):
        # Race protection: another instance of this script — or a fresh
        # SK Torrent import — may have inserted the film between our
        # SELECT and now. Re-check before fetching TMDB to skip the
        # round-trip cost. We probe `tmdb_id` first (fast indexed
        # lookup that catches the prehraj.to-pipeline race) and re-
        # check by `imdb_id` AFTER the TMDB fetch — `films.imdb_id`
        # is what carries the UNIQUE constraint (`idx_films_imdb_id_unique`),
        # so missing that branch is the actual UniqueViolation risk.
        cur.execute("SELECT id FROM films WHERE tmdb_id = %s", (tmdb_id,))
        existing = cur.fetchone()
        if existing:
            existing_film_id = existing[0]
            print(f"[{i:>3}] SKIP_EXISTING tmdb={tmdb_id} → film_id={existing_film_id}  "
                  f"(linking cluster only)  ← {sample_title[:60]}",
                  flush=True)
            counters["skipped_existing"] += 1
            if not args.dry_run:
                cur.execute("""
                    UPDATE prehrajto_unmatched_clusters
                       SET resolved_film_id = %s,
                           resolved_at = NOW(),
                           last_failure_reason = NULL
                     WHERE id = %s AND resolved_at IS NULL
                """, (existing_film_id, rid))
                conn.commit()
            continue

        merged = fetch_tmdb_movie(http, tmdb_key, tmdb_id)
        if not merged:
            print(f"[{i:>3}] FAIL_TMDB     tmdb={tmdb_id} (fetch failed)  "
                  f"← {sample_title[:60]}", flush=True)
            counters["skipped_no_tmdb"] += 1
            if not args.dry_run:
                cur.execute("""
                    UPDATE prehrajto_unmatched_clusters
                       SET last_attempt_at = NOW(),
                           attempt_count = attempt_count + 1,
                           last_failure_reason = 'tmdb_fetch_failed'
                     WHERE id = %s AND resolved_at IS NULL
                """, (rid,))
                conn.commit()
            continue

        row = _build_film_row(merged)
        if not row:
            print(f"[{i:>3}] FAIL_PAYLOAD  tmdb={tmdb_id} (no usable title)  "
                  f"← {sample_title[:60]}", flush=True)
            counters["failed"] += 1
            if not args.dry_run:
                cur.execute("""
                    UPDATE prehrajto_unmatched_clusters
                       SET last_attempt_at = NOW(),
                           attempt_count = attempt_count + 1,
                           last_failure_reason = 'tmdb_empty_payload'
                     WHERE id = %s AND resolved_at IS NULL
                """, (rid,))
                conn.commit()
            continue

        # Second-stage race check: `films.imdb_id` carries the actual
        # UNIQUE constraint (`idx_films_imdb_id_unique`). A film row
        # could exist with the same `imdb_id` but a different `tmdb_id`
        # (e.g. SK Torrent imported it via IMDB-first resolution while
        # this script's SELECT FOR UPDATE was still pending). INSERTing
        # would crash; reuse the existing film instead.
        if row.get("imdb_id"):
            cur.execute("SELECT id FROM films WHERE imdb_id = %s",
                        (row["imdb_id"],))
            existing_by_imdb = cur.fetchone()
            if existing_by_imdb:
                existing_film_id = existing_by_imdb[0]
                print(f"[{i:>3}] SKIP_EXISTING_IMDB imdb={row['imdb_id']} → "
                      f"film_id={existing_film_id}  ← {sample_title[:60]}",
                      flush=True)
                counters["skipped_existing"] += 1
                if not args.dry_run:
                    cur.execute("""
                        UPDATE prehrajto_unmatched_clusters
                           SET resolved_film_id = %s,
                               resolved_at = NOW(),
                               last_failure_reason = NULL
                         WHERE id = %s AND resolved_at IS NULL
                    """, (existing_film_id, rid))
                    conn.commit()
                continue

        if args.dry_run:
            print(f"[{i:>3}] DRY-RUN  tmdb={tmdb_id} '{row['title']}' {row.get('year')}  "
                  f"({upload_count} uploads) ← {sample_title[:50]}",
                  flush=True)
            counters["added"] += 1
            continue

        try:
            film_id = _insert_film(cur, row)
            linked = _link_genres(cur, film_id, row.get("genre_ids", []))
            cur.execute("""
                UPDATE prehrajto_unmatched_clusters
                   SET resolved_film_id = %s,
                       resolved_at = NOW(),
                       last_failure_reason = NULL
                 WHERE id = %s AND resolved_at IS NULL
            """, (film_id, rid))
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[{i:>3}] FAIL_INSERT   tmdb={tmdb_id} '{row.get('title')}' "
                  f"err={type(e).__name__}: {str(e)[:120]}",
                  file=sys.stderr, flush=True)
            counters["failed"] += 1
            try:
                cur.execute("""
                    UPDATE prehrajto_unmatched_clusters
                       SET last_attempt_at = NOW(),
                           attempt_count = attempt_count + 1,
                           last_failure_reason = 'film_insert_failed'
                     WHERE id = %s AND resolved_at IS NULL
                """, (rid,))
                conn.commit()
            except Exception:
                conn.rollback()
            continue

        print(f"[{i:>3}] ADDED         tmdb={tmdb_id} → film_id={film_id} "
              f"'{row['title']}' {row.get('year')} ({linked} genres)  "
              f"← {sample_title[:50]}",
              flush=True)
        counters["added"] += 1

    print()
    print("=== Summary ===")
    for k, v in counters.items():
        print(f"  {k:<20} {v}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
