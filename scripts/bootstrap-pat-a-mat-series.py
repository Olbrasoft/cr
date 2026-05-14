#!/usr/bin/env python3
"""One-shot bootstrap of the Pat a Mat series (TMDB 20475, IMDb tt0841929).

The auto-import has been routing CZ shorts under
"Pat a Mat / E32 / Sekacka / CZ" to the film pipeline because (a) the
title parser didn't recognise the `/ E\\d+ /` shorthand as an episode
marker (fixed in PR #725) and (b) Pat a Mat doesn't exist as a
`series` row to attach episodes to. This script fixes (b) so the next
auto-import that finds a fresh Pat a Mat upload can wire it in
automatically.

Steps:
  1. INSERT into `series` from TMDB `/tv/20475` (skipped if a
     `pat-a-mat` row already exists).
  2. INSERT all 141 episodes (across 9 seasons, ignoring S0 Specials)
     from `/tv/20475/season/{n}`. Each episode gets a Czech name +
     overview + slug derived from the name. Idempotency is keyed on
     the partial unique `(series_id, slug) WHERE slug IS NOT NULL`
     index — that one IS NOT NULL-gated, so re-running won't insert
     duplicates the way the natural-key UNIQUE (which includes the
     nullable `sktorrent_video_id`) would.
  3. For the 20 currently-known sktorrent video_ids (60923–60942 =
     SK Torrent flat numbering E20..E39) match by EPISODE NAME (after
     diacritic strip + case fold) against the TMDB episode list and
     stamp `episodes.sktorrent_video_id`. Matching by name is more
     reliable than the cumulative number — SK Torrent's E34 "Dveře"
     vs TMDB's S2E6 "Dveře" already disagree with the naive offset.
  4. INSERT a `video_sources` row per attachment — the series detail
     handler renders only episodes with a live `video_sources` row
     (see `cr-web/src/handlers/series.rs::EPISODE_HAS_SOURCE_PREDICATE`),
     so without this step the episodes would stay invisible despite
     having `sktorrent_video_id` stamped.

Idempotent: re-running is safe — the series INSERT is `ON CONFLICT
DO NOTHING` keyed on the unique slug, and episodes use the existing
`(series_id, season, episode, sktorrent_video_id)` UNIQUE.

Cover download + R2 push happens via the existing helpers so the
detail page renders properly from the start.

Usage:
  DATABASE_URL=postgres://...@127.0.0.1:25432/cr \\
  TMDB_API_KEY=... \\
      python3 scripts/bootstrap-pat-a-mat-series.py
"""

from __future__ import annotations

import logging
import os
import re
import sys
import unicodedata
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
    import requests
except ImportError as e:
    print(f"ERROR: missing dependency ({e.name}). "
          "pip install psycopg2-binary requests",
          file=sys.stderr)
    sys.exit(2)

_SCRIPTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPTS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.auto_import.cover_downloader import download_cover  # noqa: E402

log = logging.getLogger("bootstrap-pat-a-mat")

TMDB_ID = 20475
IMDB_ID = "tt0841929"
SLUG = "pat-a-mat"
SERIES_COVERS_DIR = Path("data/series/covers-webp")

# SK Torrent video ids known to be Pat a Mat episodes E20–E39
# (uploaded 2026-05-13/14 as a single batch).
SKTORRENT_VIDEOS = {
    60923: ("Snidane v trave", 20),
    60924: ("Pracka", 21),
    60925: ("Dest", 22),
    60926: ("Vylet", 23),
    60927: ("Vinari", 24),
    60928: ("Brusle", 25),
    60929: ("Klavir", 26),
    60930: ("Hrnciri", 27),
    60931: ("Porucha", 28),
    60932: ("Jablko", 29),
    60933: ("Klic", 30),
    60934: ("Nabytek", 31),
    60935: ("Sekacka", 32),
    60936: ("Generalni uklid", 33),
    60937: ("Dvere", 34),
    60938: ("Strecha", 35),
    60939: ("Susenky", 36),
    60940: ("Vrata", 37),
    60941: ("Cyklisti", 38),
    60942: ("Dlazdice", 39),
}


def _normalize(s: str) -> str:
    """Lowercase, strip diacritics, collapse whitespace — for name matching."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-zA-Z0-9 ]+", " ", s).lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _slugify(s: str) -> str:
    n = _normalize(s)
    return re.sub(r"\s+", "-", n)[:120]


def _tmdb_get(path: str, api_key: str) -> dict | None:
    r = requests.get(
        f"https://api.themoviedb.org/3{path}",
        params={"api_key": api_key, "language": "cs-CZ"},
        timeout=30,
    )
    if r.status_code != 200:
        log.error("TMDB %s returned %d: %s", path, r.status_code, r.text[:200])
        return None
    return r.json()


def _fetch_series(api_key: str) -> dict:
    s = _tmdb_get(f"/tv/{TMDB_ID}?append_to_response=external_ids", api_key)
    if not s:
        raise RuntimeError("series fetch failed")
    return s


def _fetch_all_episodes(api_key: str, season_count: int) -> list[dict]:
    """Return [{season, episode, name, overview, runtime, air_date}, ...].

    Skips S0 Specials — too many duplicates / odd one-offs and the
    detail page renders cleaner without them. We can backfill later
    if needed.
    """
    rows: list[dict] = []
    for season_num in range(1, season_count + 1):
        s = _tmdb_get(f"/tv/{TMDB_ID}/season/{season_num}", api_key)
        if not s:
            log.warning("season %d fetch failed — skipping", season_num)
            continue
        for e in s.get("episodes", []):
            rows.append({
                "season": season_num,
                "episode": int(e["episode_number"]),
                "name": (e.get("name") or "").strip(),
                "overview": (e.get("overview") or "").strip() or None,
                "runtime": e.get("runtime"),
                "air_date": e.get("air_date") or None,
            })
    return rows


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    dsn = os.environ.get("DATABASE_URL")
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not dsn or not api_key:
        log.error("DATABASE_URL + TMDB_API_KEY env required")
        return 2

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    # 1. Series row
    cur.execute("SELECT id FROM series WHERE slug = %s", (SLUG,))
    row = cur.fetchone()
    if row is not None:
        series_id = row[0]
        log.info("series %s already exists (id=%d) — re-running episodes step",
                 SLUG, series_id)
    else:
        log.info("fetching series from TMDB ...")
        s = _fetch_series(api_key)
        first_year = (s.get("first_air_date") or "0000")[:4]
        last_year = (s.get("last_air_date") or first_year)[:4]
        cur.execute(
            "INSERT INTO series (title, original_title, slug, first_air_year, "
            "last_air_year, description, imdb_id, tmdb_id, season_count, "
            "episode_count, added_at, tmdb_poster_path, tmdb_overview_en) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now(),%s,%s) RETURNING id",
            (
                s["name"][:255], s.get("original_name", "")[:255] or None,
                SLUG, int(first_year) if first_year.isdigit() else None,
                int(last_year) if last_year.isdigit() else None,
                s.get("overview"), IMDB_ID, TMDB_ID,
                s.get("number_of_seasons"), s.get("number_of_episodes"),
                s.get("poster_path"), None,
            ),
        )
        series_id = cur.fetchone()[0]
        log.info("created series id=%d", series_id)

        # Cover
        if s.get("poster_path"):
            log.info("downloading cover ...")
            outcome = download_cover(s["poster_path"], series_id,
                                     SERIES_COVERS_DIR)
            log.info("cover outcome: %s", outcome)

    # 2. Episodes
    season_count = 9
    log.info("fetching %d seasons from TMDB ...", season_count)
    eps = _fetch_all_episodes(api_key, season_count)
    log.info("fetched %d episodes from TMDB", len(eps))

    inserted = 0
    skipped = 0
    for e in eps:
        slug = f"s{e['season']}e{e['episode']:02d}-{_slugify(e['name'])}"
        # `episodes_unique` is on `(series_id, season, episode,
        # sktorrent_video_id)` — and Postgres treats NULLs as distinct,
        # so a naive `ON CONFLICT … DO NOTHING` with NULL sktorrent_id
        # would let a re-run insert duplicate stub rows. Key on the
        # partial UNIQUE index `idx_episodes_series_slug` instead,
        # which IS NOT NULL-gated and matches a single bootstrap row
        # per (series_id, slug).
        cur.execute(
            "INSERT INTO episodes (series_id, season, episode, episode_name, "
            "  overview, runtime, slug, sktorrent_video_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s, NULL) "
            "ON CONFLICT (series_id, slug) WHERE slug IS NOT NULL "
            "DO NOTHING RETURNING id",
            (series_id, e["season"], e["episode"], e["name"][:500],
             e["overview"], e["runtime"], slug),
        )
        if cur.fetchone() is not None:
            inserted += 1
        else:
            skipped += 1
    log.info("episodes: inserted=%d skipped=%d (already existed)",
             inserted, skipped)

    # 3. Attach SK Torrent video_ids by name. A handful of Pat a Mat
    #    episode names repeat across seasons ("Nábytek" appears in S2E2
    #    AND S8E4; "Sekačka" in S2E3 AND S6E2). SK Torrent's flat
    #    E20..E39 numbering corresponds to the *early* seasons, so when
    #    a name collides we want the EARLIEST (season, episode) — build
    #    the map in chronological order and ignore later duplicates.
    name_to_ep: dict[str, tuple[int, int]] = {}
    for e in eps:
        key = _normalize(e["name"])
        name_to_ep.setdefault(key, (e["season"], e["episode"]))

    attached = 0
    vsource_inserted = 0
    unmatched: list[tuple[int, str]] = []
    for vid, (raw_name, sk_e_num) in SKTORRENT_VIDEOS.items():
        loc = name_to_ep.get(_normalize(raw_name))
        if loc is None:
            unmatched.append((vid, raw_name))
            continue
        season, ep_num = loc
        # Attach to the existing NULL-stamped row from step 2. If a sibling
        # row with the same (season, episode) already carries a different
        # sktorrent_video_id, INSERT a new row — `episodes_unique` keys on
        # (series_id, season, episode, sktorrent_video_id) and with both
        # vids non-NULL the conflict semantics work as expected.
        cur.execute(
            "UPDATE episodes "
            "   SET sktorrent_video_id = %s, sktorrent_added_at = now() "
            " WHERE series_id = %s AND season = %s AND episode = %s "
            "   AND sktorrent_video_id IS NULL "
            "RETURNING id",
            (vid, series_id, season, ep_num),
        )
        row = cur.fetchone()
        if row is None:
            # NULL slot was taken — append a sibling row carrying this vid.
            cur.execute(
                "INSERT INTO episodes (series_id, season, episode, "
                "  episode_name, sktorrent_video_id, sktorrent_added_at) "
                "VALUES (%s,%s,%s,(SELECT episode_name FROM episodes WHERE "
                "  series_id=%s AND season=%s AND episode=%s LIMIT 1), "
                "  %s, now()) "
                "ON CONFLICT (series_id, season, episode, sktorrent_video_id) "
                "DO NOTHING RETURNING id",
                (series_id, season, ep_num,
                 series_id, season, ep_num, vid),
            )
            row = cur.fetchone()
        if row is None:
            # Both branches found the slot already in our desired state —
            # idempotent re-run, still proceed to ensure video_sources.
            cur.execute(
                "SELECT id FROM episodes WHERE series_id = %s AND season = %s "
                "  AND episode = %s AND sktorrent_video_id = %s LIMIT 1",
                (series_id, season, ep_num, vid),
            )
            row = cur.fetchone()
        episode_id = row[0]
        attached += 1

        # The series detail handler renders an episode only when a live
        # `video_sources` row points at it (see
        # cr-web/src/handlers/series.rs `EPISODE_HAS_SOURCE_PREDICATE`).
        # `episodes.sktorrent_video_id` alone is invisible. UNIQUE on
        # `(provider_id, external_id)` keeps the upsert idempotent.
        cur.execute(
            "INSERT INTO video_sources (provider_id, episode_id, external_id, "
            "  lang_class, is_primary, is_alive, last_seen) "
            "VALUES (1, %s, %s, 'UNKNOWN', true, true, now()) "
            "ON CONFLICT (provider_id, external_id) DO UPDATE "
            "  SET episode_id = EXCLUDED.episode_id, "
            "      is_alive = true, last_seen = now() "
            "RETURNING id",
            (episode_id, str(vid)),
        )
        if cur.fetchone() is not None:
            vsource_inserted += 1

        log.info("  vid=%d (SK E%d %s) → S%dE%d %s",
                 vid, sk_e_num, raw_name, season, ep_num,
                 next((e["name"] for e in eps if e["season"] == season
                       and e["episode"] == ep_num), "?"))

    if unmatched:
        log.warning("unmatched SK Torrent videos (%d):", len(unmatched))
        for vid, name in unmatched:
            log.warning("  vid=%d name=%r", vid, name)

    conn.commit()
    log.info("DONE — series_id=%d, episodes_inserted=%d, "
             "sktorrent_attached=%d, video_sources=%d",
             series_id, inserted, attached, vsource_inserted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
