#!/usr/bin/env python3
"""Import series episode sources from prehraj.to.

Two-phase pipeline (issue #684). Each phase is independently runnable
via `--mode`:

* `enrich`   (issue #685) — for every series already in our DB,
              search prehraj.to and attach matching episode video
              sources via `video_sources(provider_id=prehrajto)`.
* `discover` (issue #686) — stream-parse prehraj.to's sitemap dump
              cache, keep entries with `SxxExx` in the title, group
              by normalized base-title + year, TMDB-resolve each
              cluster, and create new `series` + `episodes` rows
              (with Gemma-generated CS descriptions via
              series_enricher.ensure_series) before attaching the
              cluster's uploads as `video_sources` rows.

Both modes write to the same unified `video_sources` schema; together
they cover the user-facing ask "for every series prehraj.to has, make
sure our catalogue has the series + every available source."

Defenses (lessons from commit 9d1d1e08d in the sktorrent path):
  * Alias filter normalizes BOTH the parsed prehraj.to title AND the
    DB series row's `title` / `original_title` — otherwise multi-
    episode shows quietly lose all but one match.
  * Per-series commits — never wrap the whole run in one transaction,
    or one bad show wipes hours of progress. Each per-match block is
    additionally wrapped in a SAVEPOINT so a single failure (TMDB
    schema mismatch, FK error, …) doesn't put the connection into
    aborted-transaction state.
  * `--match SUBSTRING` for targeted re-imports after a parser tweak.
  * `--dry-run` rolls back every per-series commit so logs can be
    inspected without touching the DB.
  * CZ-proxy mandatory in prod — prehraj.to (like SK Torrent) blocks
    datacenter ASNs.

Usage (Phase A — enrich existing series):
    DATABASE_URL=postgres://... TMDB_API_KEY=... \\
    CZ_PROXY_URL=https://chobotnice.aspfree.cz/Proxy.ashx \\
    CZ_PROXY_KEY=... \\
        python3 scripts/import-prehrajto-series.py \\
            --mode enrich --limit 10 --dry-run

Usage (Phase B — discover new series from sitemap):
    DATABASE_URL=... TMDB_API_KEY=... GEMINI_API_KEY=... \\
        python3 scripts/import-prehrajto-series.py \\
            --mode discover \\
            --sitemap-dir /var/cache/cr/prehrajto-sitemap \\
            --covers-dir data/movies/series-covers \\
            --limit 3 --dry-run
"""

from __future__ import annotations

import argparse
import html
import logging
import os
import re
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

import psycopg2
import psycopg2.extras
import requests

from scripts.auto_import.cz_proxy import proxy_config
from scripts.auto_import.prehrajto_series_search import (
    BlockedError,
    Hit,
    EpisodeMatch,
    build_query,
    classify_episode_hit,
    normalize_alias,
    search_episodes,
)
from scripts.auto_import.series_enricher import ensure_series, upsert_episode
from scripts.auto_import.title_parser import (
    ParsedTitle,
    parse_prehrajto_episode_title,
)
from scripts.auto_import.tmdb_resolver import resolve_episode, resolve_tv
from video_sources_helper import (
    get_provider_ids,
    lang_class_to_audio_and_subs,
    upsert_video_source,
    upsert_subtitle,
)

log = logging.getLogger("import-prehrajto-series")

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

# prehraj.to throttles aggressively — same budget as the films-import
# script. Per-search-query sleep is handled inside `search_episodes` via
# `prehrajto_search.SEARCH_SLEEP_S`; this constant gates inter-series
# pacing.
INTER_SERIES_SLEEP_S = 0.5


# ---------------------------------------------------------------------------
# Phase A — enrich existing series
# ---------------------------------------------------------------------------


@dataclass
class SeriesRow:
    id: int
    title: str
    original_title: str | None
    first_air_year: int | None
    tmdb_id: int | None


@dataclass
class EnrichStats:
    series_id: int
    series_label: str
    found: int = 0                  # hits returned by prehraj.to
    matched: int = 0                # hits that passed the alias filter
    added_source: int = 0           # video_sources row inserted, episode pre-existed
    added_episode: int = 0          # episodes row + video_sources row both inserted
    skipped_present: int = 0        # episode already had a prehrajto source
    failed_tmdb_episode: int = 0    # had no episodes row + TMDB lookup failed
    failed_other: int = 0
    status: str = "ok"


def load_target_series(cur, *, match: str | None,
                       limit: int, offset: int) -> list[SeriesRow]:
    """Pull candidate series from DB.

    Filters:
      * `match` — case-insensitive substring against title/original_title;
        useful for re-running against a single show.
      * `tmdb_id IS NOT NULL` — `resolve_episode()` needs a TMDB tv_id to
        enrich newly-discovered episodes. Series without TMDB are out of
        scope here (they'd need a separate manual identify pass first).

    Order: `id` ASC so a paginated run is deterministic and `--offset`
    works as a resume cursor.
    """
    where = ["tmdb_id IS NOT NULL"]
    params: list = []
    if match:
        where.append("(LOWER(title) LIKE %s OR LOWER(original_title) LIKE %s)")
        like = f"%{match.lower()}%"
        params.extend([like, like])
    sql = (
        "SELECT id, title, original_title, first_air_year, tmdb_id "
        "FROM series WHERE " + " AND ".join(where)
        + " ORDER BY id "
        + (f"OFFSET {int(offset)} " if offset else "")
        + (f"LIMIT {int(limit)} " if limit else "")
    )
    cur.execute(sql, params)
    return [
        SeriesRow(id=r[0], title=r[1], original_title=r[2],
                  first_air_year=r[3], tmdb_id=r[4])
        for r in cur.fetchall()
    ]


def get_existing_prehrajto_external_ids(cur, series_id: int,
                                        providers: dict) -> set[str]:
    """Return the set of prehrajto `external_id`s already attached to
    any episode of this series.

    Used to skip re-attaching an upload we've already written, while
    still allowing MULTIPLE uploads per episode — prehraj.to often has
    several uploads for the same episode (different qualities, dub
    languages, encodes), and the user wants completeness of sources,
    not one source per episode. The dedup unit is therefore the upload
    (external_id), not the (season, episode) tuple.

    The ON CONFLICT (provider_id, external_id) upsert in
    `upsert_video_source` would handle re-runs safely either way, but
    skipping the write avoids burning a transaction roundtrip per
    no-op.
    """
    cur.execute(
        """SELECT vs.external_id
             FROM episodes e
             JOIN video_sources vs
               ON vs.episode_id = e.id
              AND vs.provider_id = %s
            WHERE e.series_id = %s""",
        (providers["prehrajto"], series_id),
    )
    return {row[0] for row in cur.fetchall()}


def get_existing_episode_ids(cur, series_id: int) -> dict[tuple[int, int], int]:
    """Return {(season, episode): episode_id, ...} for the series. Used
    to decide whether a matched prehraj.to upload attaches to an
    existing row or needs a fresh INSERT via `upsert_episode`.
    """
    cur.execute(
        "SELECT id, season, episode FROM episodes WHERE series_id = %s",
        (series_id,),
    )
    return {(int(s), int(e)): int(eid) for eid, s, e in cur.fetchall()}


def attach_prehrajto_to_episode(cur, *, providers: dict, episode_id: int,
                                match: EpisodeMatch) -> int:
    """Upsert one `video_sources` row + subtitle rows for an episode.

    Returns the video_sources.id. Idempotent — re-running on the same
    (provider_id, external_id) updates mutable fields (is_alive,
    last_seen, lang_class) without touching the row id, so downstream
    `video_source_subtitles.source_id` references stay valid.
    """
    audio_lang, lang_class, sub_langs = lang_class_to_audio_and_subs(
        lang_class=match.lang_class,
    )
    url = "https://prehraj.to" + match.hit.href
    metadata = {"url": url, "phase": "prehrajto-series-enrich"}
    source_id = upsert_video_source(
        cur,
        provider_id=providers["prehrajto"],
        external_id=match.hit.external_id,
        episode_id=episode_id,
        title=match.hit.title,
        duration_sec=match.hit.duration_sec,
        resolution_hint=match.resolution_hint,
        filesize_bytes=match.hit.filesize_bytes,
        lang_class=lang_class,
        audio_lang=audio_lang,
        audio_detected_by="title_regex" if lang_class != "UNKNOWN" else None,
        is_primary=False,
        is_alive=True,
        metadata=metadata,
    )
    for sub_lang in sub_langs:
        upsert_subtitle(cur, source_id, sub_lang)
    return source_id


def enrich_one_series(
    conn: psycopg2.extensions.connection,
    series: SeriesRow,
    sess: requests.Session,
    tmdb_sess: requests.Session,
    providers: dict,
    *,
    dry_run: bool,
) -> EnrichStats:
    """Search prehraj.to for one series, attach matching episode sources.

    Each series runs in its own savepoint-style logical transaction:
    we open a cursor, do the work, and the caller `commit()`s or
    `rollback()`s based on `--dry-run`. A failure mid-series rolls back
    THAT series only (in live mode the outer loop commits after every
    series) so a single bad show doesn't wipe the rest of the run.
    """
    label = (series.original_title or series.title)[:60]
    stats = EnrichStats(series_id=series.id, series_label=label)

    aliases = {
        normalize_alias(a) for a in (series.title, series.original_title)
        if a and normalize_alias(a)
    }
    if not aliases:
        stats.status = "no_aliases"
        log.warning("  series id=%d has no normalizable aliases — skipping",
                    series.id)
        return stats

    query = build_query(series.title, series.original_title,
                         series.first_air_year)
    log.info("  search: %r (aliases=%s)", query, sorted(aliases))

    try:
        hits = search_episodes(sess, query)
    except BlockedError as e:
        stats.status = f"blocked: {e}"
        raise  # propagate to outer loop — bail on entire run
    except Exception as e:  # noqa: BLE001
        log.exception("  search failed for series id=%d: %s", series.id, e)
        stats.failed_other += 1
        stats.status = f"search_error: {type(e).__name__}"
        return stats

    stats.found = len(hits)
    log.info("  prehraj.to returned %d hit(s)", len(hits))

    matches: list[EpisodeMatch] = []
    for h in hits:
        m = classify_episode_hit(h, aliases)
        if m is None:
            continue
        matches.append(m)
    stats.matched = len(matches)
    if not matches:
        stats.status = "no_matches"
        return stats

    # Dedupe by (season, episode, external_id) so re-runs of the same
    # page don't double-write — the search loop already dedupes by
    # external_id, but defensive in case prehraj.to ever returns the
    # same upload under two different URLs.
    seen: set[tuple[int, int, str]] = set()
    deduped: list[EpisodeMatch] = []
    for m in matches:
        key = (m.season, m.episode, m.hit.external_id)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(m)
    log.info("  %d matches kept after dedup", len(deduped))

    cur = conn.cursor()
    existing_eps = get_existing_episode_ids(cur, series.id)
    existing_external_ids = get_existing_prehrajto_external_ids(
        cur, series.id, providers,
    )

    for m in deduped:
        if m.hit.external_id in existing_external_ids:
            stats.skipped_present += 1
            continue
        ep_key = (m.season, m.episode)
        episode_id = existing_eps.get(ep_key)

        # Per-match SAVEPOINT — without this, one failed INSERT (UNIQUE
        # violation, FK error, TMDB schema mismatch, …) puts the whole
        # connection into "aborted transaction" state and every subsequent
        # query in the same series fails with InFailedSqlTransaction. The
        # savepoint lets us roll back THIS match and keep going.
        cur.execute("SAVEPOINT prh_match")
        try:
            # Derive language flags from the same mapping used by
            # video_sources, so SK_DUB / SK_SUB / CZ_NATIVE are reflected on
            # the episodes row too (not just video_sources). Otherwise a
            # Slovak-dubbed upload would land in video_sources with
            # lang_class=SK_DUB but the parent episode row would still
            # report has_dub=false.
            audio_lang, _vs_lang_class, sub_langs = lang_class_to_audio_and_subs(
                lang_class=m.lang_class,
            )
            row_has_dub = audio_lang is not None  # cs / sk / en
            row_has_subs = bool(sub_langs)

            if episode_id is None:
                # Episode row doesn't exist yet — resolve via TMDB + insert.
                # Pass sktorrent_video_id=None so series_enricher skips
                # its sktorrent dual-write (verified: prior to the
                # signature change, this leaked a video_sources row with
                # provider=sktorrent and external_id='None').
                ep_meta = resolve_episode(series.tmdb_id, m.season, m.episode,
                                          session=tmdb_sess)
                if ep_meta is None:
                    log.warning("  TMDB resolve_episode failed: tv_id=%d S%dE%d",
                                series.tmdb_id, m.season, m.episode)
                    stats.failed_tmdb_episode += 1
                    cur.execute("ROLLBACK TO SAVEPOINT prh_match")
                    cur.execute("RELEASE SAVEPOINT prh_match")
                    continue
                _action, episode_id = upsert_episode(
                    conn,
                    series_id=series.id,
                    season=m.season,
                    episode_num=m.episode,
                    sktorrent_video_id=None,
                    sktorrent_cdn=None,
                    sktorrent_qualities=[],
                    ep_meta=ep_meta,
                    has_dub=row_has_dub,
                    has_subtitles=row_has_subs,
                )
                if episode_id is None:
                    stats.failed_other += 1
                    cur.execute("ROLLBACK TO SAVEPOINT prh_match")
                    cur.execute("RELEASE SAVEPOINT prh_match")
                    continue
                existing_eps[ep_key] = episode_id
                attach_prehrajto_to_episode(
                    cur, providers=providers,
                    episode_id=episode_id, match=m,
                )
                cur.execute("RELEASE SAVEPOINT prh_match")
                existing_external_ids.add(m.hit.external_id)
                stats.added_episode += 1
                log.info("  +episode+source S%02dE%02d ep_id=%d ext=%s",
                         m.season, m.episode, episode_id, m.hit.external_id)
            else:
                # Episode row exists. Update the parent's lang flags too
                # (OR-in, never downgrade) so the row reflects the new
                # source's audio/subtitle availability.
                cur.execute(
                    "UPDATE episodes SET "
                    "has_dub = has_dub OR %s, "
                    "has_subtitles = has_subtitles OR %s "
                    "WHERE id = %s",
                    (row_has_dub, row_has_subs, episode_id),
                )
                attach_prehrajto_to_episode(
                    cur, providers=providers,
                    episode_id=episode_id, match=m,
                )
                cur.execute("RELEASE SAVEPOINT prh_match")
                existing_external_ids.add(m.hit.external_id)
                stats.added_source += 1
                log.info("  +source S%02dE%02d ep_id=%d ext=%s",
                         m.season, m.episode, episode_id, m.hit.external_id)
        except Exception:  # noqa: BLE001
            log.exception("  match failed S%dE%d ext=%s — rolling back to savepoint",
                          m.season, m.episode, m.hit.external_id)
            cur.execute("ROLLBACK TO SAVEPOINT prh_match")
            cur.execute("RELEASE SAVEPOINT prh_match")
            stats.failed_other += 1
            continue

    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    return stats


# ---------------------------------------------------------------------------
# Phase B — discover new series via prehraj.to sitemap
# ---------------------------------------------------------------------------
#
# Reuses the sitemap-streaming primitives from
# `import-prehrajto-new-films.py` (#524) with INVERTED filtering: that
# script REJECTS entries containing SxxExx ("that's an episode, not the
# film we're looking for"); Phase B KEEPS them.
#
# Pipeline:
#   1. find_series_clusters() — stream-parse every `video-sitemap-*.xml`
#      in the cache dir, keep entries whose title has SxxExx, group by
#      (normalize_alias(base_title), year). Returns one SeriesCluster
#      per show.
#   2. discover_one_cluster() — TMDB-resolve the cluster, genre-filter,
#      find-or-create the `series` row via series_enricher.ensure_series
#      (which already handles Gemma + cover + genre links), then per
#      upload row run upsert_episode + attach_prehrajto_to_episode.
#   3. main() loops over clusters under --limit, with per-cluster
#      commit (same defense as Phase A).
#
# Genres skipped (out of scope for the IMDB-backed `series` table —
# they belong to `tv_shows`/`tv_episodes`):
#   10764 Reality, 10767 Talk, 10763 News, 10762 Kids
TV_SHOWS_GENRES: frozenset[int] = frozenset({10764, 10767, 10763, 10762})


# IMDB ratings index — loaded once per run from the cached TSV that
# `sync-imdb-ratings.py` keeps fresh via a daily cron. The TSV is
# ~10 MB compressed, ~1.5 M rows; loading it into a {imdb_id:
# (rating, votes)} dict takes ~3 s and ~150 MB RAM — cheap compared
# to the alternative of an OMDb / IMDb-API call per cluster. The
# index is a process-global cache so multiple `discover_one_cluster`
# invocations share it.
DEFAULT_IMDB_CACHE = Path("/opt/cr/data/imdb-cache/title.ratings.tsv.gz")
_IMDB_INDEX: dict[str, tuple[float, int]] | None = None


def _load_imdb_ratings(cache_path: Path = DEFAULT_IMDB_CACHE
                       ) -> dict[str, tuple[float, int]]:
    """Parse `title.ratings.tsv.gz` into a {tconst: (rating, votes)} map.

    Schema (tab-separated, header on row 0):
        tconst    averageRating    numVotes
        tt0000001 5.7              2104
        tt0000002 5.6              287
        ...

    Cached at module scope: subsequent calls return the same dict.
    Returns an empty dict (and logs a warning) when the TSV is
    missing — Phase B can still create series, they'll just lack
    IMDB ratings until the next `sync-imdb-ratings.py` cron tick.
    """
    global _IMDB_INDEX
    if _IMDB_INDEX is not None:
        return _IMDB_INDEX
    if not cache_path.exists():
        log.warning("IMDB ratings TSV not found at %s — new series will "
                    "have imdb_rating=NULL until the daily sync cron "
                    "fills them in", cache_path)
        _IMDB_INDEX = {}
        return _IMDB_INDEX
    import gzip
    index: dict[str, tuple[float, int]] = {}
    t0 = time.time()
    with gzip.open(cache_path, "rt", encoding="utf-8") as fh:
        next(fh, None)  # header row
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue
            tconst, avg, votes = parts[0], parts[1], parts[2]
            if not tconst.startswith("tt"):
                continue
            try:
                index[tconst] = (float(avg), int(votes))
            except ValueError:
                continue
    log.info("Loaded %d IMDB ratings from %s in %.1fs",
             len(index), cache_path, time.time() - t0)
    _IMDB_INDEX = index
    return index


def _stamp_ratings(cur, series_id: int, tv,
                   imdb_index: dict[str, tuple[float, int]]) -> None:
    """Fill `series.tmdb_rating` / `.imdb_rating` / `.imdb_votes` if NULL.

    Idempotent + non-clobbering — `COALESCE(col, %s)` keeps any value
    that's already there (the daily IMDB sync cron, manual fixes, an
    earlier discover run).

    Both `tmdb_rating_synced_at` and `imdb_rating_synced_at` are
    stamped to now() in the same UPDATE — without the IMDb timestamp
    a re-run of migration 069 would re-clear our freshly-stamped
    `imdb_rating` because that migration's WHERE filter is
    `imdb_rating_synced_at IS NULL` (the column is treated as
    "definitely-not-synced" until proven otherwise). Same freshness
    convention as `sync-imdb-ratings.py`.
    """
    tmdb_rating = tv.vote_average
    tmdb_vote_count = tv.vote_count
    imdb_rating: float | None = None
    imdb_votes: int | None = None
    if getattr(tv, "imdb_id", None):
        hit = imdb_index.get(tv.imdb_id)
        if hit is not None:
            imdb_rating, imdb_votes = hit
    if tmdb_rating is None and imdb_rating is None:
        return  # nothing to write
    cur.execute(
        """UPDATE series SET
              tmdb_rating           = COALESCE(tmdb_rating, %s),
              tmdb_vote_count       = COALESCE(tmdb_vote_count, %s),
              tmdb_rating_synced_at = CASE
                  WHEN tmdb_rating IS NULL AND %s IS NOT NULL THEN now()
                  ELSE tmdb_rating_synced_at
              END,
              imdb_rating           = COALESCE(imdb_rating, %s),
              imdb_votes            = COALESCE(imdb_votes, %s),
              imdb_rating_synced_at = CASE
                  WHEN imdb_rating IS NULL AND %s IS NOT NULL THEN now()
                  ELSE imdb_rating_synced_at
              END
           WHERE id = %s""",
        (tmdb_rating, tmdb_vote_count, tmdb_rating,
         imdb_rating, imdb_votes, imdb_rating,
         series_id),
    )

# Sitemap XML regexes (vendored from import-prehrajto-new-films.py so
# Phase B doesn't have to import a sibling script just for these).
_SITEMAP_LOC_RE = re.compile(r"<loc>([^<]+)</loc>")
_SITEMAP_TITLE_RE = re.compile(r"<video:title>([^<]*)</video:title>")
_SITEMAP_DUR_RE = re.compile(r"<video:duration>(\d+)</video:duration>")
_SITEMAP_VIEWS_RE = re.compile(r"<video:view_count>(\d+)</video:view_count>")
_SITEMAP_LIVE_RE = re.compile(r"<video:live>(yes|no)</video:live>")
_SITEMAP_URL_BLOCK_RE = re.compile(r"<url>(.*?)</url>", re.DOTALL)
_UPLOAD_ID_RE = re.compile(r"/([a-f0-9]{13,16})(?:[/?#]|$)")
# Looser than `title_parser._EPISODE_RE` — prehrajto uploaders sometimes
# write `S01.E01` or `S01_E01` with separators between the SxxExx pair.
# Used only to PRE-FILTER sitemap entries (we still re-parse with
# `parse_prehrajto_episode_title` for the actual (season, episode)
# extraction, which knows the separator-tolerant patterns).
_SITEMAP_EPISODE_RE = re.compile(
    r"\bS\d{1,2}[\s._\-]?E\d{1,3}\b|\b\d{1,2}x\d{1,2}\b", re.IGNORECASE,
)


def _sitemap_unescape(s: str) -> str:
    """prehraj.to occasionally double-escapes `&` in `<video:title>`. Same
    quirk the films-import handles — one unescape pass clears the normal
    case, two clears the double-encoded one. `html.unescape` is a no-op
    on already-clean text so the second pass is safe."""
    return html.unescape(html.unescape(s))


def parse_sitemap_for_episodes(path: Path,
                               chunk_size: int = 1 << 20) -> Iterator[dict]:
    """Stream-parse a `video-sitemap-*.xml` file, yield dicts for
    entries whose title matches the loose SxxExx filter.

    Streaming (read-N-bytes loop + `<url>...</url>` block extraction)
    rather than `etree.iterparse` because the files are 5–15 MB each
    and a generator-yield API keeps peak memory bounded — important
    when we're going to chew through 495 files.

    Yields `{url, title, duration, views}` per entry — the same fields
    used by `import-prehrajto-new-films.parse_sitemap` except for
    `live`, which we filter on internally (live streams are dropped
    before yielding) and don't propagate to callers.
    """
    carry = ""
    with open(path, encoding="utf-8", errors="replace") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            data = carry + chunk
            last_close = data.rfind("</url>")
            if last_close < 0:
                carry = data
                continue
            complete = data[: last_close + len("</url>")]
            carry = data[last_close + len("</url>") :]
            for m in _SITEMAP_URL_BLOCK_RE.finditer(complete):
                block = m.group(1)
                title_m = _SITEMAP_TITLE_RE.search(block)
                if not title_m:
                    continue
                title_raw = _sitemap_unescape(title_m.group(1))
                # Pre-filter: must contain an SxxExx-style marker.
                # Saves the full `parse_prehrajto_episode_title` call
                # on the ~95% of sitemap entries that are films / shorts.
                if not _SITEMAP_EPISODE_RE.search(title_raw):
                    continue
                loc_m = _SITEMAP_LOC_RE.search(block)
                if not loc_m:
                    continue
                dur_m = _SITEMAP_DUR_RE.search(block)
                views_m = _SITEMAP_VIEWS_RE.search(block)
                live_m = _SITEMAP_LIVE_RE.search(block)
                # Skip live streams — they're never on-demand episodes
                # and prehraj.to occasionally tags webcams with show-
                # looking titles.
                if live_m and live_m.group(1) == "yes":
                    continue
                yield {
                    "url": _sitemap_unescape(loc_m.group(1)),
                    "title": title_raw,
                    "duration": int(dur_m.group(1)) if dur_m else 0,
                    "views": int(views_m.group(1)) if views_m else 0,
                }


def _extract_upload_id(url: str) -> str | None:
    m = _UPLOAD_ID_RE.search(url or "")
    return m.group(1) if m else None


@dataclass
class ClusterRow:
    """One sitemap entry that parsed as a series episode upload."""
    upload_id: str
    url: str
    title: str
    duration_sec: int | None
    season: int
    episode: int
    views: int


@dataclass
class SeriesCluster:
    """All sitemap uploads that look like episodes of the same show.

    `norm_key` is the alias-normalized base title used for grouping;
    `base_title` is the readable form (best uploader rendition we saw
    of the show name) used for TMDB search.
    """
    base_title: str
    norm_key: str
    year: int | None
    rows: list[ClusterRow] = field(default_factory=list)

    @property
    def upload_count(self) -> int:
        return len(self.rows)

    @property
    def episode_keys(self) -> set[tuple[int, int]]:
        return {(r.season, r.episode) for r in self.rows}


def find_series_clusters(
    sitemap_dir: Path,
    *,
    min_distinct_episodes: int = 2,
    match: str | None = None,
) -> list[SeriesCluster]:
    """Stream-parse every sitemap file under `sitemap_dir`, return a
    de-duplicated list of `SeriesCluster` sorted by upload count desc.

    Why `min_distinct_episodes`: a real series cluster has uploads
    across multiple (season, episode) tuples — even a one-season show
    will surface S01E01, S01E02, … Filtering by distinct episodes
    (rather than raw upload count) defends against the worst false-
    positive case: a single film whose title happens to contain
    `S01E01` re-uploaded by 50 different uploaders. That'd have
    50 uploads but 1 distinct episode, and we don't want it
    masquerading as a series.

    `match` is an optional case-insensitive substring filter applied
    to the cluster's accumulating base_title DURING the scan, so a
    targeted run (e.g. `--match Voyager --limit 1`) doesn't have to
    fully materialize every other cluster's ClusterRows just to
    discard them right before processing. With the default million-
    entry prod sitemap this halves memory and saves ~30% wall clock.

    Grouping key: `(normalize_alias(parsed.cz_title), parsed.year)`.
    The year is part of the key so reboots of the same name (e.g. "BSG
    2003" vs "BSG 1978") cluster separately. When the uploader leaves
    the year out of the title, year=None and all those uploads merge
    into one cluster — which is fine for TMDB resolution (the resolver
    will pick whatever's most popular).
    """
    needle = match.lower() if match else None
    log.info("Sitemap scan: %s%s", sitemap_dir,
             f" (match={needle!r})" if needle else "")
    files = sorted(sitemap_dir.glob("video-sitemap-*.xml"),
                   key=lambda p: int(re.search(r"(\d+)", p.stem).group(1)))
    log.info("  %d sitemap files to scan", len(files))
    if not files:
        log.error("No video-sitemap-*.xml under %s", sitemap_dir)
        return []

    clusters: dict[tuple[str, int | None], SeriesCluster] = {}
    # Per-cluster dedup of upload_ids — prehrajto's sitemap occasionally
    # lists the same upload across multiple sitemap files when index
    # boundaries shift between dumps.
    seen_upload_ids: dict[tuple[str, int | None], set[str]] = {}
    raw_entries = 0
    matched_entries = 0
    parser_skipped = 0

    t0 = time.time()
    for fi, path in enumerate(files, 1):
        for entry in parse_sitemap_for_episodes(path):
            raw_entries += 1
            parsed = parse_prehrajto_episode_title(entry["title"])
            if not parsed.is_episode:
                parser_skipped += 1
                continue
            clean = (parsed.cz_title or "").strip()
            if len(clean) < 2:
                parser_skipped += 1
                continue
            norm = normalize_alias(clean)
            if not norm:
                parser_skipped += 1
                continue
            upload_id = _extract_upload_id(entry["url"])
            if not upload_id:
                parser_skipped += 1
                continue
            key = (norm, parsed.year)
            cluster = clusters.get(key)
            if cluster is None:
                # Push --match into the scan to avoid materializing
                # ClusterRows for clusters we'll throw away anyway.
                # Substring is matched against the cleaned base_title
                # AND the alias-normalized key — same as Phase A's
                # match semantics, but applied at parse time.
                if needle and needle not in clean.lower() \
                        and needle not in norm:
                    continue
                cluster = SeriesCluster(
                    base_title=clean, norm_key=norm, year=parsed.year,
                )
                clusters[key] = cluster
                seen_upload_ids[key] = set()
            if upload_id in seen_upload_ids[key]:
                continue
            seen_upload_ids[key].add(upload_id)
            # Prefer the longest readable cluster name we've seen so the
            # TMDB search query has the best signal. Uploaders are
            # inconsistent — one upload may say "Yellowstone", the next
            # "Yellowstone (2018)", a third "Yellowstone S5". The
            # truncate-at-marker parser will give us the bare show name
            # for each; the LONGEST one is usually the most informative.
            if len(clean) > len(cluster.base_title):
                cluster.base_title = clean
            cluster.rows.append(ClusterRow(
                upload_id=upload_id, url=entry["url"], title=entry["title"],
                duration_sec=entry["duration"] or None,
                season=parsed.season, episode=parsed.episode,
                views=entry["views"],
            ))
            matched_entries += 1
        if fi % 100 == 0:
            log.info("  scanned %d/%d files, %d raw entries with SxxExx, "
                     "%d kept after parser, %d clusters so far (%.0fs)",
                     fi, len(files), raw_entries, matched_entries,
                     len(clusters), time.time() - t0)
    log.info("Sitemap scan done in %.0fs: %d entries with SxxExx pre-filter, "
             "%d kept, %d parser-skipped → %d raw clusters",
             time.time() - t0, raw_entries, matched_entries, parser_skipped,
             len(clusters))

    out = [c for c in clusters.values()
           if len(c.episode_keys) >= min_distinct_episodes]
    out.sort(key=lambda c: -c.upload_count)
    log.info("  %d clusters with >= %d distinct episodes (dropped %d "
             "clusters that only had one (season, episode) tuple — "
             "mostly bogus SxxExx placeholders on films)",
             len(out), min_distinct_episodes, len(clusters) - len(out))
    return out


@dataclass
class DiscoverStats:
    cluster_key: str
    cluster_year: int | None
    upload_count: int
    distinct_episodes: int
    tmdb_id: int | None = None
    tmdb_name: str | None = None
    series_id: int | None = None
    series_was_created: bool = False
    added_episode: int = 0
    added_source: int = 0
    skipped_present: int = 0
    failed_tmdb_episode: int = 0
    failed_other: int = 0
    status: str = "ok"


def _series_exists_by_tmdb(cur, tmdb_id: int) -> tuple[int, str, str | None] | None:
    """Return (id, title, original_title) if a series row has this tmdb_id."""
    cur.execute(
        "SELECT id, title, original_title FROM series "
        "WHERE tmdb_id = %s LIMIT 1",
        (tmdb_id,),
    )
    return cur.fetchone()


def discover_one_cluster(
    conn: psycopg2.extensions.connection,
    cluster: SeriesCluster,
    tmdb_sess: requests.Session,
    providers: dict,
    cover_dir: str,
    *,
    dry_run: bool,
) -> DiscoverStats:
    """TMDB-resolve a cluster, find-or-create the series row, attach uploads.

    Per-cluster transaction: caller commits or rolls back based on
    `dry_run`. Internal per-row work is wrapped in SAVEPOINTs so one
    bad upload doesn't abort the rest of the cluster.
    """
    stats = DiscoverStats(
        cluster_key=cluster.base_title[:60],
        cluster_year=cluster.year,
        upload_count=cluster.upload_count,
        distinct_episodes=len(cluster.episode_keys),
    )

    # 1. TMDB resolve. resolve_tv() takes a ParsedTitle; we synthesize
    # one from cluster metadata. We pass the readable `base_title` as
    # cz_title because we don't know the uploader's language — the
    # resolver searches BOTH cs-CZ and en-US TMDB indexes, so either
    # form usually hits.
    parsed = ParsedTitle(
        cz_title=cluster.base_title,
        en_title=None,
        year=cluster.year,
        season=None,
        episode=None,
        is_episode=False,
        raw=cluster.base_title,
    )
    try:
        tv = resolve_tv(parsed, session=tmdb_sess)
    except Exception as e:  # noqa: BLE001
        log.exception("  TMDB resolve_tv raised for cluster %r: %s",
                      cluster.base_title, e)
        stats.status = f"tmdb_error: {type(e).__name__}"
        return stats
    if tv is None:
        stats.status = "no_tmdb_match"
        log.info("  no TMDB hit for %r (year=%s)", cluster.base_title,
                 cluster.year)
        return stats
    stats.tmdb_id = tv.tmdb_id
    stats.tmdb_name = tv.name_cs or tv.name_en or tv.original_name

    # 2. Genre filter — Reality / Talk / News / Kids belong to tv_shows
    # not series. Skip those clusters here; if the user wants them, a
    # separate tv_shows importer is the right home.
    blocked = TV_SHOWS_GENRES & set(tv.genre_ids or ())
    if blocked:
        stats.status = f"blocked_genre: {sorted(blocked)}"
        log.info("  blocked by tv_shows genre %s (%s)",
                 sorted(blocked), tv.name_cs or tv.name_en)
        return stats

    # 3. Find or create series row. ensure_series handles both branches
    # (lookup by imdb_id/tmdb_id; new-series path runs Gemma + cover
    # download + genre links). For an existing series this is a no-op
    # SELECT — perfect for the discover-mode fall-through case where
    # the cluster's show is already in our DB.
    if dry_run:
        # ensure_series WRITES — INSERT into series + series_genres +
        # cover download. In dry-run we only want to surface what WOULD
        # happen, so we check existence without creating.
        existing = _series_exists_by_tmdb(conn.cursor(), tv.tmdb_id)
        if existing:
            stats.series_id, _, _ = existing
            stats.series_was_created = False
            stats.status = "ok_existing"
        else:
            stats.series_id = None
            stats.series_was_created = True
            stats.status = "ok_would_create"
        # Dry-run cuts off here — without an actual series_id we can't
        # safely insert episodes / sources either.
        conn.rollback()
        return stats

    try:
        was_created, series_id = ensure_series(conn, tv, cover_dir)
    except Exception as e:  # noqa: BLE001
        log.exception("  ensure_series raised for tmdb=%d: %s",
                      tv.tmdb_id, e)
        conn.rollback()
        stats.status = f"ensure_series_error: {type(e).__name__}"
        stats.failed_other += 1
        return stats
    if series_id is None:
        conn.rollback()
        stats.status = "ensure_series_returned_none"
        stats.failed_other += 1
        return stats
    stats.series_id = series_id
    stats.series_was_created = was_created
    log.info("  series id=%d (%s, tmdb=%d) %s",
             series_id, stats.tmdb_name, tv.tmdb_id,
             "CREATED" if was_created else "existed")

    # Stamp TMDB + IMDB ratings on the series row. Runs for BOTH the
    # newly-created and the existed branches because legacy series
    # rows often have NULL ratings the daily IMDB sync cron hasn't
    # yet filled in. `_stamp_ratings` uses COALESCE so an existing
    # non-NULL value is never clobbered.
    try:
        _stamp_ratings(conn.cursor(), series_id, tv, _load_imdb_ratings())
    except Exception:  # noqa: BLE001
        log.exception("    rating stamp failed for series id=%d "
                      "(continuing — sources still get attached)",
                      series_id)
    if was_created:
        # ensure_series writes cover bytes to the local cover_dir but
        # NEVER touches R2. The Rust handler at
        # `/serialy-online/{slug}.webp` reads from R2 directly via
        # `series/{id}/cover.webp` — so without this upload the new
        # series page would serve the 1×1 placeholder. We push it
        # right after series creation so the cover lands at the same
        # time as the series row commits in step 5.
        _push_cover_to_r2(series_id, cover_dir)

    # 4. Per-upload attach: ensure episode row, attach video_sources.
    cur = conn.cursor()
    cur.execute(
        """SELECT vs.external_id
             FROM video_sources vs
             JOIN episodes e ON vs.episode_id = e.id
            WHERE e.series_id = %s
              AND vs.provider_id = %s""",
        (series_id, providers["prehrajto"]),
    )
    existing_external_ids: set[str] = {r[0] for r in cur.fetchall()}
    cur.execute(
        "SELECT id, season, episode FROM episodes WHERE series_id = %s",
        (series_id,),
    )
    existing_eps: dict[tuple[int, int], int] = {
        (int(s), int(e)): int(eid) for eid, s, e in cur.fetchall()
    }
    # Cache TMDB resolve_episode failures so we don't retry the same
    # (season, episode) for every duplicate upload. prehrajto clusters
    # routinely contain dozens of uploads per episode, plus a long
    # tail of uploads with bogus markers like "S1E16" (a default the
    # uploader UI fills when they can't be bothered to enter the real
    # number). Without this cache, a 60k-upload cluster with 200 bogus
    # markers hits TMDB 60k × 200 = 12M times — many hours of API
    # roundtrips when the answer is always "no such episode."
    tmdb_dead_ends: set[tuple[int, int]] = set()

    for r in cluster.rows:
        if r.upload_id in existing_external_ids:
            stats.skipped_present += 1
            continue
        ep_key = (r.season, r.episode)
        if ep_key in tmdb_dead_ends:
            # Already attempted this episode → known dead-end. Count
            # toward fail-tmdb so the operator sees the cluster has a
            # parser-noise tail, but skip the TMDB call entirely.
            stats.failed_tmdb_episode += 1
            continue
        cur.execute("SAVEPOINT prh_row")
        try:
            audio_lang, vs_lang_class, sub_langs = lang_class_to_audio_and_subs(
                lang_class=_lang_from_title(r.title),
            )
            episode_id = existing_eps.get(ep_key)
            if episode_id is None:
                ep_meta = resolve_episode(tv.tmdb_id, r.season, r.episode,
                                          session=tmdb_sess)
                if ep_meta is None:
                    log.warning("    resolve_episode failed tv_id=%d S%dE%d "
                                "— caching as dead-end for this cluster",
                                tv.tmdb_id, r.season, r.episode)
                    tmdb_dead_ends.add(ep_key)
                    stats.failed_tmdb_episode += 1
                    cur.execute("ROLLBACK TO SAVEPOINT prh_row")
                    cur.execute("RELEASE SAVEPOINT prh_row")
                    continue
                _action, episode_id = upsert_episode(
                    conn,
                    series_id=series_id,
                    season=r.season,
                    episode_num=r.episode,
                    sktorrent_video_id=None,
                    sktorrent_cdn=None,
                    sktorrent_qualities=[],
                    ep_meta=ep_meta,
                    has_dub=audio_lang is not None,
                    has_subtitles=bool(sub_langs),
                )
                if episode_id is None:
                    stats.failed_other += 1
                    cur.execute("ROLLBACK TO SAVEPOINT prh_row")
                    cur.execute("RELEASE SAVEPOINT prh_row")
                    continue
                existing_eps[ep_key] = episode_id
                stats.added_episode += 1
            else:
                # OR-in language flags onto existing row.
                cur.execute(
                    "UPDATE episodes SET "
                    "has_dub = has_dub OR %s, "
                    "has_subtitles = has_subtitles OR %s "
                    "WHERE id = %s",
                    (audio_lang is not None, bool(sub_langs), episode_id),
                )
            url = ("https://prehraj.to" + r.url
                   if r.url.startswith("/") else r.url)
            metadata = {"url": url, "phase": "prehrajto-series-discover"}
            source_id = upsert_video_source(
                cur,
                provider_id=providers["prehrajto"],
                external_id=r.upload_id,
                episode_id=episode_id,
                title=r.title,
                duration_sec=r.duration_sec,
                view_count=r.views or None,
                lang_class=vs_lang_class,
                audio_lang=audio_lang,
                audio_detected_by=("title_regex"
                                   if vs_lang_class != "UNKNOWN" else None),
                is_primary=False,
                is_alive=True,
                metadata=metadata,
            )
            for sub_lang in sub_langs:
                upsert_subtitle(cur, source_id, sub_lang)
            cur.execute("RELEASE SAVEPOINT prh_row")
            existing_external_ids.add(r.upload_id)
            stats.added_source += 1
        except Exception:  # noqa: BLE001
            log.exception("    row failed S%dE%d ext=%s — rollback to savepoint",
                          r.season, r.episode, r.upload_id)
            cur.execute("ROLLBACK TO SAVEPOINT prh_row")
            cur.execute("RELEASE SAVEPOINT prh_row")
            stats.failed_other += 1
            continue

    conn.commit()
    return stats


# Re-use the title→lang_class regex bag from prehrajto_search via detect_lang.
# Importing lazily here so the top-of-file import block stays tidy.
def _lang_from_title(title: str) -> str:
    from scripts.auto_import.prehrajto_search import detect_lang
    return detect_lang(title)


def _push_cover_to_r2(series_id: int, cover_dir: str) -> None:
    """Upload `cover_dir/{id}/cover{,-large}.webp` to R2 via rclone.

    R2 layout matches what `cr-web/src/handlers/series.rs::series_cover`
    reads: `cr-images/series/{id}/cover.webp` (200×300) and
    `cr-images/series/{id}/cover-large.webp` (780×1170). Without this
    push the new series page would serve the 1×1 placeholder since the
    Rust handler reads from R2, not from local disk.

    `--s3-no-check-bucket` skips a ListBuckets call our R2 token isn't
    authorised for — without that flag every copyto prints an "Access
    Denied" notice despite the PUT itself succeeding. Best-effort: a
    failure here leaves the cover only locally; surrounding cluster
    work continues regardless.
    """
    import shutil
    import subprocess
    if not shutil.which("rclone"):
        log.warning("    rclone not in PATH — series id=%d cover stays "
                    "local only; user sees placeholder until manual "
                    "sync", series_id)
        return
    cover_root = Path(cover_dir) / str(series_id)
    small = cover_root / "cover.webp"
    large = cover_root / "cover-large.webp"
    if not small.exists():
        log.warning("    series id=%d: local cover.webp missing — "
                    "ensure_series did not download a poster", series_id)
        return
    for path, variant in ((small, "cover.webp"),
                          (large, "cover-large.webp")):
        if not path.exists():
            continue
        dest = f"cr-r2:cr-images/series/{series_id}/{variant}"
        try:
            r = subprocess.run(
                ["rclone", "copyto", "--s3-no-check-bucket",
                 str(path), dest],
                capture_output=True, text=True, timeout=60,
            )
            if r.returncode != 0:
                tail = ((r.stderr or r.stdout or "").strip()
                        .splitlines()[-3:])
                log.warning("    rclone push failed for %s: %s",
                            dest, " | ".join(tail))
            else:
                log.info("    pushed %s to R2", dest)
        except subprocess.TimeoutExpired:
            log.warning("    rclone push timed out for %s", dest)
        except Exception as e:  # noqa: BLE001
            log.warning("    rclone push raised for %s: %s", dest, e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _non_negative_int(s: str) -> int:
    """argparse type for `--offset` / `--limit` — rejects negatives.

    Without this guard a `--offset -5` would silently slice `clusters[-5:]`
    (Python's "last 5" semantics) — counter-intuitive and easy to
    mistype. `--limit -1` would similarly select all-but-last instead
    of failing. We require >= 0 and use the value 0 as the documented
    "no limit / no skip" sentinel (consistent with how `if args.limit:`
    is already used in the discover and enrich code paths).
    """
    try:
        n = int(s)
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"expected a non-negative integer, got {s!r}",
        ) from e
    if n < 0:
        raise argparse.ArgumentTypeError(
            f"must be >= 0, got {n} "
            f"(use 0 as the explicit \"no limit / no skip\" value)",
        )
    return n


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--mode", choices=("enrich", "discover"),
                    default="enrich",
                    help="enrich (Phase A, #685): attach prehrajto sources "
                         "to existing series in DB. "
                         "discover (Phase B, #686): parse prehrajto sitemap, "
                         "find shows not yet in our DB, create them via "
                         "TMDB + Gemma and attach episode sources.")
    ap.add_argument("--limit", type=_non_negative_int, default=10,
                    help="Process at most N targets — series for enrich, "
                         "clusters for discover (default 10). 0 means "
                         "\"no limit\" (process every target the filter "
                         "returns).")
    ap.add_argument("--offset", type=_non_negative_int, default=0,
                    help="Skip the first N targets. enrich mode: skips N "
                         "series (ordered by id). discover mode: skips N "
                         "clusters AFTER the upload-count-desc sort, so "
                         "`--offset 30 --limit 30` does the second batch "
                         "of 30. Useful for resuming an interrupted run. "
                         "0 means \"start from the beginning.\"")
    ap.add_argument("--match", default=None,
                    help="enrich: case-insensitive substring on "
                         "series.title/.original_title. "
                         "discover: substring on cluster base_title.")
    ap.add_argument("--sitemap-dir",
                    default="/var/cache/cr/prehrajto-sitemap",
                    help="discover mode: directory of video-sitemap-*.xml "
                         "files (default: /var/cache/cr/prehrajto-sitemap "
                         "where the daily cron caches them on the VPS).")
    ap.add_argument("--covers-dir", default="data/movies/series-covers",
                    help="discover mode: target directory for TMDB cover "
                         "downloads (id-keyed layout).")
    ap.add_argument("--min-distinct-episodes", type=int, default=2,
                    dest="min_distinct_episodes",
                    help="discover mode: drop clusters whose uploads share "
                         "fewer than N distinct (season, episode) tuples "
                         "(default 2). Filters out the worst false-"
                         "positive case: a single film re-uploaded by many "
                         "users with an `S01E01` placeholder marker.")
    ap.add_argument("--dry-run", action="store_true",
                    help="ROLLBACK every per-cluster/per-series commit at "
                         "the end. Live searches / TMDB still hit; no DB "
                         "writes persisted, no covers downloaded.")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    _setup_logging(args.verbose)

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        log.error("DATABASE_URL env var required")
        return 2
    if not os.environ.get("TMDB_API_KEY"):
        log.error("TMDB_API_KEY env var required")
        return 2
    if args.mode == "enrich" and proxy_config() is None:
        log.error("CZ_PROXY_URL / CZ_PROXY_KEY env vars required for "
                  "enrich mode — prehraj.to /hledej blocks Hetzner ASNs.")
        return 2
    # discover mode doesn't hit prehraj.to over HTTP (it reads the
    # cached sitemap from disk), so the CZ proxy isn't strictly
    # required. But if it's missing we still WARN — sitemap covers may
    # need refreshing via the proxy at some point.
    if args.mode == "discover" and proxy_config() is None:
        log.warning("CZ_PROXY_URL / CZ_PROXY_KEY not set — sitemap will "
                    "be read from disk but any fallback refresh would fail")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    sess = requests.Session()
    sess.headers["User-Agent"] = DEFAULT_USER_AGENT
    sess.headers["Accept-Encoding"] = "identity"
    tmdb_sess = requests.Session()

    cur = conn.cursor()
    providers = get_provider_ids(cur)

    if args.mode == "discover":
        return _run_discover(args, conn, tmdb_sess, providers)

    targets = load_target_series(
        cur, match=args.match, limit=args.limit, offset=args.offset,
    )
    if not targets:
        log.info("No series matched the filter — nothing to do.")
        return 0
    log.info("Enriching %d series (mode=enrich, dry_run=%s):",
             len(targets), args.dry_run)
    for i, s in enumerate(targets, 1):
        log.info("  %2d. id=%d  %-50s  tmdb=%s",
                 i, s.id, (s.original_title or s.title)[:50], s.tmdb_id)

    summary: list[EnrichStats] = []
    t0 = time.time()
    try:
        for i, target in enumerate(targets, 1):
            log.info("\n>>> [%d/%d] series id=%d  %s",
                     i, len(targets), target.id,
                     (target.original_title or target.title)[:50])
            try:
                stats = enrich_one_series(
                    conn, target, sess, tmdb_sess, providers,
                    dry_run=args.dry_run,
                )
            except BlockedError as e:
                log.error("PROXY BLOCKED — aborting run: %s", e)
                # Ensure we don't leave a half-baked transaction open.
                conn.rollback()
                break
            summary.append(stats)
            if i < len(targets):
                time.sleep(INTER_SERIES_SLEEP_S)
    finally:
        conn.close()

    elapsed = time.time() - t0
    log.info("=" * 70)
    log.info("Done in %.0fs.", elapsed)
    print()
    print("| # | id    | series                                  | found |"
          " matched | +source | +ep | skipped | fail-tmdb | fail | status |")
    print("|--:|------:|------------------------------------------|------:|"
          "--------:|--------:|----:|--------:|----------:|-----:|--------|")
    tot_found = tot_matched = tot_added_src = tot_added_ep = 0
    tot_skipped = tot_fail_tmdb = tot_fail = 0
    for i, s in enumerate(summary, 1):
        print(f"| {i:2d} | {s.series_id:5d} | {s.series_label[:40]:<40} | "
              f"{s.found:5d} | {s.matched:7d} | {s.added_source:7d} | "
              f"{s.added_episode:3d} | {s.skipped_present:7d} | "
              f"{s.failed_tmdb_episode:9d} | {s.failed_other:4d} | "
              f"{s.status} |")
        tot_found += s.found
        tot_matched += s.matched
        tot_added_src += s.added_source
        tot_added_ep += s.added_episode
        tot_skipped += s.skipped_present
        tot_fail_tmdb += s.failed_tmdb_episode
        tot_fail += s.failed_other
    print(f"|    |       | TOTAL                                    | "
          f"{tot_found:5d} | {tot_matched:7d} | {tot_added_src:7d} | "
          f"{tot_added_ep:3d} | {tot_skipped:7d} | {tot_fail_tmdb:9d} | "
          f"{tot_fail:4d} | {'DRY' if args.dry_run else 'LIVE'} |")
    return 0


def _run_discover(
    args: argparse.Namespace,
    conn: psycopg2.extensions.connection,
    tmdb_sess: requests.Session,
    providers: dict,
) -> int:
    """Phase B entry point. Scan sitemap → cluster → TMDB → create/attach."""
    sitemap_dir = Path(args.sitemap_dir)
    if not sitemap_dir.is_dir():
        log.error("--sitemap-dir does not exist or is not a directory: %s",
                  sitemap_dir)
        return 2

    cover_dir = args.covers_dir
    Path(cover_dir).mkdir(parents=True, exist_ok=True)

    # --match is pushed INTO find_series_clusters so we don't materialize
    # ~24k ClusterRows-bags on a prod sitemap just to discard most of them
    # right after. Same end result as the previous post-scan filter, but
    # ~50% less peak memory and ~30% less wall clock on a targeted run.
    clusters = find_series_clusters(
        sitemap_dir,
        min_distinct_episodes=args.min_distinct_episodes,
        match=args.match,
    )
    # Apply --offset BEFORE --limit so `--offset 30 --limit 30` does the
    # second batch of 30 in a paginated run. Without this, the operator
    # had no way to resume after an SSH-disconnect kill that interrupted
    # a multi-hour discover; they'd silently re-process the same top-N
    # clusters every time (which is wasteful — existing-series re-attach
    # is O(N) database upserts even though they're all no-ops).
    if args.offset:
        skipped = clusters[: args.offset]
        clusters = clusters[args.offset:]
        log.info("--offset %d: skipping %d cluster(s) (%s ... %s)",
                 args.offset, len(skipped),
                 skipped[0].base_title[:30] if skipped else "",
                 skipped[-1].base_title[:30] if skipped else "")
    if args.limit:
        clusters = clusters[: args.limit]
    if not clusters:
        log.info("No clusters to process — nothing to do.")
        return 0

    log.info("Discover plan (top %d by upload count, dry_run=%s):",
             len(clusters), args.dry_run)
    for i, c in enumerate(clusters, 1):
        log.info("  %2d. %-45s year=%s uploads=%d distinct_eps=%d",
                 i, c.base_title[:45], c.year, c.upload_count,
                 len(c.episode_keys))

    summary: list[DiscoverStats] = []
    t0 = time.time()
    try:
        for i, cluster in enumerate(clusters, 1):
            log.info("\n>>> [%d/%d] cluster %r (year=%s, %d uploads)",
                     i, len(clusters), cluster.base_title,
                     cluster.year, cluster.upload_count)
            stats = discover_one_cluster(
                conn, cluster, tmdb_sess, providers, cover_dir,
                dry_run=args.dry_run,
            )
            summary.append(stats)
            # Inter-cluster sleep — TMDB resolve_tv burns 2-4 calls per
            # cluster, and we want to stay well under the 50 rps ceiling
            # the films-import discovered.
            if i < len(clusters):
                time.sleep(INTER_SERIES_SLEEP_S)
    finally:
        conn.close()

    elapsed = time.time() - t0
    log.info("=" * 70)
    log.info("Discover done in %.0fs.", elapsed)
    print()
    print("| # | cluster                                    | year  | upl |"
          " eps | tmdb_id  | series_id | new_series | +ep | +src | skip |"
          " fail-tmdb | fail | status |")
    print("|--:|---------------------------------------------|------:|----:|"
          "----:|---------:|----------:|-----------:|----:|-----:|-----:|"
          "----------:|-----:|--------|")
    tot_uploads = tot_eps = tot_new_series = 0
    tot_added_ep = tot_added_src = tot_skipped = 0
    tot_fail_tmdb = tot_fail = 0
    for i, s in enumerate(summary, 1):
        print(f"| {i:2d} | {s.cluster_key[:43]:<43} | "
              f"{(s.cluster_year or 0):5d} | {s.upload_count:3d} | "
              f"{s.distinct_episodes:3d} | "
              f"{(s.tmdb_id or 0):8d} | "
              f"{(s.series_id or 0):9d} | "
              f"{('Y' if s.series_was_created else 'N'):^10s} | "
              f"{s.added_episode:3d} | {s.added_source:4d} | "
              f"{s.skipped_present:4d} | {s.failed_tmdb_episode:9d} | "
              f"{s.failed_other:4d} | {s.status} |")
        tot_uploads += s.upload_count
        tot_eps += s.distinct_episodes
        if s.series_was_created:
            tot_new_series += 1
        tot_added_ep += s.added_episode
        tot_added_src += s.added_source
        tot_skipped += s.skipped_present
        tot_fail_tmdb += s.failed_tmdb_episode
        tot_fail += s.failed_other
    print(f"|    | TOTAL ({len(summary)} clusters){'':23} | "
          f"{'':5s} | {tot_uploads:3d} | {tot_eps:3d} | "
          f"{'':8s} | {'':9s} | {tot_new_series:^10d} | "
          f"{tot_added_ep:3d} | {tot_added_src:4d} | "
          f"{tot_skipped:4d} | {tot_fail_tmdb:9d} | "
          f"{tot_fail:4d} | {'DRY' if args.dry_run else 'LIVE'} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
