#!/usr/bin/env python3
"""Import series episode sources from prehraj.to.

Two-phase pipeline (issue #684). Each phase is independently runnable
via `--mode`:

* `enrich`   (issue #685, **implemented here**) — for every series
              already in our DB, search prehraj.to and attach matching
              episode video sources via `video_sources(provider_id=
              prehrajto)`.
* `discover` (issue #686, not yet implemented) — parse prehraj.to
              sitemap, filter SxxExx hits, group by base-title, TMDB-
              resolve, create new `series` + `episodes` rows. To be
              added in a follow-up PR.

The enrich path mirrors the proven sktorrent-series importer
(`scripts/import-sktorrent-series.py`) but talks to prehraj.to instead
of online.sktorrent.eu, and writes via the unified `video_sources`
schema (provider=prehrajto) rather than the legacy
`episodes.sktorrent_video_id` column.

Defenses (lessons from commit 9d1d1e08d in the sktorrent path):
  * Alias filter normalizes BOTH the parsed prehraj.to title AND the
    DB series row's `title` / `original_title` — otherwise multi-
    episode shows quietly lose all but one match.
  * Per-series commits — never wrap the whole run in one transaction,
    or one bad show wipes hours of progress.
  * `--match SUBSTRING` for targeted re-imports after a parser tweak.
  * `--dry-run` rolls back at the end so logs can be inspected without
    touching the DB.

Usage (Phase A — enrich existing series):
    DATABASE_URL=postgres://... TMDB_API_KEY=... \\
    CZ_PROXY_URL=https://chobotnice.aspfree.cz/Proxy.ashx \\
    CZ_PROXY_KEY=... \\
        python3 scripts/import-prehrajto-series.py \\
            --mode enrich --limit 10 --dry-run

For prod: drop --dry-run, raise --limit, optionally narrow with
--match (substring matched against series.title / .original_title).
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
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
from scripts.auto_import.series_enricher import upsert_episode
from scripts.auto_import.tmdb_resolver import resolve_episode
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
# Main
# ---------------------------------------------------------------------------


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
    ap.add_argument("--mode", choices=("enrich", "discover", "both"),
                    default="enrich",
                    help="enrich = attach prehrajto sources to existing series "
                         "in DB (issue #685, implemented). discover = create "
                         "new series from prehrajto sitemap (issue #686, not "
                         "yet implemented).")
    ap.add_argument("--limit", type=int, default=10,
                    help="Process at most N series in enrich mode (default 10)")
    ap.add_argument("--offset", type=int, default=0,
                    help="Skip the first N series — for paginated runs.")
    ap.add_argument("--match", default=None,
                    help="Case-insensitive substring filter on "
                         "series.title / .original_title — for "
                         "re-running against a specific show.")
    ap.add_argument("--dry-run", action="store_true",
                    help="ROLLBACK every per-series commit at the end of "
                         "the series. Live searches still hit prehraj.to + "
                         "TMDB. No DB writes persisted.")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    _setup_logging(args.verbose)

    if args.mode in ("discover", "both"):
        log.error("--mode=%s is not yet implemented — tracked in issue "
                  "#686. Run with --mode=enrich for Phase A.", args.mode)
        return 2

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        log.error("DATABASE_URL env var required")
        return 2
    if not os.environ.get("TMDB_API_KEY"):
        log.error("TMDB_API_KEY env var required (for resolve_episode "
                  "when a matched upload has no episodes row yet)")
        return 2
    if proxy_config() is None:
        log.error("CZ_PROXY_URL / CZ_PROXY_KEY env vars required — "
                  "prehraj.to blocks datacenter ASNs, the CZ residential "
                  "proxy is mandatory.")
        return 2

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    sess = requests.Session()
    sess.headers["User-Agent"] = DEFAULT_USER_AGENT
    sess.headers["Accept-Encoding"] = "identity"
    tmdb_sess = requests.Session()

    cur = conn.cursor()
    providers = get_provider_ids(cur)

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


if __name__ == "__main__":
    sys.exit(main())
