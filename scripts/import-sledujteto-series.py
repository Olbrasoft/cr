#!/usr/bin/env python3
"""Import series episode sources from sledujteto.cz (#744 / #745 / #746).

Two-phase pipeline modelled on `scripts/import-prehrajto-series.py`,
with the key difference that sledujteto's discovery surface is the
offline raw-scrape JSON in `data/sledujteto/sledujteto-series-raw-*.json`
(produced by `scripts/scrape-sledujteto-series.py`) rather than a live
sitemap stream. The site blocks Hetzner datacenter IPs on its search
endpoint, so all enrich/discover work runs against the pre-scraped
local file — no live HTTP to sledujteto.cz is needed.

Phases (selected via `--mode`):

* `enrich`   (issue #745) — for every SxxExx-tagged sledujteto upload
              that (a) is **playable from Hetzner** (cdn=www) and
              (b) whose parsed show title alias-matches a series
              already in our DB, INSERT a `video_sources` row attached
              to the episode (creating the episode via TMDB resolve
              if it doesn't yet exist).

* `discover` (issue #746) — clusters not matching any existing series:
              TMDB-resolve them. If at least one upload in the cluster
              is playable, create the series (ensure_series + Gemma +
              cover + episodes) and attach all playable sources. If
              none are playable, append the cluster's episodes to a
              JSONL upload-queue file so the future prehraj.to upload
              pipeline can re-host them.

Policy (confirmed with user):
  * EXISTING series + NOT playable → SKIP (do nothing — episode
    already has some source via another provider)
  * EXISTING series + PLAYABLE     → attach (Phase A)
  * NEW      series + PLAYABLE     → create + attach (Phase B)
  * NEW      series + NOT playable → write to upload-queue JSONL

Lessons baked in from the prehrajto-series equivalent:
  * Alias matching uses TMDB `alternative_titles` (#748) so branded
    upload names (e.g. "Jo Nesbo's Detective Hole" for Harry Hole)
    actually match the DB row's `tmdb_id`. Cached on disk.
  * Kids genre (TMDB 10762) is NOT filtered (#747) — kids series
    belong in `series`, not `tv_shows`.
  * Per-cluster SAVEPOINT — one failed cluster doesn't roll back the
    run.
  * `--match`/`--limit`/`--offset`/`--dry-run` mirror the prehrajto
    importer's UX.

Usage:
    DATABASE_URL=... TMDB_API_KEY=... GEMINI_API_KEY=... \\
        python3 scripts/import-sledujteto-series.py \\
            --input data/sledujteto/sledujteto-series-raw-2026-05-18.json \\
            --mode discover --limit 5 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_PROJECT_ROOT / "scripts"))

import psycopg2
import psycopg2.extras
import requests

from scripts.auto_import.lang_detect import detect_lang
from scripts.auto_import.prehrajto_series_search import (
    alias_variants,
    normalize_alias,
)
from scripts.auto_import.series_enricher import ensure_series, upsert_episode
from scripts.auto_import.title_parser import (
    ParsedTitle,
    parse_prehrajto_episode_title,
)
from scripts.auto_import.tmdb_resolver import (
    fetch_alternative_titles,
    resolve_episode,
    resolve_tv,
)
from video_sources_helper import (
    get_provider_ids,
    lang_class_to_audio_and_subs,
    upsert_video_source,
)

log = logging.getLogger("import-sledujteto-series")

PROVIDER_SLUG = "sledujteto"
DEFAULT_INPUT = (
    _PROJECT_ROOT / "data" / "sledujteto"
    / f"sledujteto-series-raw-{date.today().isoformat()}.json"
)
UPLOAD_QUEUE_DIR = _PROJECT_ROOT / "data" / "sledujteto"
DEFAULT_COVERS_DIR = _PROJECT_ROOT / "data" / "movies" / "series-covers"

# Compound-title separators uploaders use ("CS - EN").
_SEP_RE = re.compile(r"\s+[\-—|]\s+")

# Boundary cut to mirror the prehrajto parser — same constants but we
# need a slightly broader one here because sledujteto entries sometimes
# wrap year in {YYYY} or just `YYYY ` after the title.
_BOUNDARY_RE = re.compile(
    r"\bS\d{1,2}E\d{1,3}\b|"
    r"\b\d{1,2}x\d{1,3}\b|"
    r"\{[12]\d{3}\}|"
    r"\([12]\d{3}\)|"
    r"\s[12]\d{3}\s",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class SledujtetoEpisode:
    """One sledujteto upload, post-parse."""
    slug_id: str
    raw_title: str
    season: int
    episode: int
    lang_class: str
    playable: bool          # preview URL host == www.sledujteto.cz
    full_url: str
    preview_url: str
    duration_sec: int | None
    filesize_bytes: int | None
    resolution_hint: str | None
    view_count: int | None
    year: int | None


@dataclass
class SledujtetoCluster:
    """All episodes of one show in the scrape, grouped by base title + year."""
    base_title: str            # display form (first candidate's text)
    candidates: list[str]      # all alias-candidate strings (compound + halves)
    year: int | None
    episodes: list[SledujtetoEpisode] = field(default_factory=list)

    @property
    def has_any_playable(self) -> bool:
        return any(e.playable for e in self.episodes)


@dataclass
class Stats:
    clusters_total: int = 0
    clusters_matched: int = 0
    clusters_new_tmdb: int = 0
    clusters_no_tmdb: int = 0
    clusters_skipped_existing_unplayable: int = 0
    series_created: int = 0
    episodes_created: int = 0
    sources_added: int = 0
    sources_skipped_present: int = 0
    sources_skipped_unplayable: int = 0
    queued_for_upload: int = 0
    failed_resolve_episode: int = 0
    failed_other: int = 0


# ---------------------------------------------------------------------------
# Scrape parsing
# ---------------------------------------------------------------------------


def _split_compound(raw_title: str) -> list[str]:
    """Return alias-match candidates for the raw upload title.

    "Chirurgové - Greys Anatomy S22E06" → ["Chirurgové Greys Anatomy",
    "Chirurgové", "Greys Anatomy"]. Single-half titles return [whole].
    """
    raw = raw_title.strip()
    m = _BOUNDARY_RE.search(raw)
    if m:
        raw = raw[:m.start()].strip()
    raw = re.sub(r"^[\s\-—|]+|[\s\-—|]+$", "", raw)
    candidates = [raw]
    parts = _SEP_RE.split(raw)
    if len(parts) >= 2:
        for p in parts:
            p = p.strip()
            if len(p) >= 3 and not p.isdigit():
                candidates.append(p)
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        k = c.lower()
        if k and k not in seen:
            seen.add(k)
            out.append(c)
    return out


def _is_playable(entry: dict) -> bool:
    """Hetzner-playable iff the preview URL host is www.sledujteto.cz.

    Per `cr-web/src/handlers/movies_api/sledujteto.rs:15-18`,
    www.sledujteto.cz serves 206 Partial Content from any ASN;
    data{N}.sledujteto.cz is blocked from datacenter IPs (and from
    the cr-web validate endpoint). The preview-URL host is a good
    proxy for the upload's actual storage cluster.
    """
    preview = (entry.get("preview") or "").strip()
    if not preview or "://" not in preview:
        return False
    try:
        host = preview.split("/")[2]
    except IndexError:
        return False
    return host == "www.sledujteto.cz"


def _parse_duration_to_sec(s: str | None) -> int | None:
    """Parse '1h 53m 29s' → 6809 seconds. None on unknown format."""
    if not s:
        return None
    total = 0
    for n, unit in re.findall(r"(\d+)\s*([hms])", s):
        if unit == "h":
            total += int(n) * 3600
        elif unit == "m":
            total += int(n) * 60
        elif unit == "s":
            total += int(n)
    return total or None


def _parse_filesize_to_bytes(s: str | None) -> int | None:
    """Parse '5.36 GB' → 5755558297. None on unknown format."""
    if not s:
        return None
    m = re.match(r"^\s*([\d.,]+)\s*([KMGT])B\s*$", s, re.IGNORECASE)
    if not m:
        return None
    num = float(m.group(1).replace(",", "."))
    mult = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}[m.group(2).upper()]
    return int(num * mult)


def load_clusters(raw_path: Path) -> dict[tuple[str, int | None], SledujtetoCluster]:
    """Stream-parse the scrape, return clusters keyed by (norm, year).

    Cluster key is the normalized form of the compound candidate
    ("Chirurgové - Greys Anatomy" → "chirurgovegreysanatomy"). To
    prevent the same show being split across multiple clusters when
    different uploaders use different name halves
    ("Chirurgové S22E01" vs "Greys Anatomy S22E02"), we also maintain
    a reverse index from EVERY candidate variant's norm back to the
    cluster's primary key. Uploads B/C above find A's cluster via the
    half-norm lookup and merge in.
    """
    log.info("Loading scrape: %s", raw_path)
    raw = json.loads(raw_path.read_text())
    log.info("  %d total uploads", len(raw))

    clusters: dict[tuple[str, int | None], SledujtetoCluster] = {}
    # Maps (norm_of_any_candidate, year) → primary cluster key. Same
    # year-bound semantics as the cluster key so reboots (e.g.
    # "Yellowstone" 2018 vs 2005) still get separate clusters.
    candidate_index: dict[tuple[str, int | None], tuple[str, int | None]] = {}
    parsed = 0
    skipped_no_episode = 0
    for slug_id, u in raw.items():
        name = (u.get("name") or u.get("filename") or "").strip()
        if not name:
            continue
        pt = parse_prehrajto_episode_title(name)
        if not pt.is_episode:
            skipped_no_episode += 1
            continue
        candidates = _split_compound(name)
        if not candidates:
            continue
        # Look up existing cluster via ANY candidate's normalized form
        # before falling back to a fresh key. This merges later uploads
        # that use just one half of a previously-seen compound title.
        key: tuple[str, int | None] | None = None
        for c in candidates:
            cand_key = (normalize_alias(c), pt.year)
            if cand_key[0] and cand_key in candidate_index:
                key = candidate_index[cand_key]
                break
        if key is None:
            base_norm = normalize_alias(candidates[0])
            if not base_norm:
                continue
            key = (base_norm, pt.year)
        cl = clusters.setdefault(key, SledujtetoCluster(
            base_title=candidates[0],
            candidates=[],
            year=pt.year,
        ))
        # Register all candidate variants → this cluster's key so future
        # uploads with a half-only title find their way home.
        for c in candidates:
            cand_key = (normalize_alias(c), pt.year)
            if cand_key[0]:
                candidate_index.setdefault(cand_key, key)
            if c not in cl.candidates:
                cl.candidates.append(c)
        cl.episodes.append(SledujtetoEpisode(
            slug_id=slug_id,
            raw_title=name,
            season=pt.season,
            episode=pt.episode,
            # Use detect_lang's video_sources-compatible enum
            # (CZ_DUB|CZ_SUB|SK_DUB|SK_SUB|CZ_NATIVE|EN|UNKNOWN) rather
            # than the parser's raw `langs` flags (SUBS_CZ etc.) which
            # don't match the DB CHECK constraint.
            lang_class=detect_lang(name),
            playable=_is_playable(u),
            full_url=u.get("full_url") or u.get("link") or "",
            preview_url=u.get("preview", "") or "",
            duration_sec=_parse_duration_to_sec(u.get("duration")),
            filesize_bytes=_parse_filesize_to_bytes(u.get("filesize")),
            resolution_hint=u.get("resolution"),
            view_count=u.get("views"),
            year=pt.year,
        ))
        parsed += 1

    log.info("  %d parsed episodes → %d clusters (%d non-episode skipped)",
             parsed, len(clusters), skipped_no_episode)
    return clusters


# ---------------------------------------------------------------------------
# Alias index: DB series → match candidates (incl. TMDB alt-titles)
# ---------------------------------------------------------------------------


@dataclass
class SeriesRow:
    id: int
    title: str
    original_title: str | None
    first_air_year: int | None
    tmdb_id: int | None


def load_series_alias_index(cur, tmdb_sess: requests.Session,
                              ) -> dict[str, SeriesRow]:
    """Build {normalized_alias: SeriesRow} for every series in DB.

    Per series, we add: title, original_title, AND every TMDB
    alternative_title (fetched lazily, cached to
    `data/tmdb-cache/alt-titles.json`). The cache makes a re-run cheap
    — alt-titles are already populated for the 10 291 series after
    #748 + the discover backfill.
    """
    cur.execute(
        "SELECT id, title, original_title, first_air_year, tmdb_id "
        "FROM series ORDER BY id"
    )
    rows = [SeriesRow(*r) for r in cur.fetchall()]
    log.info("loading alias index for %d series ...", len(rows))

    idx: dict[str, SeriesRow] = {}
    alt_count = 0
    for s in rows:
        for variant in (s.title, s.original_title):
            for k in alias_variants(variant or ""):
                if k not in idx:
                    idx[k] = s
        if s.tmdb_id:
            try:
                for alt in fetch_alternative_titles(s.tmdb_id, session=tmdb_sess):
                    for k in alias_variants(alt):
                        if k not in idx:
                            idx[k] = s
                            alt_count += 1
            except Exception as e:  # noqa: BLE001
                log.warning("alt-titles fetch failed for series #%d tmdb=%s: %s",
                             s.id, s.tmdb_id, e)
    log.info("alias index built: %d total entries (%d from TMDB alt-titles)",
             len(idx), alt_count)
    return idx


def match_cluster_to_series(cluster: SledujtetoCluster,
                              alias_index: dict[str, SeriesRow],
                              ) -> SeriesRow | None:
    """Try every candidate alias variant for this cluster against the index."""
    for c in cluster.candidates:
        for k in alias_variants(c):
            s = alias_index.get(k)
            if s is not None:
                return s
    return None


# ---------------------------------------------------------------------------
# Phase A — enrich (attach sources to existing series)
# ---------------------------------------------------------------------------


def get_existing_episode_ids(cur, series_id: int) -> dict[tuple[int, int], int]:
    cur.execute(
        "SELECT id, season, episode FROM episodes WHERE series_id = %s",
        (series_id,),
    )
    return {(r[1], r[2]): r[0] for r in cur.fetchall()}


def get_existing_sledujteto_external_ids(cur, series_id: int,
                                            providers: dict) -> set[str]:
    cur.execute(
        """SELECT vs.external_id
             FROM episodes e
             JOIN video_sources vs
               ON vs.episode_id = e.id
              AND vs.provider_id = %s
            WHERE e.series_id = %s""",
        (providers[PROVIDER_SLUG], series_id),
    )
    return {row[0] for row in cur.fetchall()}


def enrich_cluster(conn, cluster: SledujtetoCluster, series: SeriesRow,
                    providers: dict, stats: Stats,
                    tmdb_sess: requests.Session) -> None:
    """Phase A: attach playable sledujteto sources to an existing series.

    Per user policy: not-playable episodes are SKIPPED (the existing
    series already has some source, a dead sledujteto link adds no value
    without the prehraj.to-upload pipeline that would re-host them).
    """
    cur = conn.cursor()
    existing_eps = get_existing_episode_ids(cur, series.id)
    existing_ext = get_existing_sledujteto_external_ids(cur, series.id, providers)

    for ep in cluster.episodes:
        if not ep.playable:
            stats.sources_skipped_unplayable += 1
            continue
        if ep.slug_id in existing_ext:
            stats.sources_skipped_present += 1
            continue

        cur.execute("SAVEPOINT slt_match")
        try:
            ep_key = (ep.season, ep.episode)
            episode_id = existing_eps.get(ep_key)
            if episode_id is None:
                if not series.tmdb_id:
                    log.warning("  series #%d has no tmdb_id — can't resolve S%dE%d",
                                 series.id, ep.season, ep.episode)
                    cur.execute("ROLLBACK TO SAVEPOINT slt_match")
                    cur.execute("RELEASE SAVEPOINT slt_match")
                    stats.failed_resolve_episode += 1
                    continue
                ep_meta = resolve_episode(series.tmdb_id, ep.season, ep.episode,
                                            session=tmdb_sess)
                if ep_meta is None:
                    log.warning("  TMDB resolve_episode failed: tv=%d S%dE%d",
                                 series.tmdb_id, ep.season, ep.episode)
                    cur.execute("ROLLBACK TO SAVEPOINT slt_match")
                    cur.execute("RELEASE SAVEPOINT slt_match")
                    stats.failed_resolve_episode += 1
                    continue
                audio_lang, _, sub_langs = lang_class_to_audio_and_subs(
                    lang_class=ep.lang_class)
                _action, episode_id = upsert_episode(
                    conn,
                    series_id=series.id,
                    season=ep.season,
                    episode_num=ep.episode,
                    sktorrent_video_id=None,
                    sktorrent_cdn=None,
                    sktorrent_qualities=[],
                    ep_meta=ep_meta,
                    has_dub=audio_lang is not None,
                    has_subtitles=bool(sub_langs),
                )
                stats.episodes_created += 1
                existing_eps[ep_key] = episode_id

            audio_lang, _, sub_langs = lang_class_to_audio_and_subs(
                lang_class=ep.lang_class)
            upsert_video_source(
                cur,
                provider_id=providers[PROVIDER_SLUG],
                external_id=ep.slug_id,
                episode_id=episode_id,
                title=ep.raw_title,
                duration_sec=ep.duration_sec,
                resolution_hint=ep.resolution_hint,
                filesize_bytes=ep.filesize_bytes,
                view_count=ep.view_count,
                lang_class=ep.lang_class,
                audio_lang=audio_lang,
                audio_detected_by="title_regex",
                cdn="www",
                is_alive=True,
            )
            stats.sources_added += 1
            cur.execute("RELEASE SAVEPOINT slt_match")
        except Exception as e:  # noqa: BLE001
            log.exception("  unexpected failure on series=%d S%dE%d ext=%s: %s",
                            series.id, ep.season, ep.episode, ep.slug_id, e)
            cur.execute("ROLLBACK TO SAVEPOINT slt_match")
            cur.execute("RELEASE SAVEPOINT slt_match")
            stats.failed_other += 1


# ---------------------------------------------------------------------------
# Phase B — discover (create new series OR queue for prehrajto upload)
# ---------------------------------------------------------------------------


def load_queued_slug_ids(queue_path: Path) -> set[str]:
    """Read every `slug_id` already present in `queue_path`.

    Used to dedupe before append — re-running the importer on the same
    day (or after a crash) would otherwise stack duplicate JSONL rows
    that the downstream prehraj.to upload pipeline has to filter back
    out. Returns an empty set if the file doesn't exist yet.
    """
    if not queue_path.exists():
        return set()
    seen: set[str] = set()
    with queue_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                slug = json.loads(line).get("slug_id")
            except json.JSONDecodeError:
                continue
            if slug:
                seen.add(slug)
    return seen


def _queue_unplayable_episodes(eps: list[SledujtetoEpisode], tv,
                                 queue_fh, queued_ext_ids: set[str],
                                 stats: Stats) -> int:
    """Append non-playable episodes to the upload queue, skipping
    `slug_id`s already present (rerun-safe — see `load_queued_slug_ids`).
    Returns the number of NEW entries written.
    """
    today = date.today().isoformat()
    written = 0
    for ep in eps:
        if ep.slug_id in queued_ext_ids:
            continue
        queue_fh.write(json.dumps({
            "sledujteto_url": ep.full_url,
            "slug_id": ep.slug_id,
            "raw_title": ep.raw_title,
            "season": ep.season,
            "episode": ep.episode,
            "year": ep.year,
            "tmdb_tv_id": tv.tmdb_id,
            "tmdb_name": tv.name_cs or tv.name_en or tv.original_name,
            "lang_class": ep.lang_class,
            "found_at": today,
        }, ensure_ascii=False) + "\n")
        queued_ext_ids.add(ep.slug_id)
        stats.queued_for_upload += 1
        written += 1
    return written


def discover_cluster(conn, cluster: SledujtetoCluster, providers: dict,
                       stats: Stats, queue_fh, queued_ext_ids: set[str], *,
                       covers_dir: Path, tmdb_sess: requests.Session,
                       dry_run: bool) -> None:
    """Phase B: TMDB-resolve unmatched cluster; create or queue."""
    parsed = ParsedTitle(
        raw=cluster.base_title,
        cz_title=cluster.base_title,
        en_title=None,
        year=cluster.year,
    )
    tv = resolve_tv(parsed, session=tmdb_sess)
    if tv is None or not tv.tmdb_id:
        stats.clusters_no_tmdb += 1
        log.info("  no TMDB hit for %r year=%s", cluster.base_title, cluster.year)
        # If at least one upload is playable, the user still loses access
        # because we can't catalog it without TMDB metadata. Logging only
        # for now — the unmatched-TMDB CSV writer is the right home and
        # is out of scope for the first ship.
        return

    cur = conn.cursor()
    cur.execute("SELECT id FROM series WHERE tmdb_id = %s", (tv.tmdb_id,))
    row = cur.fetchone()
    if row is not None:
        # Race: another concurrent run, or alias-index miss earlier in
        # this run. Fall through to enrich path so we don't lose the
        # episodes — re-fetch the SeriesRow.
        cur.execute(
            "SELECT id, title, original_title, first_air_year, tmdb_id "
            "FROM series WHERE id = %s", (row[0],))
        s_row = SeriesRow(*cur.fetchone())
        log.info("  TMDB tv=%d already in DB as series #%d — switching to enrich",
                  tv.tmdb_id, s_row.id)
        stats.clusters_matched += 1
        if cluster.has_any_playable:
            enrich_cluster(conn, cluster, s_row, providers, stats, tmdb_sess)
        else:
            stats.clusters_skipped_existing_unplayable += 1
        return

    if not cluster.has_any_playable:
        # All episodes unplayable → queue every one for re-host.
        written = _queue_unplayable_episodes(cluster.episodes, tv, queue_fh,
                                              queued_ext_ids, stats)
        log.info("  cluster %r (%d eps, tmdb=%d): %d queued for re-host, "
                  "%d already in queue (skipped)",
                  cluster.base_title, len(cluster.episodes), tv.tmdb_id,
                  written, len(cluster.episodes) - written)
        return

    # Playable + new: create series + attach all playable sources.
    if dry_run:
        log.info("  [dry-run] WOULD create series tmdb=%d (%s, %d eps to attach)",
                  tv.tmdb_id, tv.name_cs or tv.name_en, len(cluster.episodes))
        # Still queue the unplayable half so the prehraj.to upload
        # pipeline has a complete picture even in dry-run.
        _queue_unplayable_episodes(
            [e for e in cluster.episodes if not e.playable],
            tv, queue_fh, queued_ext_ids, stats,
        )
        stats.clusters_new_tmdb += 1
        return

    was_created, series_id = ensure_series(conn, tv, str(covers_dir))
    if series_id is None:
        log.warning("  ensure_series returned None for tmdb=%d", tv.tmdb_id)
        stats.failed_other += 1
        return
    if was_created:
        stats.series_created += 1

    # Re-load aliases for THIS new series and call enrich_cluster for
    # the source-attach loop (DRY — same per-episode logic, same
    # SAVEPOINT defenses).
    cur.execute(
        "SELECT id, title, original_title, first_air_year, tmdb_id "
        "FROM series WHERE id = %s", (series_id,))
    s_row = SeriesRow(*cur.fetchone())
    enrich_cluster(conn, cluster, s_row, providers, stats, tmdb_sess)
    # Mixed cluster: enrich_cluster only attached the playable half.
    # Queue the unplayable half for re-host so it isn't silently lost.
    _queue_unplayable_episodes(
        [e for e in cluster.episodes if not e.playable],
        tv, queue_fh, queued_ext_ids, stats,
    )
    stats.clusters_new_tmdb += 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _non_negative_int(s: str) -> int:
    n = int(s)
    if n < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return n


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--mode", choices=("enrich", "discover", "both"),
                    default="both",
                    help="enrich: only attach sources to existing series. "
                         "discover: only create new series + queue. "
                         "both (default): process each cluster — enrich if "
                         "matched, discover otherwise.")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                    help=f"raw scrape JSON (default: {DEFAULT_INPUT})")
    ap.add_argument("--covers-dir", type=Path, default=DEFAULT_COVERS_DIR)
    ap.add_argument("--limit", type=_non_negative_int, default=10,
                    help="process at most N clusters (0 = no limit)")
    ap.add_argument("--offset", type=_non_negative_int, default=0,
                    help="skip N clusters (after sort, before --limit)")
    ap.add_argument("--match", default=None,
                    help="case-insensitive substring on cluster base_title")
    ap.add_argument("--dry-run", action="store_true",
                    help="ROLLBACK every per-cluster transaction; queue "
                         "JSONL goes to a `*-dryrun-*` filename so it can "
                         "be inspected without polluting the real queue.")
    ns = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL not set")
        return 2
    if not os.environ.get("TMDB_API_KEY"):
        log.error("TMDB_API_KEY not set — alt-titles + TMDB resolve won't work")
        return 2

    if not ns.input.exists():
        log.error("Input scrape file not found: %s", ns.input)
        return 2

    clusters_map = load_clusters(ns.input)
    clusters = list(clusters_map.values())
    if ns.match:
        clusters = [c for c in clusters
                    if ns.match.lower() in c.base_title.lower()]
        log.info("--match %r: %d clusters survive", ns.match, len(clusters))
    # Sort: playable+large first (more user value), then by ep count desc.
    clusters.sort(key=lambda c: (
        -sum(1 for e in c.episodes if e.playable),
        -len(c.episodes),
    ))
    if ns.offset:
        clusters = clusters[ns.offset:]
    if ns.limit:
        clusters = clusters[:ns.limit]
    log.info("processing %d clusters (offset=%d limit=%d match=%r)",
             len(clusters), ns.offset, ns.limit, ns.match)

    UPLOAD_QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    queue_name = (
        f"sledujteto-pending-prehrajto-uploads-{'dryrun-' if ns.dry_run else ''}"
        f"{date.today().isoformat()}.jsonl"
    )
    queue_path = UPLOAD_QUEUE_DIR / queue_name
    queued_ext_ids = load_queued_slug_ids(queue_path)
    queue_fh = queue_path.open("a", encoding="utf-8")
    log.info("upload queue: %s (%d slug_ids already present, will skip)",
              queue_path, len(queued_ext_ids))

    conn = psycopg2.connect(db_url)
    tmdb_sess = requests.Session()
    cur = conn.cursor()
    providers = get_provider_ids(cur)
    if PROVIDER_SLUG not in providers:
        log.error("video_providers row %r not found", PROVIDER_SLUG)
        return 2

    alias_index = load_series_alias_index(cur, tmdb_sess)
    stats = Stats()
    stats.clusters_total = len(clusters)

    started = time.time()
    for i, cluster in enumerate(clusters, 1):
        log.info(">>> [%d/%d] cluster %r (year=%s, %d eps, %d playable)",
                  i, len(clusters), cluster.base_title, cluster.year,
                  len(cluster.episodes),
                  sum(1 for e in cluster.episodes if e.playable))
        cur.execute("SAVEPOINT slt_cluster")
        try:
            match = match_cluster_to_series(cluster, alias_index)
            if match is not None and ns.mode in ("enrich", "both"):
                stats.clusters_matched += 1
                if cluster.has_any_playable:
                    enrich_cluster(conn, cluster, match, providers, stats,
                                    tmdb_sess)
                else:
                    stats.clusters_skipped_existing_unplayable += 1
            elif match is None and ns.mode in ("discover", "both"):
                discover_cluster(conn, cluster, providers, stats, queue_fh,
                                  queued_ext_ids,
                                  covers_dir=ns.covers_dir, tmdb_sess=tmdb_sess,
                                  dry_run=ns.dry_run)
            cur.execute("RELEASE SAVEPOINT slt_cluster")
            if ns.dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception as e:  # noqa: BLE001
            log.exception("  cluster failed: %s", e)
            conn.rollback()
            stats.failed_other += 1

    queue_fh.close()
    elapsed = time.time() - started

    print("\n=== summary ===", file=sys.stderr)
    print(f"  clusters processed:         {stats.clusters_total}", file=sys.stderr)
    print(f"  matched existing series:    {stats.clusters_matched}", file=sys.stderr)
    print(f"    skipped (no playable):    {stats.clusters_skipped_existing_unplayable}",
          file=sys.stderr)
    print(f"  new series created:         {stats.series_created}", file=sys.stderr)
    print(f"  new clusters TMDB-resolved: {stats.clusters_new_tmdb}", file=sys.stderr)
    print(f"  no TMDB match:              {stats.clusters_no_tmdb}", file=sys.stderr)
    print(f"  episodes created:           {stats.episodes_created}", file=sys.stderr)
    print(f"  video_sources added:        {stats.sources_added}", file=sys.stderr)
    print(f"  sources skipped (present):  {stats.sources_skipped_present}",
          file=sys.stderr)
    print(f"  sources skipped (unplay):   {stats.sources_skipped_unplayable}",
          file=sys.stderr)
    print(f"  queued for prehrajto upload:{stats.queued_for_upload}", file=sys.stderr)
    print(f"  failed (TMDB episode):      {stats.failed_resolve_episode}",
          file=sys.stderr)
    print(f"  failed (other):             {stats.failed_other}", file=sys.stderr)
    print(f"  elapsed:                    {elapsed:.1f}s", file=sys.stderr)
    print(f"  queue file:                 {queue_path}", file=sys.stderr)
    print(f"  mode: {'DRY-RUN (rolled back)' if ns.dry_run else 'LIVE'}",
          file=sys.stderr)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
