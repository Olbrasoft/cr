#!/usr/bin/env python3
"""Backfill missing SK Torrent series into our DB.

Reads `data/movies/sktorrent-new-series.csv` (produced by
find-sktorrent-new-series.py), takes the top-N entries flagged as `missing`
and ≥`--min-episodes` episodes, and runs the standard auto-import pipeline
for every episode of those series. Each series gets exactly ONE
`process_series_batch` call so the `series` row is created once and all
episodes are inserted in a single transaction-equivalent unit.

How episode video_ids are gathered:
  We don't have per-episode IDs in the CSV (only sample_video_id). For each
  target series we hit SK Torrent's `/search/videos?search_query=<title>`
  endpoint, paginate to the end, and keep results whose parsed title
  collapses to the same union-find anchor we used in the discovery script
  (so spinoffs / similarly-named shows don't bleed in).

Run this on the VPS — it expects local DATABASE_URL pointing at prod, and
covers go to the same series-covers dir auto-import.py already uses.

Usage:
    DATABASE_URL=... TMDB_API_KEY=... GEMINI_API_KEY=... \\
        python3 scripts/import-sktorrent-series.py \\
            --csv data/movies/sktorrent-new-series.csv --top 20

Add --dry-run to enumerate without DB writes.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import psycopg2
import psycopg2.extras
import requests

from scripts.auto_import.cz_proxy import proxy_get
from scripts.auto_import.sktorrent_scanner import _parse_listing_html
from scripts.auto_import.title_parser import parse_sktorrent_title, ParsedTitle
from scripts.auto_import.tmdb_resolver import resolve_tv
from scripts.auto_import.series_enricher import process_series_batch
from scripts.auto_import.tv_show_enricher import process_tv_show_episode

log = logging.getLogger("import-sktorrent-series")

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
SEARCH_URL = "https://online.sktorrent.eu/search/videos"
PAGE_SLEEP_S = 0.6
SEARCH_MAX_PAGES = 30  # 18 results/page → 540 results max per term, plenty for our biggest


def _normalize(s: str | None) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    no_diacritics = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", no_diacritics.lower())


@dataclass
class Target:
    """One missing series we want to import."""

    cz_title: str
    en_title: str
    cz_norm: str
    en_norm: str
    expected_episode_count: int
    sample_url: str


def load_targets(csv_path: Path, top: int, min_eps: int, skip: int = 0) -> list[Target]:
    rows: list[Target] = []
    with csv_path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["status"] != "missing":
                continue
            eps = int(r["episode_count"])
            if eps < min_eps:
                continue
            rows.append(Target(
                cz_title=r["cz_title"],
                en_title=r["en_title"],
                cz_norm=_normalize(r["cz_title"]),
                en_norm=_normalize(r["en_title"]),
                expected_episode_count=eps,
                sample_url=r["sample_url"],
            ))
    rows.sort(key=lambda t: -t.expected_episode_count)
    return rows[skip:skip + top]


def _strip_episode_suffix_for_search(title: str) -> str:
    """Strip every episode / season marker from the tail of a CSV title so
    SK Torrent search hits the actual show name.

    SK Torrent uploaders glue episode numbers onto the show name in many
    inconsistent shapes — every variant below is real:

        "Bokutachi Wa Benkyou Ga Dekinai S2 -10"
        "Mairimashita! Iruma-kun S2 - 08"
        "Mushoku Tensei -23"
        "Strike the blood -24"
        "Tensei Shitara Slime Datta Ken - 05 oprava"
        "Witch Craft Works -13 Ova"
        "Alenka a Lewis 51"
        "My Little Pony: Vyprávěj svůj příběh 70"
        "Gakusen Toshi Asterisk 2 -12"
        "Strike the blood IV -12"
        "World Trigger S2 - 09"
        "ŽIVOT NA ZÁMKU-05_Stopadesát…"
        "Nanatsu No Taizai - Fundo No Shinpa -10"
        "Pomocnice / The Housemaid (S04E51)(CZ)"  → SxxExx form
        "Pohotovost (E05)"                        → bare (E##) form

    We loop the regex set until it stops changing — many real titles need
    two passes (e.g. "S2 - 08" first strips "- 08", then strips "S2").
    """
    if not title:
        return ""
    rules = [
        # Trailing SxxExx (and anything after it).
        (re.compile(r"\bS\d{1,2}E\d{1,2}\b.*$", re.IGNORECASE), ""),
        # "Show NN- Episode title" / "Show NN-Episode title" — the dash is
        # right next to the number, no space. Anchor on word boundary so
        # we don't eat legitimate hyphenated names.
        (re.compile(r"\s\d{1,3}\s*-\s*\S.*$"), ""),
        # Trailing "(E##)" / " E##" / "(EN ##)".
        (re.compile(r"\s*\(E\d{1,3}\)\s*$", re.IGNORECASE), ""),
        (re.compile(r"\s+E\d{1,3}\s*$", re.IGNORECASE), ""),
        # Trailing "Sn - n" / "Sn -n" / "Sn n".
        (re.compile(r"\s+S\d{1,2}(?:\s*-?\s*\d{1,3})?(?:\s*Ova)?\s*$",
                    re.IGNORECASE), ""),
        # Trailing " - <digits>" (with space-dash-space or space-dash) plus
        # an optional trailing word like "Ova", "oprava", "raw", "extra".
        (re.compile(r"\s+-\s*\d{1,3}(?:\s+\w+)?\s*$", re.IGNORECASE), ""),
        # Trailing " IV / V / VI / VII / VIII" Roman season suffix.
        (re.compile(r"\s+(?:II|III|IV|V|VI|VII|VIII|IX|X)\s*$"), ""),
    ]
    prev = None
    s = title
    for _ in range(8):
        if s == prev:
            break
        prev = s
        for rx, repl in rules:
            s = rx.sub(repl, s).rstrip()
    # Bare-digit suffix is the most ambiguous: "Ben 10", "Top Gun 2", "1917",
    # "Sezona 1" all look like "name SPACE digits" but the digit is part of
    # the name. Only strip when the title is long enough (≥4 words) that the
    # trailing number is almost certainly an episode-counter dropped on by
    # the SK Torrent uploader (e.g. "Alenka a Lewis 51",
    # "My Little Pony: Vyprávěj svůj příběh 70").
    if len(s.split()) >= 4 and re.search(r"\s+\d{1,3}\s*$", s):
        s = re.sub(r"\s+\d{1,3}\s*$", "", s)
    return s.strip(" -:_")


def _fetch_search_page(
    session: requests.Session,
    query: str,
    page: int,
) -> requests.Response | None:
    """Fetch one search page with backoff on 403/429/5xx.

    SK Torrent rate-limits aggressively and starts returning 403 to all
    subsequent requests for ~30-60s once tripped. Wait it out instead of
    abandoning the search half-way.

    Routes through `proxy_get` so this works from a Hetzner VPS — sktorrent
    silently returns empty 200s to datacenter ASNs, which would otherwise
    look like an empty result page (and therefore "already_imported").
    """
    from urllib.parse import quote_plus

    target = f"{SEARCH_URL}?search_query={quote_plus(query)}"
    if page > 1:
        target += f"&page={page}"
    delays = [30, 60, 120, 240]
    for attempt, wait in enumerate([0, *delays]):
        if wait:
            log.warning("  page %d backoff #%d — sleeping %ds before retry",
                        page, attempt, wait)
            time.sleep(wait)
        try:
            r = proxy_get(target, session, timeout=30)
        except requests.RequestException as exc:
            log.warning("  page %d transport error: %s", page, exc)
            continue
        if r.status_code == 200:
            return r
        if r.status_code in (403, 429) or 500 <= r.status_code < 600:
            log.warning("  page %d HTTP %d — backing off", page, r.status_code)
            continue
        log.warning("  page %d unexpected HTTP %d — giving up on this page",
                    page, r.status_code)
        return None
    log.error("  page %d retries exhausted", page)
    return None


def search_episodes_for(
    target: Target,
    session: requests.Session,
    max_pages: int = SEARCH_MAX_PAGES,
) -> list[tuple[int, str, ParsedTitle]]:
    """Return (video_id, raw_title, parsed) tuples for episodes matching `target`.

    Tries CZ title first, then EN title if CZ yields nothing. Results are
    filtered through union-find equivalence: keep only titles whose parsed
    cz_norm or en_norm matches one of the target's canonical aliases.
    """
    # Aliases include BOTH the raw CSV-derived form and the stripped form,
    # because sktorrent search results parse to the *clean* show name without
    # the episode marker. Without this, "Misfits" hits returned by searching
    # for "Misfits" wouldn't match alias "misfitsepizoda7" derived from the
    # fragmented CSV title "Misfits - Epizoda 7".
    aliases: set[str] = set()
    queries: list[str] = []
    for raw in (target.cz_title, target.en_title):
        if raw:
            aliases.add(_normalize(raw))
        cleaned = _strip_episode_suffix_for_search(raw)
        if cleaned:
            aliases.add(_normalize(cleaned))
            if cleaned not in queries:
                queries.append(cleaned)
    aliases.discard("")

    seen_ids: set[int] = set()
    matched: list[tuple[int, str, ParsedTitle]] = []
    for q in queries:
        log.info("[%s] search: %r", target.cz_title or target.en_title, q)
        for page in range(1, max_pages + 1):
            r = _fetch_search_page(session, q, page)
            if r is None:
                break
            items = _parse_listing_html(r.text)
            if not items:
                log.info("  page %d empty — done", page)
                break
            new_on_page = sum(1 for it in items if it.video_id not in seen_ids)
            if new_on_page == 0:
                log.info("  page %d all duplicates — done", page)
                break
            for it in items:
                if it.video_id in seen_ids:
                    continue
                seen_ids.add(it.video_id)
                parsed = parse_sktorrent_title(it.title)
                cz_n = _normalize(parsed.cz_title)
                en_n = _normalize(parsed.en_title)
                if cz_n in aliases or en_n in aliases:
                    matched.append((it.video_id, it.title, parsed))
            time.sleep(PAGE_SLEEP_S)
        if matched:
            break
    matched.sort(key=lambda t: t[0])
    return matched


def _langs_to_flags(langs: list[str]) -> tuple[bool, bool]:
    """ParsedTitle.langs → (has_dub, has_subtitles). Mirror of auto-import.py."""
    has_dub = any(x in langs for x in ("DUB_CZ", "DUB_SK", "CZ", "SK"))
    has_subs = any(x in langs for x in ("SUBS_CZ", "SUBS_SK"))
    return has_dub, has_subs


def _import_via_tv_shows(
    label: str,
    tv,
    fresh: list[tuple[int, str, ParsedTitle]],
    conn: psycopg2.extensions.connection | None,
    dry_run: bool,
) -> dict:
    """Fallback for CZ/SK-only shows: insert into tv_shows / tv_episodes.

    Mirrors auto-import.py's `_process_tv_show` but loops directly over our
    pre-fetched search results instead of one scanner item at a time.
    `process_tv_show_episode` is idempotent on the (tv_show_id, season,
    episode, sktorrent_video_id) key so re-runs are safe.
    """
    log.info("[%s] no IMDB on TMDB match — routing to tv_shows fallback", label)
    if dry_run:
        log.info("[%s] DRY RUN — would tv_shows-upsert %d episodes",
                 label, len(fresh))
        return {"target": label, "status": "dry_run_tv_shows_ok",
                "found": len(fresh), "fresh": len(fresh),
                "imported": len(fresh), "failed": 0,
                "tmdb_id": tv.tmdb_id}

    if conn is None:
        return {"target": label, "status": "no_db",
                "found": len(fresh), "fresh": len(fresh),
                "imported": 0, "failed": len(fresh)}

    added = 0
    failed = 0
    for vid, raw_title, parsed in fresh:
        season = parsed.season if parsed.is_episode and parsed.season else 1
        episode = parsed.episode if parsed.is_episode and parsed.episode else None
        if episode is None:
            failed += 1
            log.debug("[%s] tv_shows skip vid=%d — no parseable episode#", label, vid)
            continue
        has_dub, has_subs = _langs_to_flags(parsed.langs)
        try:
            res = process_tv_show_episode(
                conn,
                tv=tv,
                season=season,
                episode=episode,
                sktorrent_video_id=vid,
                sktorrent_cdn=None,        # ephemeral — see series flow
                sktorrent_qualities=[],
                has_dub=has_dub,
                has_subtitles=has_subs,
            )
        except Exception:
            conn.rollback()
            failed += 1
            log.exception("[%s] tv_shows insert crashed for vid=%d", label, vid)
            continue
        conn.commit()
        if res.action.startswith("added"):
            added += 1
        elif res.action == "skipped":
            pass
        else:
            failed += 1
    log.info("[%s] tv_shows DONE — added=%d failed=%d", label, added, failed)
    return {"target": label, "status": "imported_tv_shows",
            "found": len(fresh), "fresh": len(fresh),
            "imported": added, "failed": failed,
            "tmdb_id": tv.tmdb_id}


def import_one_series(
    target: Target,
    conn: psycopg2.extensions.connection,
    series_covers: Path,
    tmdb_session: requests.Session,
    skt_session: requests.Session,
    known_video_ids: set[int],
    dry_run: bool,
) -> dict:
    """Process one target series end-to-end and return per-series stats."""
    label = target.cz_title or target.en_title
    log.info("=" * 70)
    log.info("[%s] starting (CSV expected ~%d episodes)",
             label, target.expected_episode_count)

    found = search_episodes_for(target, skt_session)
    fresh = [t for t in found if t[0] not in known_video_ids]
    log.info("[%s] search found %d matching episodes; %d already in DB, %d to import",
             label, len(found), len(found) - len(fresh), len(fresh))
    if not fresh:
        return {"target": label, "status": "already_imported", "found": len(found),
                "fresh": 0, "imported": 0, "failed": 0}

    # TMDB resolution. Try the SxxExx-bearing parsed title from the highest-
    # video_id match (most recent ⇒ richest metadata in title).
    parsed_for_tmdb: ParsedTitle | None = None
    for _vid, _t, p in reversed(fresh):
        if p.is_episode:
            parsed_for_tmdb = p
            break
    if parsed_for_tmdb is None:
        parsed_for_tmdb = fresh[-1][2]
    log.info("[%s] TMDB resolve via cz=%r en=%r year=%r",
             label, parsed_for_tmdb.cz_title, parsed_for_tmdb.en_title,
             parsed_for_tmdb.year)
    tv = resolve_tv(parsed_for_tmdb, session=tmdb_session)
    if tv is None:
        log.warning("[%s] TMDB resolve failed — skipping", label)
        return {"target": label, "status": "tmdb_failed", "found": len(found),
                "fresh": len(fresh), "imported": 0, "failed": len(fresh)}
    log.info("[%s] TMDB ✓ tmdb_id=%s imdb=%s cs=%r en=%r",
             label, tv.tmdb_id, tv.imdb_id, tv.name_cs, tv.name_en)
    if not tv.imdb_id:
        # CZ/SK-only show — TMDB has it but no IMDB link. The `series` table
        # requires imdb_id; route to `tv_shows` instead, the same way
        # auto-import.py does for /tv-porady/ scans.
        return _import_via_tv_shows(label, tv, fresh, conn, dry_run)

    # Don't fetch sktorrent detail pages for backfill: per the project memory
    # `feedback_sktorrent_cdn_ephemeral.md`, sktorrent_cdn rotates and is
    # resolved at play time through the CZ proxy. Saving a snapshot taken
    # months/years after upload would just be wrong. Episodes get NULL cdn
    # and empty qualities; the play-time resolver fills them in live.
    episodes_to_add: list[tuple] = []
    skipped_no_episode_marker = 0
    for vid, raw_title, parsed in fresh:
        if not parsed.is_episode:
            skipped_no_episode_marker += 1
            log.debug("[%s] skip vid=%d no SxxExx in %r", label, vid, raw_title)
            continue
        has_dub, has_subs = _langs_to_flags(parsed.langs)
        episodes_to_add.append((
            parsed.season, parsed.episode, vid,
            None, [],          # cdn / qualities — left for play-time resolver
            has_dub, has_subs,
        ))
    log.info("[%s] %d episodes have SxxExx and ready to upsert "
             "(skipped %d without episode markers)",
             label, len(episodes_to_add), skipped_no_episode_marker)
    if not episodes_to_add:
        return {"target": label, "status": "no_episodes_with_marker",
                "found": len(found), "fresh": len(fresh),
                "imported": 0, "failed": skipped_no_episode_marker}

    if dry_run:
        log.info("[%s] DRY RUN — would upsert %d episodes via process_series_batch",
                 label, len(episodes_to_add))
        return {"target": label, "status": "dry_run_ok",
                "found": len(found), "fresh": len(fresh),
                "imported": len(episodes_to_add), "failed": 0,
                "tmdb_id": tv.tmdb_id, "imdb_id": tv.imdb_id}

    try:
        results = process_series_batch(
            conn, tv=tv,
            episodes_to_add=episodes_to_add,
            cover_dir=str(series_covers),
        )
    except Exception as exc:
        conn.rollback()
        log.exception("[%s] process_series_batch crashed", label)
        return {"target": label, "status": f"crash:{exc}",
                "found": len(found), "fresh": len(fresh),
                "imported": 0, "failed": len(episodes_to_add)}

    conn.commit()
    added = sum(1 for a, _, _, _ in results if "added" in a)
    updated = sum(1 for a, _, _, _ in results if "updated" in a)
    failed = sum(1 for a, _, _, _ in results if a == "failed")
    log.info("[%s] DONE — added=%d updated=%d failed=%d", label, added, updated, failed)
    return {"target": label, "status": "imported",
            "found": len(found), "fresh": len(fresh),
            "imported": added + updated, "failed": failed,
            "tmdb_id": tv.tmdb_id, "imdb_id": tv.imdb_id}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", default="data/movies/sktorrent-new-series.csv",
                    help="Missing-series CSV from find-sktorrent-new-series.py")
    ap.add_argument("--top", type=int, default=20,
                    help="Process the N highest-episode-count missing series")
    ap.add_argument("--skip", type=int, default=0,
                    help="Skip the first M series (after sort) — useful to "
                         "import positions 21..50 without re-searching 1..20")
    ap.add_argument("--min-episodes", type=int, default=2,
                    help="Skip CSV rows below this episode_count")
    ap.add_argument("--covers-dir", default=os.environ.get(
        "SERIES_COVERS_DIR", "data/series/covers-webp"),
                    help="Where ensure_series writes downloaded series cover")
    ap.add_argument("--dry-run", action="store_true",
                    help="Search + TMDB resolve, but skip DB writes")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    targets = load_targets(Path(args.csv), top=args.top,
                           min_eps=args.min_episodes, skip=args.skip)
    log.info("loaded %d target series from %s (top %d, skip %d, min_eps=%d)",
             len(targets), args.csv, args.top, args.skip, args.min_episodes)
    for i, t in enumerate(targets, 1):
        log.info("  %2d. %-45s eps=%d", i,
                 (t.cz_title or t.en_title)[:45], t.expected_episode_count)

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn and not args.dry_run:
        raise SystemExit("DATABASE_URL is required (use --dry-run for no-DB testing)")
    if not os.environ.get("TMDB_API_KEY"):
        raise SystemExit("TMDB_API_KEY is required")

    conn: psycopg2.extensions.connection | None = None
    if dsn:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False

    # Build "already known" set so we don't redundantly try to upsert episodes
    # whose sktorrent_video_id already lives in DB.
    known_video_ids: set[int] = set()
    if conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT sktorrent_video_id FROM episodes
                   WHERE sktorrent_video_id IS NOT NULL
                   UNION ALL
                   SELECT sktorrent_video_id FROM tv_episodes
                   WHERE sktorrent_video_id IS NOT NULL"""
            )
            known_video_ids = {row[0] for row in cur.fetchall()}
        log.info("DB already knows %d sktorrent episode IDs", len(known_video_ids))

    skt_session = requests.Session()
    skt_session.headers["User-Agent"] = DEFAULT_USER_AGENT
    skt_session.headers["Accept-Encoding"] = "identity"
    tmdb_session = requests.Session()

    series_covers = Path(args.covers_dir)
    series_covers.mkdir(parents=True, exist_ok=True)

    summary: list[dict] = []
    t0 = time.time()
    try:
        for i, target in enumerate(targets, 1):
            log.info("\n>>> [%d/%d] %s",
                     i, len(targets), target.cz_title or target.en_title)
            stats = import_one_series(
                target, conn, series_covers,
                tmdb_session, skt_session, known_video_ids,
                dry_run=args.dry_run,
            )
            summary.append(stats)
            # Pull fresh known_video_ids after each series — multiple targets
            # might share a video_id (rare but possible with crossover specials).
            if conn and not args.dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT sktorrent_video_id FROM episodes "
                        "WHERE sktorrent_video_id IS NOT NULL"
                    )
                    known_video_ids = {row[0] for row in cur.fetchall()}
    finally:
        if conn:
            conn.close()

    elapsed = time.time() - t0
    log.info("=" * 70)
    log.info("Import finished in %.0fs.", elapsed)
    print()
    print("| # | series                          | found | fresh | imported | failed | status |")
    print("|--:|---------------------------------|------:|------:|---------:|-------:|--------|")
    for i, s in enumerate(summary, 1):
        print(f"| {i:2d} | {s['target'][:32]:<32} | "
              f"{s['found']:5d} | {s.get('fresh', 0):5d} | "
              f"{s.get('imported', 0):8d} | {s.get('failed', 0):6d} | "
              f"{s['status']} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
