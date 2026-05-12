"""Search prehraj.to for SERIES content (SxxExx-tagged uploads).

Counterpart to `prehrajto_search.py`, which searches for FILMS and
*rejects* any hit whose title contains SxxExx ("that's an episode, not
the film we're looking for"). This module does the opposite: it *keeps*
SxxExx hits and parses them into (season, episode, base_title) triples
suitable for matching against the `series` / `episodes` schema.

Public API:

    search_episodes(sess, query) -> list[Hit]
        Paginate through prehraj.to's `/hledej/{query}` results, return
        every hit (no filtering — the caller decides which to keep).

    build_query(title, original_title, year) -> str
        Compose a search query. Prefers EN title (uploaders mostly use
        the English release name), falls back to CS, optionally appends
        the year.

    classify_episode_hit(hit, series_aliases) -> EpisodeMatch | None
        Parse the upload title via `parse_prehrajto_episode_title`,
        alias-match the cleaned show name against the DB series row's
        title / original_title (normalized identically on both sides —
        same defense as commit 9d1d1e08d in the sktorrent importer),
        and return an `EpisodeMatch` if accepted, else None.

Both Phase A (enrich existing series) and Phase B (discover new series
from sitemap) use the same `classify_episode_hit` so the alias rules
stay in one place.

Raises `BlockedError` (re-exported from `prehrajto_search`) when the CZ
proxy / prehraj.to returns HTTP non-200 or a suspiciously short body —
callers should abort the run before burning more proxy quota.
"""

from __future__ import annotations

import logging
import time
import unicodedata
import urllib.parse
from dataclasses import dataclass

import requests

from .cz_proxy import proxy_get
from .prehrajto_search import (
    BlockedError,
    Hit,
    MAX_PAGES,
    MIN_BODY_LEN,
    SEARCH_BASE,
    SEARCH_SLEEP_S,
    _detect_max_page,
    detect_lang,
    extract_resolution,
    parse_search_html,
)
from .title_parser import parse_prehrajto_episode_title

log = logging.getLogger(__name__)

__all__ = [
    "BlockedError",
    "Hit",
    "EpisodeMatch",
    "build_query",
    "search_episodes",
    "classify_episode_hit",
    "normalize_alias",
]


@dataclass
class EpisodeMatch:
    """Accepted prehraj.to upload bound to a (season, episode) tuple.

    `clean_title` is the bare show name with SxxExx + release-noise
    stripped (the value that matched against the series aliases).
    `lang_class` is the legacy 7-value enum
    (CZ_DUB | CZ_NATIVE | CZ_SUB | SK_DUB | SK_SUB | EN | UNKNOWN);
    callers pass it straight to `lang_class_to_audio_and_subs`.
    """
    hit: Hit
    season: int
    episode: int
    clean_title: str
    year: int | None
    lang_class: str
    resolution_hint: str | None


def normalize_alias(s: str) -> str:
    """Same normalization rule used inside the sktorrent importer's
    alias filter (commit 9d1d1e08d). Lowercase + strip diacritics +
    alnum-only — symmetric across the parsed prehraj.to title and the
    DB series row's `title` / `original_title` so the filter accepts a
    show whose uploader spelled it slightly differently (punctuation,
    dots, capitalization) but rejects unrelated hits.
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    out = []
    for ch in s.lower():
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def build_query(title: str, original_title: str | None, year: int | None) -> str:
    """Compose the prehraj.to search query for a series.

    Prefers the English title (uploaders mostly use the international
    release name) but falls back to CZ. Year is included when known to
    disambiguate reboots (Yellowstone 2018 vs 2005), even though
    prehraj.to's search ignores parens — the year still helps the
    similarity ranker prefer the right show.
    """
    base = original_title or title or ""
    if year:
        base = f"{base} ({year})"
    # Strip characters that break the URL path. Same scrubbing as the
    # films search (`prehrajto_search.build_query`) — keep the two in
    # sync if either changes.
    bad = "/?#&%"
    cleaned = "".join((" " if ch in bad else ch) for ch in base)
    return " ".join(cleaned.split())


def search_episodes(sess: requests.Session, query: str) -> list[Hit]:
    """Paginate through prehraj.to search results, return every hit.

    Unlike `prehrajto_search.search_prehrajto` (which short-circuits as
    soon as it finds a "strong" film match), we always crawl every page
    up to `MAX_PAGES` — a 10-episode season needs all 10 hits, and
    rejecting any of them silently breaks the alias-match defense.

    Returns hits in the order prehraj.to served them, de-duplicated by
    `external_id`.
    """
    all_hits: list[Hit] = []
    seen: set[str] = set()
    page = 1
    max_page = MAX_PAGES
    while page <= max_page:
        if page == 1:
            url = SEARCH_BASE + urllib.parse.quote(query, safe="")
        else:
            url = (
                SEARCH_BASE + urllib.parse.quote(query, safe="")
                + f"?videoListing-visualPaginator-page={page}"
            )
        r = proxy_get(url, sess, timeout=30)
        if r.status_code != 200:
            log.error("BLOCKED: HTTP %d for query=%r url=%s",
                      r.status_code, query, url)
            raise BlockedError(f"HTTP {r.status_code}")
        body = r.text
        if len(body) < MIN_BODY_LEN:
            log.error("BLOCKED: short body len=%d for query=%r",
                      len(body), query)
            raise BlockedError(f"body too short ({len(body)})")
        if page == 1:
            max_page = _detect_max_page(body)
        page_hits = parse_search_html(body)
        new_hits = 0
        for h in page_hits:
            if h.external_id in seen:
                continue
            seen.add(h.external_id)
            all_hits.append(h)
            new_hits += 1
        if new_hits == 0:
            break
        page += 1
        if page <= max_page:
            time.sleep(SEARCH_SLEEP_S)
    return all_hits


def classify_episode_hit(
    hit: Hit, series_aliases: set[str],
) -> EpisodeMatch | None:
    """Parse + alias-match a single prehraj.to hit. Returns None to skip.

    Skip reasons:
      * Hit has no detectable (season, episode) — it's a film, a
        season-pack, or a multi-episode upload. Out of scope.
      * Cleaned show title doesn't normalize-match ANY entry in
        `series_aliases` — wrong show, even if prehraj.to ranked it
        high (their search ranker is loose; the alias filter is what
        guarantees we don't attach the wrong source to an episode).

    `series_aliases` is the caller's pre-normalized set —
    `{normalize_alias(s.title), normalize_alias(s.original_title)}` —
    so we pay the normalization cost once per series even when looping
    over dozens of hits.
    """
    parsed = parse_prehrajto_episode_title(hit.title)
    if not parsed.is_episode:
        return None
    clean = parsed.cz_title or ""
    if not clean:
        return None
    if normalize_alias(clean) not in series_aliases:
        return None
    return EpisodeMatch(
        hit=hit,
        season=parsed.season,
        episode=parsed.episode,
        clean_title=clean,
        year=parsed.year,
        lang_class=detect_lang(hit.title),
        resolution_hint=extract_resolution(hit.title),
    )
