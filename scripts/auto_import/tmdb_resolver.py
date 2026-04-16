"""Resolve a SK Torrent video to a TMDB record + IMDB ID.

SK Torrent does NOT expose IMDB IDs in HTML, so we infer them from the parsed
title (CZ + EN + year + season/episode). TMDB is queried with progressive
fallbacks (CZ first, then EN, then any year-less retry) and we pick the best
candidate by year-match + popularity.

Three entry points:
    resolve_movie(parsed)         → MovieResolution | None
    resolve_tv(parsed)            → TvResolution | None
    resolve_episode(tv_id, S, E)  → EpisodeResolution | None

The first two require a `ParsedTitle` from `title_parser`. All three return
None when nothing acceptable is found (caller adds to import_skipped_videos).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, asdict
from typing import TYPE_CHECKING

import requests

# title_parser ships in #416 (PR #427). Use TYPE_CHECKING-only import so this
# module is importable on its own (CI runs each PR's branch in isolation, so
# integration only works once all sub-issues land in main). Runtime callers
# pass any object with .cz_title / .en_title / .year / .season / .episode
# attributes — duck-typing keeps the dependency loose.
if TYPE_CHECKING:
    from scripts.auto_import.title_parser import ParsedTitle  # noqa: F401

TMDB_API_BASE = "https://api.themoviedb.org/3"
# Required env var — no inline fallback (GitGuardian flags hardcoded keys).
# Set TMDB_API_KEY in `.env` (loaded by python-dotenv at script entrypoint).
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")

# TMDB allows ~50 req/s but be polite — this is a lookup, not a bulk job.
DEFAULT_TIMEOUT = 15

log = logging.getLogger(__name__)


@dataclass
class MovieResolution:
    tmdb_id: int
    imdb_id: str | None
    title_cs: str | None
    title_en: str | None
    original_title: str | None
    overview_cs: str | None
    overview_en: str | None
    year: int | None
    runtime_min: int | None
    poster_path: str | None        # TMDB path like "/abc.jpg" — caller fetches via image.tmdb.org
    genre_ids: list[int]           # raw TMDB genre ids
    # TMDB's own vote_average on 0–10. Our films table stores imdb_rating
    # (0–10) and TMDB/IMDB usually agree within ±0.5 on popular titles, so
    # we use it as an imdb_rating proxy until we wire in the real IMDB
    # endpoint. Small indie/obscure CZ titles may diverge more — accept
    # that for now.
    vote_average: float | None = None
    popularity: float = 0.0
    raw_search_score: float = 0.0  # how confident we are in the match

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TvResolution:
    tmdb_id: int
    imdb_id: str | None
    name_cs: str | None
    name_en: str | None
    original_name: str | None
    overview_cs: str | None
    overview_en: str | None
    first_air_year: int | None
    last_air_year: int | None
    season_count: int | None
    episode_count: int | None
    poster_path: str | None
    genre_ids: list[int]
    popularity: float = 0.0
    raw_search_score: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EpisodeResolution:
    tmdb_tv_id: int
    season: int
    episode: int
    name: str | None
    overview: str | None
    air_date: str | None
    runtime_min: int | None
    still_path: str | None

    def to_dict(self) -> dict:
        return asdict(self)


def _request(session: requests.Session, path: str, params: dict | None = None,
             retries: int = 2) -> dict | None:
    """GET TMDB endpoint with simple retry. Returns parsed JSON or None."""
    url = f"{TMDB_API_BASE}{path}"
    p = {"api_key": TMDB_API_KEY}
    if params:
        p.update(params)
    for attempt in range(retries + 1):
        try:
            r = session.get(url, params=p, timeout=DEFAULT_TIMEOUT)
        except requests.RequestException as e:
            log.warning("TMDB %s attempt %d failed: %s", path, attempt + 1, e)
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            log.warning("TMDB rate-limited; sleeping %ds", wait)
            time.sleep(wait)
            continue
        if r.status_code != 200:
            log.warning("TMDB %s returned HTTP %d", path, r.status_code)
            return None
        try:
            return r.json()
        except ValueError:
            return None
    return None


def _score_movie(candidate: dict, parsed: ParsedTitle) -> float:
    """Score a TMDB movie candidate against parsed title.

    Higher is better. Year match dominates; popularity breaks ties.
    """
    score = float(candidate.get("popularity") or 0.0)
    cand_year = None
    rd = candidate.get("release_date") or ""
    if len(rd) >= 4 and rd[:4].isdigit():
        cand_year = int(rd[:4])
    if parsed.year and cand_year:
        if cand_year == parsed.year:
            score += 100
        elif abs(cand_year - parsed.year) == 1:
            score += 30  # off by 1 year — TMDB sometimes uses release vs production year
        else:
            score -= 50  # different year — likely different film
    # Title overlap (loose)
    parsed_titles = {(parsed.cz_title or "").lower(), (parsed.en_title or "").lower()}
    parsed_titles.discard("")
    cand_titles = {
        (candidate.get("title") or "").lower(),
        (candidate.get("original_title") or "").lower(),
    }
    if parsed_titles & cand_titles:
        score += 20
    return score


def _score_tv(candidate: dict, parsed: ParsedTitle) -> float:
    score = float(candidate.get("popularity") or 0.0)
    cand_year = None
    fad = candidate.get("first_air_date") or ""
    if len(fad) >= 4 and fad[:4].isdigit():
        cand_year = int(fad[:4])
    if parsed.year and cand_year:
        if cand_year == parsed.year:
            score += 100
        elif abs(cand_year - parsed.year) == 1:
            score += 30
        else:
            score -= 30  # less harsh than film — series often span years
    parsed_titles = {(parsed.cz_title or "").lower(), (parsed.en_title or "").lower()}
    parsed_titles.discard("")
    cand_titles = {
        (candidate.get("name") or "").lower(),
        (candidate.get("original_name") or "").lower(),
    }
    if parsed_titles & cand_titles:
        score += 20
    return score


def _movie_search_queries(parsed: ParsedTitle) -> list[tuple[str, dict]]:
    """Ordered list of (query, extra_params) to try, most specific first."""
    out: list[tuple[str, dict]] = []
    titles = [parsed.cz_title, parsed.en_title]
    titles = [t for t in titles if t]
    for t in titles:
        if parsed.year:
            out.append((t, {"year": parsed.year}))
        out.append((t, {}))
    return out


def _shorten_title_candidates(title: str) -> list[str]:
    """Progressive shortenings for TMDB search fallback.

    Splits on each known separator independently and returns all candidates
    sorted by length descending, so fallbacks are always monotonically
    shorter. For "X - Y: Z" we get ["X - Y: Z", "X - Y", "X"]. Handy for SK
    Torrent titles with marketing noise like "Výměna manželek - nyní pouze
    na Oneplay" where the full string misses on TMDB but the bare show
    name matches.
    """
    candidates: set[str] = {title}
    for sep in (" - ", ": "):
        if sep in title:
            prefix = title.split(sep, 1)[0].strip()
            if prefix:
                candidates.add(prefix)
    return sorted(candidates, key=len, reverse=True)


def _tv_search_queries(parsed: ParsedTitle) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    titles = [parsed.cz_title, parsed.en_title]
    titles = [t for t in titles if t]
    # Expand each title with its shortened variants so shows like
    # "Výměna manželek - nyní pouze na Oneplay" also get tried as the bare
    # "Výměna manželek" (which is what TMDB actually indexes).
    expanded: list[str] = []
    for t in titles:
        for v in _shorten_title_candidates(t):
            if v not in expanded:
                expanded.append(v)
    for t in expanded:
        if parsed.year:
            out.append((t, {"first_air_date_year": parsed.year}))
        out.append((t, {}))
    return out


def _build_movie_resolution(session: requests.Session, candidate: dict, score: float) -> MovieResolution | None:
    """Fetch full /movie/{id} (CS + EN) and return a complete MovieResolution."""
    tmdb_id = candidate.get("id")
    if not tmdb_id:
        return None
    cs = _request(session, f"/movie/{tmdb_id}", {"language": "cs-CZ"})
    en = _request(session, f"/movie/{tmdb_id}", {"language": "en-US"})
    if not cs and not en:
        return None
    src = cs or en or {}
    src_en = en or {}

    rd = (cs or src_en).get("release_date") or ""
    year = int(rd[:4]) if len(rd) >= 4 and rd[:4].isdigit() else None

    # TMDB returns vote_average=0 for brand-new films with no votes yet —
    # treat that as "no rating" to avoid seeding the column with a bogus 0.0
    # that the list page would then display as a legit rating.
    va_raw = src.get("vote_average") or src_en.get("vote_average")
    vote_average = float(va_raw) if va_raw else None

    return MovieResolution(
        tmdb_id=tmdb_id,
        imdb_id=src.get("imdb_id") or src_en.get("imdb_id"),
        title_cs=(cs or {}).get("title"),
        title_en=src_en.get("title") or src.get("title"),
        original_title=src.get("original_title"),
        overview_cs=((cs or {}).get("overview") or "").strip() or None,
        overview_en=(src_en.get("overview") or "").strip() or None,
        year=year,
        runtime_min=src.get("runtime") or src_en.get("runtime"),
        poster_path=src.get("poster_path") or src_en.get("poster_path"),
        genre_ids=[g["id"] for g in (src.get("genres") or src_en.get("genres") or []) if g.get("id")],
        vote_average=vote_average,
        popularity=float(candidate.get("popularity") or 0.0),
        raw_search_score=score,
    )


def _build_tv_resolution(session: requests.Session, candidate: dict, score: float) -> TvResolution | None:
    tmdb_id = candidate.get("id")
    if not tmdb_id:
        return None
    cs = _request(session, f"/tv/{tmdb_id}", {"language": "cs-CZ"})
    en = _request(session, f"/tv/{tmdb_id}", {"language": "en-US"})
    if not cs and not en:
        return None
    src = cs or en or {}
    src_en = en or {}
    # external_ids holds imdb_id for TV
    ext = _request(session, f"/tv/{tmdb_id}/external_ids") or {}

    fad = (cs or src_en).get("first_air_date") or ""
    first_year = int(fad[:4]) if len(fad) >= 4 and fad[:4].isdigit() else None
    lad = (cs or src_en).get("last_air_date") or ""
    last_year = int(lad[:4]) if len(lad) >= 4 and lad[:4].isdigit() else None

    return TvResolution(
        tmdb_id=tmdb_id,
        imdb_id=ext.get("imdb_id"),
        name_cs=(cs or {}).get("name"),
        name_en=src_en.get("name") or src.get("name"),
        original_name=src.get("original_name"),
        overview_cs=((cs or {}).get("overview") or "").strip() or None,
        overview_en=(src_en.get("overview") or "").strip() or None,
        first_air_year=first_year,
        last_air_year=last_year,
        season_count=src.get("number_of_seasons"),
        episode_count=src.get("number_of_episodes"),
        poster_path=src.get("poster_path") or src_en.get("poster_path"),
        genre_ids=[g["id"] for g in (src.get("genres") or src_en.get("genres") or []) if g.get("id")],
        popularity=float(candidate.get("popularity") or 0.0),
        raw_search_score=score,
    )


def resolve_movie(parsed: ParsedTitle, session: requests.Session | None = None) -> MovieResolution | None:
    """Find the best TMDB movie match for the parsed title."""
    if not parsed.cz_title and not parsed.en_title:
        return None
    own_session = session is None
    if session is None:
        session = requests.Session()

    best_candidate: dict | None = None
    best_score = float("-inf")
    seen_ids: set[int] = set()
    try:
        for query, extra in _movie_search_queries(parsed):
            params = {"query": query, "language": "cs-CZ", "include_adult": "false"}
            params.update(extra)
            data = _request(session, "/search/movie", params)
            for cand in (data or {}).get("results") or []:
                if cand.get("id") in seen_ids:
                    continue
                seen_ids.add(cand["id"])
                s = _score_movie(cand, parsed)
                if s > best_score:
                    best_score, best_candidate = s, cand
            if best_candidate and best_score >= 100:
                break  # year matched, stop searching
        if not best_candidate:
            return None
        return _build_movie_resolution(session, best_candidate, best_score)
    finally:
        if own_session:
            session.close()


def resolve_tv(parsed: ParsedTitle, session: requests.Session | None = None) -> TvResolution | None:
    """Find the best TMDB TV-series match for the parsed title."""
    if not parsed.cz_title and not parsed.en_title:
        return None
    own_session = session is None
    if session is None:
        session = requests.Session()

    best_candidate: dict | None = None
    best_score = float("-inf")
    seen_ids: set[int] = set()
    try:
        for query, extra in _tv_search_queries(parsed):
            params = {"query": query, "language": "cs-CZ", "include_adult": "false"}
            params.update(extra)
            data = _request(session, "/search/tv", params)
            for cand in (data or {}).get("results") or []:
                if cand.get("id") in seen_ids:
                    continue
                seen_ids.add(cand["id"])
                s = _score_tv(cand, parsed)
                if s > best_score:
                    best_score, best_candidate = s, cand
            if best_candidate and best_score >= 100:
                break
        if not best_candidate:
            return None
        return _build_tv_resolution(session, best_candidate, best_score)
    finally:
        if own_session:
            session.close()


def resolve_episode(tmdb_tv_id: int, season: int, episode: int,
                    session: requests.Session | None = None) -> EpisodeResolution | None:
    """Fetch metadata for one TV episode."""
    own_session = session is None
    if session is None:
        session = requests.Session()
    try:
        data = _request(session,
                        f"/tv/{tmdb_tv_id}/season/{season}/episode/{episode}",
                        {"language": "cs-CZ"})
        if not data:
            # Retry with EN to at least get name/overview
            data = _request(session,
                            f"/tv/{tmdb_tv_id}/season/{season}/episode/{episode}",
                            {"language": "en-US"})
        if not data:
            return None
        return EpisodeResolution(
            tmdb_tv_id=tmdb_tv_id,
            season=season,
            episode=episode,
            name=(data.get("name") or "").strip() or None,
            overview=(data.get("overview") or "").strip() or None,
            air_date=data.get("air_date") or None,
            runtime_min=data.get("runtime"),
            still_path=data.get("still_path"),
        )
    finally:
        if own_session:
            session.close()


def _cli() -> None:
    """CLI for ad-hoc smoke testing.

    Pass a full SK Torrent title string; we parse and resolve.
    Run via `python -m scripts.auto_import.tmdb_resolver "<title>"` from
    the repo root so the package import path resolves correctly.
    """
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("title", help="SK Torrent title string")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    from scripts.auto_import.title_parser import parse_sktorrent_title
    parsed = parse_sktorrent_title(args.title)
    print(f"Parsed:  {parsed.to_dict()}")
    if parsed.is_episode:
        tv = resolve_tv(parsed)
        print(f"TV:     {tv.to_dict() if tv else None}")
        if tv:
            ep = resolve_episode(tv.tmdb_id, parsed.season, parsed.episode)
            print(f"Episode: {ep.to_dict() if ep else None}")
    else:
        movie = resolve_movie(parsed)
        print(f"Movie:   {movie.to_dict() if movie else None}")


if __name__ == "__main__":
    _cli()
