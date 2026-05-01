#!/usr/bin/env python3
"""TMDB-ID resolver for prehraj.to unmatched clusters using Gemini for
title extraction.

Why this exists: the regex-based matcher in `import-prehrajto-uploads.py`
saturates at ~10,959 unresolved clusters. The remaining cases need real
NLP — they're a mix of:
  - Aliases ("50 odstínů šedi" vs "Padesát odstínů šedi", "Safírově modrá"
    vs "Modrá jako safír")
  - Czech morphology ("Kralik Peter" vs "Králíček Petr")
  - Typos ("Assasins Creed" vs "Assassin's Creed")
  - sk↔cs diacritic variants ("Doba ľadová" vs "Doba ledová")
  - Wrong years from uploaders, descriptive padding, multi-language titles

Approach: Gemini extracts a canonical title + year from the messy upload
string, TMDB API resolves to a stable `tmdb_id`. If `films.tmdb_id` exists
for that ID we mark the cluster resolved; otherwise the TMDB-ID becomes
input to the #652 auto-import pipeline.

This is a one-pass utility AND a daily cron candidate — the same logic
works for new unmatched clusters that arrive after each sitemap sync.

Usage:
  python3 scripts/resolve-unmatched-via-llm.py [--limit N] [--dry-run]
                                                [--min-uploads K]

Environment:
  DATABASE_URL    Postgres connection (mandatory)
  GEMINI_API_KEY  Google AI Studio key for Gemini (mandatory)
  TMDB_API_KEY    TMDB API key (mandatory)

Output: prints one line per cluster with the resolution outcome:
  RESOLVED   = cluster mapped to existing film_id (DB updated unless dry-run)
  NEW_TMDB   = cluster mapped to TMDB ID not yet in films (candidate for #652)
  NOT_FILM   = Gemini said this isn't a film (TV episode / concert / etc.)
  NO_TMDB    = Gemini extracted a title but TMDB didn't return a hit
  SKIP       = cluster already resolved or invalid
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
from typing import Optional

try:
    import psycopg2
    import psycopg2.extras
    import requests
except ImportError as e:
    print(f"ERROR: missing dep ({e}). apt install python3-psycopg2 python3-requests",
          file=sys.stderr)
    sys.exit(2)


GEMINI_MODEL = "gemma-3-27b-it"
GEMINI_URL = (f"https://generativelanguage.googleapis.com/v1beta/"
              f"models/{GEMINI_MODEL}:generateContent")
TMDB_URL = "https://api.themoviedb.org/3"

# Gemma free tier: ~30 req/min, ~14,400/day on AI Studio. We pace at
# 2 s between calls (≈30/min) to stay under the per-minute ceiling and
# leave room for retry on transient 429s. Gemma 3 27B is fast enough
# (~3-8 s per call) — Gemma 4 26B was too slow due to extra "thinking".
GEMINI_RATE_DELAY_S = 2.0
TMDB_RATE_DELAY_S = 0.05  # TMDB allows 40 req/10s, we stay well under

# Friendlier prompt — Gemma is a chat model and responds well to direct
# requests. We don't tell it about the matcher; we just ask "what film
# is this most likely?" and demand JSON.
#
# We ask for BOTH the original title (the language the film was filmed
# in) and the English title — TMDB can fail on a Czech-only search if
# its database carries a different Czech translation than the one we
# extracted, but the English/original title almost always hits.
# NOTE: This is a string template using `.replace()` for substitution
# rather than `str.format(...)`. Reason: sample_title may itself contain
# `{` or `}` (e.g. "Some.Movie.{2018}.x264") which would crash
# `str.format()` with `KeyError`/`ValueError` mid-run. Substitution via
# `.replace()` is brace-safe at the cost of explicit placeholder names.
PROMPT_TEMPLATE = """\
Below is an uploaded filename or label from a Czech video site. Identify
which film it most likely is — give the canonical title in the original
language AND the international/English title.

If the upload string is clearly NOT a film (TV episode, concert, TV show,
news segment, etc.), set is_film to false. When in doubt, treat it as a
film.

Strip out quality / language markers ("1080p", "CZ dabing", "BRRip",
actor names, file extensions) — those aren't part of the title.

For 2 fields:
- "title" = original-language title as the film was produced
  (e.g. "Doba ledová 4: Země v pohybu" → "Ice Age: Continental Drift",
   "Psia duša" → "A Dog's Purpose",
   "Tom yum goong" → "Tom yum goong" — original Thai romanized).
- "title_en" = the international English title if different from
  `title`, else the same string.

Return JSON only (no prose, no markdown). Schema:
{"is_film": <bool>, "title": "<string or null>", "title_en": "<string or null>", "year": <int or null>, "confidence": "<high|medium|low>"}

Upload string: "__TITLE__"
Hint year (may be wrong): __YEAR__
Hint duration in minutes (may be wrong): __DURATION__
"""


_JSON_RE = re.compile(r"\{[^{}]*\}", re.DOTALL)


def _extract_json(text: str) -> Optional[dict]:
    """Gemma sometimes prefixes JSON with stray prose / markdown fences,
    or echoes the prompt schema example before emitting the real
    response. Try a direct parse first, then iterate over EVERY
    `{...}` match and return the first one that parses as a dict —
    this handles the schema-echo case where the literal example
    `{...}` appears before the actual answer."""
    try:
        parsed = json.loads(text)
        if isinstance(parsed, (dict, list)):
            return parsed
    except ValueError:
        pass
    for m in _JSON_RE.finditer(text):
        try:
            parsed = json.loads(m.group(0))
        except ValueError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _coerce_gemini(raw: Optional[dict]) -> Optional[dict]:
    """Normalize LLM-returned types — Gemma occasionally returns
    string-y values where bools/ints are expected ("false" instead of
    false, "1999" instead of 1999). Coerce conservatively so the
    downstream `is_film==True` truthiness and `int(year) - other_year`
    arithmetic work correctly."""
    if not isinstance(raw, dict):
        return None
    is_film_raw = raw.get("is_film", True)
    if isinstance(is_film_raw, str):
        is_film = is_film_raw.strip().lower() in ("true", "yes", "1")
    else:
        is_film = bool(is_film_raw)
    year_raw = raw.get("year")
    year_int: Optional[int] = None
    if isinstance(year_raw, int):
        year_int = year_raw
    elif isinstance(year_raw, str):
        try:
            year_int = int(year_raw.strip())
        except ValueError:
            year_int = None
    title = raw.get("title")
    title_en = raw.get("title_en")
    return {
        "is_film": is_film,
        "title": title if isinstance(title, str) and title.strip() else None,
        "title_en": title_en if isinstance(title_en, str) and title_en.strip() else None,
        "year": year_int,
        "confidence": raw.get("confidence"),
    }


def call_gemini(session: requests.Session, api_key: str, sample_title: str,
                year: Optional[int], duration: Optional[int]) -> Optional[dict]:
    """Return the parsed (and type-coerced) JSON response from the LLM,
    or None on failure.

    Retries once on transient 429 / 5xx with a 5-second backoff. Gemma
    sometimes wraps JSON in prose despite the request, so we also fall
    back to regex extraction.
    """
    prompt = (PROMPT_TEMPLATE
              .replace("__TITLE__", sample_title.replace('"', "'")[:300])
              .replace("__YEAR__", str(year) if year is not None else "unknown")
              .replace("__DURATION__", str(duration) if duration is not None else "unknown"))
    # Gemma models don't support `responseMimeType: application/json`,
    # so we just rely on the prompt to enforce JSON-only output and
    # use `_extract_json` (regex fallback) on the response.
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
        },
    }
    for attempt in (1, 2):
        try:
            r = session.post(
                f"{GEMINI_URL}?key={api_key}",
                json=body, timeout=60,
            )
        except requests.RequestException as e:
            print(f"  GEMINI_ERR (try {attempt}): {type(e).__name__}",
                  file=sys.stderr)
            if attempt == 2:
                return None
            time.sleep(5)
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            print(f"  GEMINI_HTTP_{r.status_code} (try {attempt}, retry in 5s)",
                  file=sys.stderr)
            if attempt == 2:
                return None
            time.sleep(5)
            continue
        if r.status_code != 200:
            print(f"  GEMINI_HTTP_{r.status_code}: {r.text[:200]}",
                  file=sys.stderr)
            return None
        try:
            d = r.json()
            text = d["candidates"][0]["content"]["parts"][-1]["text"]
        except (KeyError, IndexError, ValueError) as e:
            print(f"  GEMINI_RESP: {type(e).__name__}", file=sys.stderr)
            return None
        return _coerce_gemini(_extract_json(text))
    return None


def _tmdb_get(session: requests.Session, url: str) -> Optional[list]:
    try:
        r = session.get(url, timeout=15)
    except requests.RequestException as e:
        print(f"  TMDB_ERR: {type(e).__name__}", file=sys.stderr)
        return None
    if r.status_code != 200:
        print(f"  TMDB_HTTP_{r.status_code}: {r.text[:120]}", file=sys.stderr)
        return None
    try:
        return r.json().get("results", [])
    except ValueError:
        print("  TMDB_JSON_PARSE_ERR", file=sys.stderr)
        return None


def search_tmdb(session: requests.Session, api_key: str, title: str,
                year: Optional[int]) -> Optional[dict]:
    """Return the top TMDB result matching `title` + `year`, or None.

    Tries primary_release_year first (strict), then year (broader),
    then no-year (last resort) to cope with TMDB sometimes filing
    the release under a different year than the canonical one.
    """
    q = urllib.parse.quote(title[:200])
    base = f"{TMDB_URL}/search/movie?api_key={api_key}&query={q}&language=cs-CZ"

    queries = []
    if year is not None:
        queries.append(f"{base}&primary_release_year={year}")
        queries.append(f"{base}&year={year}")
    queries.append(base)

    for url in queries:
        results = _tmdb_get(session, url)
        if not results:
            continue
        if year is not None:
            narrowed = [r for r in results
                        if r.get("release_date", "")[:4]
                        and abs(int(r["release_date"][:4]) - year) <= 2]
            if narrowed:
                results = narrowed
        return max(results, key=lambda r: r.get("popularity", 0))
    return None


def fetch_tmdb_runtime(session: requests.Session, api_key: str,
                       tmdb_id: int) -> Optional[int]:
    """Return runtime in minutes (or None)."""
    try:
        r = session.get(
            f"{TMDB_URL}/movie/{tmdb_id}",
            params={"api_key": api_key, "language": "en-US"},
            timeout=15,
        )
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    rt = r.json().get("runtime")
    return int(rt) if rt else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=30,
                    help="Process at most N clusters (default 30, smoke-test mode)")
    ap.add_argument("--min-uploads", type=int, default=1,
                    help="Only process clusters with ≥ this many uploads (default 1)")
    ap.add_argument("--retry-after-days", type=int, default=7,
                    help="Skip clusters whose `last_attempt_at` is newer than "
                         "this many days (default 7). Avoids burning Gemma "
                         "quota on the same backlog every day. Pass 0 to "
                         "force re-processing of everything.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show resolutions but DO NOT update the registry")
    args = ap.parse_args()

    dsn = os.environ.get("DATABASE_URL", "").strip()
    gemini_key = os.environ.get("GEMINI_API_KEY", "").strip()
    tmdb_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not (dsn and gemini_key and tmdb_key):
        print("ERROR: DATABASE_URL, GEMINI_API_KEY, TMDB_API_KEY all required",
              file=sys.stderr)
        return 2

    # One connection-pooled `Session` reused across all Gemini + TMDB
    # calls. With 30+ requests per cluster, fresh sessions per call
    # add measurable TLS-handshake overhead and break HTTP keep-alive.
    http = requests.Session()
    http.headers.update({"User-Agent": "ceskarepublika.wiki llm-resolver"})

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    # Skip-window: don't reprocess clusters we (the LLM resolver, NOT
    # the regex importer) already touched within the last
    # `--retry-after-days` days. The importer also writes `last_attempt_at`
    # on every nightly sync — that's a different signal and must NOT
    # block this resolver. We tell the two sources apart by the
    # `last_failure_reason` prefix: the importer writes "no films match
    # for cluster key (importer skip)"; this resolver writes codes
    # starting with `llm_`, `tmdb_`, or the literal `awaiting_film_import`
    # for NEW_TMDB candidates.
    resolver_reasons = (
        "llm_gemini_failed", "llm_bad_shape", "llm_not_film", "llm_no_title",
        "tmdb_no_hit", "tmdb_runtime_mismatch", "awaiting_film_import",
    )
    cur.execute("""
        SELECT id, sample_title, year, duration_bucket * 3 AS dur_min, upload_count
          FROM prehrajto_unmatched_clusters
         WHERE resolved_at IS NULL
           AND sample_title IS NOT NULL
           AND upload_count >= %s
           AND NOT (last_failure_reason = ANY(%s)
                    AND last_attempt_at IS NOT NULL
                    AND last_attempt_at >= NOW() - %s::interval)
         ORDER BY upload_count DESC, id ASC
         LIMIT %s
    """, (args.min_uploads, list(resolver_reasons),
          f"{args.retry_after_days} days", args.limit))
    rows = cur.fetchall()
    print(f"Loaded {len(rows)} unresolved clusters "
          f"(retry-after-days={args.retry_after_days})", flush=True)

    counters = {k: 0 for k in
                ("RESOLVED", "NEW_TMDB", "NOT_FILM", "NO_TMDB", "SKIP")}
    new_tmdb_candidates: list[dict] = []

    # Pre-fetch existing tmdb_id → film_id mapping
    cur.execute("SELECT tmdb_id, id FROM films WHERE tmdb_id IS NOT NULL")
    tmdb_to_film = {tmdb: fid for tmdb, fid in cur.fetchall()}
    print(f"DB maps {len(tmdb_to_film)} TMDB IDs → film_ids", flush=True)
    # Commit the read transaction so the per-row update transactions
    # below don't pile up locks behind a long-held SELECT snapshot.
    conn.commit()

    def _record_attempt(rid: int, *, reason: Optional[str],
                        film_id: Optional[int] = None,
                        tmdb_id: Optional[int] = None) -> None:
        """Persist one resolver attempt. Always bumps `attempt_count` +
        `last_attempt_at`; sets `resolved_at` + `resolved_film_id`
        when the cluster maps to an existing film, or
        `resolved_tmdb_id` (without `resolved_at`) when only the TMDB
        ID is known. Each attempt commits in its own transaction so
        long runs don't hold registry locks for the dashboard."""
        if args.dry_run:
            return
        if film_id is not None:
            cur.execute("""
                UPDATE prehrajto_unmatched_clusters
                   SET resolved_at         = NOW(),
                       resolved_film_id    = %s,
                       resolved_tmdb_id    = %s,
                       last_attempt_at     = NOW(),
                       attempt_count       = attempt_count + 1,
                       last_failure_reason = NULL
                 WHERE id = %s AND resolved_at IS NULL
            """, (film_id, tmdb_id, rid))
        elif tmdb_id is not None:
            cur.execute("""
                UPDATE prehrajto_unmatched_clusters
                   SET resolved_tmdb_id    = %s,
                       last_attempt_at     = NOW(),
                       attempt_count       = attempt_count + 1,
                       last_failure_reason = %s
                 WHERE id = %s AND resolved_at IS NULL
            """, (tmdb_id, reason, rid))
        else:
            cur.execute("""
                UPDATE prehrajto_unmatched_clusters
                   SET last_attempt_at     = NOW(),
                       attempt_count       = attempt_count + 1,
                       last_failure_reason = %s
                 WHERE id = %s AND resolved_at IS NULL
            """, (reason, rid))
        conn.commit()

    for i, (rid, sample_title, year, dur_min, upload_count) in enumerate(rows, 1):
        if not sample_title:
            counters["SKIP"] += 1
            continue

        gem = call_gemini(http, gemini_key, sample_title, year, dur_min)
        time.sleep(GEMINI_RATE_DELAY_S)
        if gem is None:
            counters["SKIP"] += 1
            _record_attempt(rid, reason="llm_gemini_failed")
            print(f"[{i:>3}] SKIP    (gemini failed)  {sample_title[:80]}",
                  flush=True)
            continue
        # Some prompt outputs come back as a list of objects (model
        # mistake) — pick the first dict.
        if isinstance(gem, list):
            gem = next((x for x in gem if isinstance(x, dict)), None)
        if not isinstance(gem, dict):
            counters["SKIP"] += 1
            _record_attempt(rid, reason="llm_bad_shape")
            print(f"[{i:>3}] SKIP    (gemini bad shape)  {sample_title[:80]}",
                  flush=True)
            continue

        if not gem.get("is_film", True):
            counters["NOT_FILM"] += 1
            _record_attempt(rid, reason="llm_not_film")
            print(f"[{i:>3}] NOT_FILM   {sample_title[:80]}", flush=True)
            continue

        title_extr = gem.get("title")
        title_en = gem.get("title_en")
        year_extr = gem.get("year") or year
        if not title_extr and not title_en:
            counters["NO_TMDB"] += 1
            _record_attempt(rid, reason="llm_no_title")
            print(f"[{i:>3}] NO_TMDB    {sample_title[:80]} (gemini gave no title)",
                  flush=True)
            continue

        # Try original title first; if no hit, fall back to English
        # title (Gemma's most-internationally-recognizable form).
        tmdb_hit = None
        for candidate in (title_extr, title_en):
            if not candidate:
                continue
            tmdb_hit = search_tmdb(http, tmdb_key, candidate, year_extr)
            time.sleep(TMDB_RATE_DELAY_S)
            if tmdb_hit:
                break
        if not tmdb_hit:
            counters["NO_TMDB"] += 1
            _record_attempt(rid, reason="tmdb_no_hit")
            print(f"[{i:>3}] NO_TMDB    {sample_title[:80]} → "
                  f"gemini='{title_extr}' year={year_extr}",
                  flush=True)
            continue

        tmdb_id = tmdb_hit["id"]
        tmdb_title = tmdb_hit.get("title")
        tmdb_year = tmdb_hit.get("release_date", "????")[:4]

        # Runtime sanity check — drop matches where TMDB's runtime
        # differs from the cluster's reported duration by more than
        # 30 min. Gemma occasionally extracts an ambiguous title
        # ("To" 1990 horror could match "Coming to America" 1988
        # comedy by name, but their durations are 187 vs 117 min).
        # Sole title agreement isn't enough; the duration anchor
        # must also coincide.
        if dur_min and dur_min > 0:
            tmdb_runtime = fetch_tmdb_runtime(http, tmdb_key, tmdb_id)
            time.sleep(TMDB_RATE_DELAY_S)
            if tmdb_runtime and abs(tmdb_runtime - dur_min) > 30:
                counters["NO_TMDB"] += 1
                _record_attempt(rid, reason="tmdb_runtime_mismatch")
                print(f"[{i:>3}] NO_TMDB    {sample_title[:80]} → "
                      f"tmdb={tmdb_id} '{tmdb_title}' {tmdb_year} "
                      f"runtime={tmdb_runtime} ≠ cluster={dur_min} (rejected)",
                      flush=True)
                continue

        existing_film_id = tmdb_to_film.get(tmdb_id)
        if existing_film_id:
            counters["RESOLVED"] += 1
            _record_attempt(rid, reason=None,
                            film_id=existing_film_id, tmdb_id=tmdb_id)
            print(f"[{i:>3}] RESOLVED   {sample_title[:60]} → "
                  f"film_id={existing_film_id} tmdb={tmdb_id} '{tmdb_title}' {tmdb_year}",
                  flush=True)
        else:
            counters["NEW_TMDB"] += 1
            _record_attempt(rid, reason="awaiting_film_import",
                            tmdb_id=tmdb_id)
            new_tmdb_candidates.append({
                "registry_id": rid,
                "tmdb_id": tmdb_id,
                "tmdb_title": tmdb_title,
                "tmdb_year": tmdb_year,
                "uploads": upload_count,
                "sample_title": sample_title,
            })
            print(f"[{i:>3}] NEW_TMDB   {sample_title[:60]} → "
                  f"tmdb={tmdb_id} '{tmdb_title}' {tmdb_year} (NOT in films)",
                  flush=True)

    if args.dry_run:
        print("DRY-RUN: no DB changes committed", flush=True)

    print()
    print("=== Summary ===")
    for k, v in counters.items():
        print(f"  {k:<10} {v}")
    if new_tmdb_candidates:
        print()
        print(f"=== {len(new_tmdb_candidates)} TMDB IDs candidate for #652 auto-import ===")
        for c in new_tmdb_candidates[:50]:
            print(f"  tmdb={c['tmdb_id']:>7}  '{c['tmdb_title']}' {c['tmdb_year']}  "
                  f"({c['uploads']} uploads) ← {c['sample_title'][:60]}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
