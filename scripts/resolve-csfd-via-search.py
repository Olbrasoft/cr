#!/usr/bin/env python3
"""Resolve missing `csfd_id` by searching csfd.cz and corroborating
the candidate against cr's local credits data (director + cast pulled
from `film_directors` / `film_actors` / `series_*`).

Title+year alone produces many false positives because common titles
("Potopa", "Munich") have several films on ČSFD. The fix is to fetch
ČSFD's detail page for each candidate and require strong overlap with
the credits cr already has — director match + cast overlap. We do NOT
call TMDB at extract time; cr's `people` table covers ~99 % of films
via the existing TMDB ingest.

Pipeline per cr row:
  1. Search ČSFD: csfd.cz/hledat/?q={cr.title or cr.original_title}
  2. Filter candidates by normalised title match against cr.title or
     cr.original_title AND |year diff| ≤ 2.
  3. For each surviving candidate (up to 5), fetch /film/{id}/prehled/
     and pull (directors, actors, origin country, runtime).
  4. Score against cr's local credits:
        +25  director name match (per name)
        +5   top-cast name match (per first 5 cast names, capped at 25)
        +5   runtime within ±10 min
        +5   year exact, +1 year ±1
     The ČSFD origin country is parsed and recorded in `breakdown`
     for audit, but cr doesn't currently store production_countries
     so it contributes 0 points — a later commit can plumb that.
  5. Accept the highest scorer iff score ≥ 30 AND ≥ 2× runner-up.
  6. Otherwise mark needs_review with all candidate scores in
     `details_json` for human triage.

Connection: Playwright over CDP to the workspace's Edge instance
(http://localhost:9226 by default; Edge already has Anubis cookie
solved per CLAUDE.md). Falls back to `EDGE_CDP` env var.

Rate-limit: 1 req/s by default (sleep after each navigation). At 6.6 k
rows × ~2 reqs each ≈ 6 hours total. Run overnight.

Resumable: extract phase appends to data/csfd-search/{table}.tsv and
skips row_ids already present on re-run.

Usage:

    # Phase A — extract proposals
    EDGE_CDP=http://localhost:9226 \\
    DATABASE_URL=postgres://cr:cr@host/cr \\
        python3 scripts/resolve-csfd-via-search.py extract \\
            [--table films|series|tv_shows|all] \\
            [--limit N] \\
            [--sleep 1.5] \\
            [--accept-score 30] \\
            [--margin 2.0]

    # Phase B — apply rows whose gate=accept
    DATABASE_URL=postgres://cr:cr@host/cr \\
        python3 scripts/resolve-csfd-via-search.py apply \\
            [--table films|series|tv_shows|all] \\
            [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
import unicodedata
import urllib.parse
from pathlib import Path

import psycopg2
import psycopg2.extras
from playwright.sync_api import sync_playwright

OUT_DIR = Path("data/csfd-search")
TABLES = ("films", "series", "tv_shows")


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

_CONFUSABLES = str.maketrans({
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x",
    "А": "a", "Е": "e", "О": "o", "Р": "p", "С": "c", "У": "y", "Х": "x",
})


def normalise(s: str | None) -> str:
    if not s:
        return ""
    s = s.translate(_CONFUSABLES).lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", s)


def normalise_name(s: str | None) -> str:
    """For person names: drop diacritics, lowercase, collapse spaces."""
    if not s:
        return ""
    s = s.translate(_CONFUSABLES).lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9 ]", "", s)
    return re.sub(r"\s+", " ", s).strip()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def fetch_rows_with_credits(
    conn,
    table: str,
    limit: int | None,
) -> list[dict]:
    """Returns one dict per cr row missing csfd_id, with director +
    actor names already joined from the local credits tables."""
    year_col = "year" if table == "films" else "first_air_year"
    fk = "film_id" if table == "films" else "series_id"
    # Junction-table names: `film_directors` (drop the plural 's') but
    # `series_directors` — `series` is already singular form. Naïve
    # `table[:-1]` produces `serie_*` which doesn't exist.
    credits_prefix = {
        "films": "film",
        "series": "series",
        "tv_shows": None,
    }[table]
    dir_table = f"{credits_prefix}_directors" if credits_prefix else None
    act_table = f"{credits_prefix}_actors" if credits_prefix else None
    # Only `films` carries `runtime_min`. series + tv_shows don't store
    # a per-row runtime; we substitute NULL so the scorer just skips the
    # runtime check for those tables instead of throwing.
    runtime_expr = "f.runtime_min" if table == "films" else "NULL::smallint"
    # tv_shows currently has no _actors/_directors tables; fall back to NULL.
    if table == "tv_shows":
        sql = (
            f"SELECT id, imdb_id, tmdb_id, title, original_title, "
            f"{year_col} AS year, NULL::smallint AS runtime_min, "
            f"NULL::text AS directors, NULL::text AS actors "
            f"FROM {table} WHERE csfd_id IS NULL "
            f"  AND (title IS NOT NULL OR original_title IS NOT NULL) "
            f"ORDER BY id"
        )
    else:
        sql = f"""
            SELECT f.id, f.imdb_id, f.tmdb_id, f.title, f.original_title,
                   f.{year_col} AS year, {runtime_expr} AS runtime_min,
                   (SELECT string_agg(p.name, '|')
                    FROM {dir_table} d
                    JOIN people p ON p.id = d.person_id
                    WHERE d.{fk} = f.id) AS directors,
                   (SELECT string_agg(p.name, '|' ORDER BY a.order_index)
                    FROM (SELECT person_id, order_index FROM {act_table}
                          WHERE {fk} = f.id
                          ORDER BY order_index LIMIT 15) a
                    JOIN people p ON p.id = a.person_id) AS actors
            FROM {table} f
            WHERE f.csfd_id IS NULL
              AND (f.title IS NOT NULL OR f.original_title IS NOT NULL)
            ORDER BY f.id
        """
    if limit:
        sql += f" LIMIT {limit}"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]


def csfd_id_used_by(conn, table: str, csfd_id: int) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {table} WHERE csfd_id = %s LIMIT 1",
            (csfd_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Playwright scraping
# ---------------------------------------------------------------------------

# Search-results extraction. Returns array of {csfd_id, title, year}.
_SEARCH_JS = r"""
() => {
  const results = [];
  const headers = document.querySelectorAll('header.article-header');
  for (const h of headers) {
    const a = h.querySelector('a[href*="/film/"]');
    if (!a) continue;
    const m = (a.getAttribute('href') || '').match(/\/film\/(\d+)/);
    if (!m) continue;
    const text = h.textContent.replace(/\s+/g, ' ').trim();
    const yearMatch = text.match(/\((\d{4})\)/);
    results.push({
      csfd_id: parseInt(m[1], 10),
      title: a.textContent.trim(),
      year: yearMatch ? parseInt(yearMatch[1], 10) : null,
    });
    if (results.length >= 20) break;
  }
  const hasNoResults = !!document.querySelector('.search-no-results') ||
                       /Žádné výsledky/i.test(document.body.textContent);
  return { results, hasNoResults };
}
"""

# Detail-page metadata extraction. Returns origin string + role groups.
_DETAIL_JS = r"""
() => {
  const result = {};
  const origin = document.querySelector('.origin');
  result.origin_raw = origin ? origin.textContent.replace(/\s+/g, ' ').trim() : null;
  const genres = document.querySelector('.genres');
  result.genres_raw = genres ? genres.textContent.replace(/\s+/g, ' ').trim() : null;
  const roles = {};
  const creators = document.querySelectorAll('.creators div');
  for (const d of creators) {
    const h4 = d.querySelector('h4');
    if (!h4) continue;
    const role = h4.textContent.replace(':', '').trim();
    const names = Array.from(d.querySelectorAll('a')).map(a => a.textContent.trim());
    roles[role] = names;
  }
  result.roles = roles;
  const altNames = Array.from(document.querySelectorAll('.film-names li'))
    .map(li => li.textContent.replace(/\s+/g, ' ').trim());
  result.alt_names = altNames.slice(0, 5);
  return result;
}
"""


# ČSFD country names → ISO 2-letter; only the most common. The
# scoring just needs *any* overlap so we don't need a complete table.
_CSFD_COUNTRY_TO_ISO = {
    "USA": "us", "Spojené státy americké": "us",
    "Velká Británie": "gb", "Anglie": "gb",
    "Česko": "cz", "Československo": "cz", "ČSSR": "cz", "ČR": "cz",
    "Slovensko": "sk",
    "Německo": "de", "NDR": "de", "NSR": "de", "Západní Německo": "de",
    "Francie": "fr",
    "Itálie": "it",
    "Španělsko": "es",
    "Polsko": "pl",
    "Rusko": "ru", "Sovětský svaz": "ru", "SSSR": "ru",
    "Japonsko": "jp",
    "Kanada": "ca",
    "Austrálie": "au",
    "Maďarsko": "hu",
    "Rakousko": "at",
    "Belgie": "be",
    "Nizozemsko": "nl", "Holandsko": "nl",
    "Švédsko": "se",
    "Norsko": "no",
    "Dánsko": "dk",
    "Finsko": "fi",
    "Švýcarsko": "ch",
    "Indie": "in",
    "Brazílie": "br",
    "Argentina": "ar",
    "Mexiko": "mx",
    "Jižní Korea": "kr",
    "Čína": "cn",
    "Hongkong": "hk",
    "Tchaj-wan": "tw",
    "Izrael": "il",
    "Turecko": "tr",
    "Irsko": "ie",
    "Nový Zéland": "nz",
    "Portugalsko": "pt",
    "Řecko": "gr",
    "Rumunsko": "ro",
}


def parse_origin(raw: str | None) -> tuple[list[str], int | None, int | None]:
    """Parse ČSFD `.origin` like 'USA, Francie, Kanada 2023 100 min' →
    (iso-country-list, year, runtime_min)."""
    if not raw:
        return [], None, None
    # Year and runtime get extracted by regex first.
    year_m = re.search(r"\b(19|20)\d{2}\b", raw)
    year = int(year_m.group(0)) if year_m else None
    rt_m = re.search(r"(\d{1,3})\s*min", raw)
    runtime = int(rt_m.group(1)) if rt_m else None
    # Country section is the prefix before the year. ČSFD uses comma
    # or just spaces depending on the template; split on both.
    country_str = raw[: year_m.start()] if year_m else raw
    countries = [c.strip() for c in re.split(r"[,/]+", country_str) if c.strip()]
    iso = [_CSFD_COUNTRY_TO_ISO.get(c) for c in countries]
    iso = [c for c in iso if c]
    return iso, year, runtime


# TMDB ISO codes are uppercased; convert when comparing.
def tmdb_country_iso_lower(name: str) -> str:
    return name.lower()


def _abort_pending_nav(page) -> None:
    """Call window.stop() to abort any lingering navigation. Without this,
    a Playwright goto() timeout leaves the underlying navigation pending,
    and the very next goto() on the same page throws
    "Navigation to … is interrupted by another navigation". This cascades —
    one slow page poisons the entire run. Safe no-op if no nav is pending."""
    try:
        page.evaluate("() => window.stop()")
    except Exception:
        # If the page is in a torn-down state, just ignore — the next goto
        # will reset things.
        pass


class PageHolder:
    """Thin wrapper around a Playwright `Page` that transparently reopens
    a new page if the tab gets closed mid-run.

    Background: long ČSFD search runs sometimes had the script's tab
    closed externally (Edge crash, user closing the wrong tab, network
    flake that tore down the page object). Every subsequent goto() then
    raised "Target page, context or browser has been closed" forever,
    silently turning the run into ~30s/row of pure errors. This holder
    catches that specific failure, creates a fresh page from the same
    browser context, and lets the caller retry once. The context (=Edge
    profile, cookies, Anubis solve) survives, so the new page resumes
    where the old one died."""

    _CLOSED_MARKERS = (
        "Target page, context or browser has been closed",
        "Target closed",
        "Browser has been closed",
    )

    def __init__(self, ctx):
        self.ctx = ctx
        self.page = ctx.new_page()

    def _is_closed_error(self, e: BaseException) -> bool:
        s = str(e)
        return any(m in s for m in self._CLOSED_MARKERS)

    def _reopen(self) -> None:
        logging.warning("page closed externally — reopening")
        try:
            self.page.close()
        except Exception:
            pass
        self.page = self.ctx.new_page()

    def goto(self, url, **kw):
        try:
            return self.page.goto(url, **kw)
        except Exception as e:
            if self._is_closed_error(e):
                self._reopen()
                return self.page.goto(url, **kw)
            raise

    def wait_for_selector(self, *a, **kw):
        return self.page.wait_for_selector(*a, **kw)

    def evaluate(self, *a, **kw):
        # If the page died mid-evaluate, the reopened page has no DOM yet
        # — we can't just re-run the same JS against it (the caller's
        # selector wait_for_selector / goto happened on the OLD page).
        # Returning None to the caller is also broken: search_csfd /
        # fetch_detail expect a dict and immediately `.get()` on it.
        # The honest behaviour is to surface the closure as an exception
        # the existing caller-side try/except already handles, so it
        # logs a single "eval failed" warning and moves to the next row
        # — on which goto() will hit the same closed-page path, reopen
        # the page properly, and resume.
        try:
            return self.page.evaluate(*a, **kw)
        except Exception as e:
            if self._is_closed_error(e):
                self._reopen()
            raise

    def close(self) -> None:
        try:
            self.page.close()
        except Exception:
            pass


def _safe_goto(page, url: str, timeout_ms: int) -> bool:
    """Navigate, swallow TimeoutError, and ensure the page is in a usable
    state for the next call. Returns True on success, False on failure.

    `wait_until='commit'` returns as soon as the response headers arrive
    (much faster than 'domcontentloaded'). We then wait for our selector
    of interest separately, which has its own timeout — meaning a slow
    DOM doesn't block the whole request, but we still wait for the
    elements we'll parse."""
    try:
        page.goto(url, wait_until="commit", timeout=timeout_ms)
        return True
    except Exception as e:
        logging.warning("nav failed for %s: %s", url, str(e).split("\n")[0])
        _abort_pending_nav(page)
        return False


def search_csfd(page, query: str, timeout_ms: int = 30000) -> list[dict] | None:
    url = "https://www.csfd.cz/hledat/?" + urllib.parse.urlencode({"q": query})
    if not _safe_goto(page, url, timeout_ms):
        return None
    try:
        page.wait_for_selector(
            "header.article-header, .search-no-results", timeout=8000
        )
    except Exception:
        # Page committed but didn't render search container. Treat as
        # no_results rather than error — usually means a redirect to a
        # film detail page (single hit), which we don't try to parse here.
        _abort_pending_nav(page)
        return []
    try:
        data = page.evaluate(_SEARCH_JS)
    except Exception as e:
        logging.warning("search eval failed for %r: %s", query,
                        str(e).split("\n")[0])
        return None
    if data.get("hasNoResults") and not data.get("results"):
        return []
    return data.get("results", [])


def fetch_detail(page, csfd_id: int, timeout_ms: int = 30000) -> dict | None:
    url = f"https://www.csfd.cz/film/{csfd_id}/prehled/"
    if not _safe_goto(page, url, timeout_ms):
        return None
    try:
        page.wait_for_selector(".origin, .creators", timeout=8000)
    except Exception:
        # Page committed but no metadata rendered. Skip rather than
        # return garbage — scoring will treat it as detail_fetch_failed.
        _abort_pending_nav(page)
        return None
    try:
        return page.evaluate(_DETAIL_JS)
    except Exception as e:
        logging.warning("detail eval failed for %d: %s", csfd_id,
                        str(e).split("\n")[0])
        return None


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def score_candidate(
    cr_row: dict,
    cand_year: int | None,
    cand_iso: list[str],
    cand_runtime: int | None,
    cand_directors_norm: set[str],
    cand_actors_norm: set[str],
) -> tuple[int, dict]:
    """Returns (score, breakdown_dict).

    cr-side country code is not currently plumbed in — `cr.films` /
    `series` / `tv_shows` don't carry production_countries (TMDB
    exposes it, but we don't store it). The ČSFD-side `cand_iso` is
    still parsed and logged in `breakdown` for the audit trail so a
    later commit can wire a cr_iso source without changing the data
    shape.
    """
    breakdown: dict[str, int | list[str]] = {}
    s = 0

    # Year
    if cr_row.get("year") and cand_year:
        diff = abs(cr_row["year"] - cand_year)
        if diff == 0:
            s += 5
            breakdown["year"] = 5
        elif diff == 1:
            s += 1
            breakdown["year"] = 1
        else:
            breakdown["year"] = 0
    else:
        breakdown["year"] = 0

    # Country signal is currently informational only — see docstring.
    # `cand_iso` is still recorded in the breakdown for audit so the
    # signal can be enabled later without rewriting TSV consumers.
    breakdown["country"] = 0
    breakdown["cand_country"] = list(cand_iso)

    # Runtime
    if cr_row.get("runtime_min") and cand_runtime:
        if abs(cr_row["runtime_min"] - cand_runtime) <= 10:
            s += 5
            breakdown["runtime"] = 5
        else:
            breakdown["runtime"] = 0
    else:
        breakdown["runtime"] = 0

    # Directors — strong signal. Per matched name +25.
    cr_dir = {normalise_name(n) for n in (cr_row.get("directors") or "").split("|") if n}
    matched_dir = sorted(cr_dir & cand_directors_norm)
    s += 25 * len(matched_dir)
    breakdown["directors"] = matched_dir

    # Actors — moderate signal. +5 per name match across the first 5
    # matched names (so the bonus is at most 25). Capping at 5 keeps
    # the docstring's "+5 per name × 5 names = 25" promise honest and
    # makes the breakdown.actors list trivially interpretable in the
    # TSV — what you see is exactly what was scored.
    cr_act = {normalise_name(n) for n in (cr_row.get("actors") or "").split("|") if n}
    matched_act = sorted(cr_act & cand_actors_norm)[:5]
    bonus = 5 * len(matched_act)
    s += bonus
    breakdown["actors"] = matched_act
    breakdown["actors_bonus"] = bonus

    return s, breakdown


# ---------------------------------------------------------------------------
# Extract phase
# ---------------------------------------------------------------------------


HEADER = [
    "row_id", "imdb_id", "tmdb_id", "title", "original_title", "year",
    "runtime_min", "n_candidates", "best_csfd_id", "best_score",
    "best_title", "best_year", "runner_up_score", "match_field",
    "gate", "details_json",
]


def load_processed(path: Path) -> set[int]:
    if not path.exists():
        return set()
    seen = set()
    with path.open() as f:
        for row in csv.DictReader(f, delimiter="\t"):
            try:
                seen.add(int(row["row_id"]))
            except (ValueError, KeyError):
                pass
    return seen


def filter_candidates(cr_row: dict, results: list[dict]) -> list[tuple[dict, str]]:
    """Return up to 5 candidates whose normalised title matches
    cr.title or cr.original_title AND |year diff| ≤ 2."""
    n_title = normalise(cr_row.get("title"))
    n_orig = normalise(cr_row.get("original_title"))
    out: list[tuple[dict, str]] = []
    for r in results:
        n_r = normalise(r["title"])
        if n_title and n_r == n_title:
            field = "title"
        elif n_orig and n_r == n_orig:
            field = "original_title"
        else:
            continue
        if cr_row.get("year") and r.get("year"):
            if abs(cr_row["year"] - r["year"]) > 2:
                continue
        out.append((r, field))
        if len(out) >= 5:
            break
    return out


def extract(args, conn) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    cdp = os.environ.get("EDGE_CDP", "http://localhost:9226")
    tables = TABLES if args.table == "all" else (args.table,)

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(cdp)
        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        # PageHolder reopens transparently if the tab is closed externally.
        page = PageHolder(ctx)
        try:
            for table in tables:
                rows = fetch_rows_with_credits(conn, table, args.limit)
                path = OUT_DIR / f"{table}.tsv"
                seen = load_processed(path)
                logging.info(
                    "table=%s rows=%d already=%d", table, len(rows), len(seen)
                )
                if not rows:
                    continue
                mode = "a" if seen else "w"
                with path.open(mode, newline="", buffering=1) as f:
                    w = csv.writer(f, delimiter="\t")
                    if not seen:
                        w.writerow(HEADER)
                    counters = {"accept": 0, "needs_review": 0,
                                "no_match": 0, "no_results": 0,
                                "error": 0}
                    for i, cr_row in enumerate(rows):
                        if cr_row["id"] in seen:
                            continue
                        if (i + 1) % 50 == 0:
                            logging.info(
                                "table=%s progress=%d/%d %s",
                                table, i + 1, len(rows), counters,
                            )
                        query = cr_row.get("title") or cr_row.get("original_title")
                        if not query:
                            continue
                        results = search_csfd(page, query)
                        if results is None:
                            w.writerow([cr_row["id"], cr_row.get("imdb_id") or "",
                                        cr_row.get("tmdb_id") or "",
                                        cr_row.get("title") or "",
                                        cr_row.get("original_title") or "",
                                        cr_row.get("year") or "",
                                        cr_row.get("runtime_min") or "",
                                        0, "", "", "", "", "", "", "error", ""])
                            counters["error"] += 1
                            time.sleep(args.sleep)
                            continue
                        if not results:
                            w.writerow([cr_row["id"], cr_row.get("imdb_id") or "",
                                        cr_row.get("tmdb_id") or "",
                                        cr_row.get("title") or "",
                                        cr_row.get("original_title") or "",
                                        cr_row.get("year") or "",
                                        cr_row.get("runtime_min") or "",
                                        0, "", "", "", "", "", "", "no_results", ""])
                            counters["no_results"] += 1
                            time.sleep(args.sleep)
                            continue
                        candidates = filter_candidates(cr_row, results)
                        if not candidates:
                            w.writerow([cr_row["id"], cr_row.get("imdb_id") or "",
                                        cr_row.get("tmdb_id") or "",
                                        cr_row.get("title") or "",
                                        cr_row.get("original_title") or "",
                                        cr_row.get("year") or "",
                                        cr_row.get("runtime_min") or "",
                                        len(results), "", "", "", "",
                                        "", "", "no_match", ""])
                            counters["no_match"] += 1
                            time.sleep(args.sleep)
                            continue
                        # Score each candidate.
                        time.sleep(args.sleep)
                        scored: list[dict] = []
                        for cand, field in candidates:
                            detail = fetch_detail(page, cand["csfd_id"])
                            time.sleep(args.sleep)
                            if not detail:
                                scored.append({
                                    "csfd_id": cand["csfd_id"],
                                    "title": cand["title"],
                                    "year": cand["year"],
                                    "score": -1,
                                    "field": field,
                                    "breakdown": {"error": "detail_fetch_failed"},
                                })
                                continue
                            iso, det_year, runtime = parse_origin(detail.get("origin_raw"))
                            roles = detail.get("roles") or {}
                            dir_names = {normalise_name(n) for n in roles.get("Režie", [])}
                            act_names = {normalise_name(n) for n in roles.get("Hrají", [])[:15]}
                            sc, bd = score_candidate(
                                cr_row, cand["year"] or det_year, iso, runtime,
                                dir_names, act_names,
                            )
                            scored.append({
                                "csfd_id": cand["csfd_id"],
                                "title": cand["title"],
                                "year": cand["year"] or det_year,
                                "score": sc,
                                "field": field,
                                "breakdown": bd,
                                "csfd_directors": list(roles.get("Režie", []))[:4],
                                "csfd_actors": list(roles.get("Hrají", []))[:8],
                                "csfd_origin": detail.get("origin_raw"),
                            })
                        scored.sort(key=lambda d: d["score"], reverse=True)
                        best = scored[0]
                        runner = scored[1]["score"] if len(scored) > 1 else 0
                        if (best["score"] >= args.accept_score and
                            (runner <= 0 or best["score"] >= runner * args.margin)):
                            gate = "accept"
                            counters["accept"] += 1
                        else:
                            gate = "needs_review"
                            counters["needs_review"] += 1
                        w.writerow([
                            cr_row["id"], cr_row.get("imdb_id") or "",
                            cr_row.get("tmdb_id") or "",
                            cr_row.get("title") or "",
                            cr_row.get("original_title") or "",
                            cr_row.get("year") or "",
                            cr_row.get("runtime_min") or "",
                            len(candidates), best["csfd_id"], best["score"],
                            best["title"], best["year"] or "", runner,
                            best["field"], gate,
                            json.dumps(scored, ensure_ascii=False),
                        ])
                logging.info("table=%s done: %s", table, counters)
        finally:
            page.close()
            browser.close()


# ---------------------------------------------------------------------------
# Apply phase
# ---------------------------------------------------------------------------


def apply_(args, conn) -> None:
    tables = TABLES if args.table == "all" else (args.table,)
    # Separate `applied` vs `would_apply` so dry-run output is honest:
    # the summary line printed at the end is the same code path for
    # both real and dry runs, and an operator copy-pasting "applied:
    # 2499" should mean rows committed to the DB, not rows that
    # would have been committed if --dry-run weren't set.
    grand = {"applied": 0, "would_apply": 0, "skipped_collision": 0,
             "skipped_needs_review": 0, "skipped_already_set": 0,
             "missing_file": 0}
    for table in tables:
        path = OUT_DIR / f"{table}.tsv"
        if not path.exists():
            grand["missing_file"] += 1
            continue
        with path.open() as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        table_count = 0
        for r in rows:
            if r["gate"] != "accept":
                grand["skipped_needs_review"] += 1
                continue
            row_id = int(r["row_id"])
            csfd = int(r["best_csfd_id"])
            collision = csfd_id_used_by(conn, table, csfd)
            if collision and collision != row_id:
                grand["skipped_collision"] += 1
                continue
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {table} SET csfd_id = %s "
                    f"WHERE id = %s AND csfd_id IS NULL",
                    (csfd, row_id),
                )
                if cur.rowcount == 0:
                    grand["skipped_already_set"] += 1
                    continue
                table_count += 1
        if args.dry_run:
            conn.rollback()
            grand["would_apply"] += table_count
            logging.info("--dry-run %s would apply %d", table, table_count)
        else:
            conn.commit()
            grand["applied"] += table_count
            logging.info("%s applied %d", table, table_count)
    logging.info("summary: %s", grand)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("extract")
    e.add_argument("--table", choices=(*TABLES, "all"), default="all")
    e.add_argument("--limit", type=int, default=None)
    e.add_argument("--sleep", type=float, default=1.5)
    e.add_argument("--accept-score", type=int, default=30,
                   help="Min score to auto-accept (default 30 ≈ director match)")
    e.add_argument("--margin", type=float, default=2.0,
                   help="Best must be ≥ margin × runner-up score (default 2.0)")

    a = sub.add_parser("apply")
    a.add_argument("--table", choices=(*TABLES, "all"), default="all")
    a.add_argument("--dry-run", action="store_true")

    args = p.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL must be set", file=sys.stderr)
        return 2
    conn = psycopg2.connect(dsn)
    try:
        if args.cmd == "extract":
            extract(args, conn)
        elif args.cmd == "apply":
            apply_(args, conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
