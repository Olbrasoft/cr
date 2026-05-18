"""Single-row Wikidata + cs.wiki SPARQL lookup for `csfd_id` (epic #730).

The bulk resolver (scripts/resolve-csfd-via-wikidata.py) runs weekly via
systemd timer and clears the backlog. This module is the *immediate*
path used during auto-import: after a new film/series/tv_show row is
INSERTed, the enricher calls `lookup_and_write_csfd` with the freshly
created row's (imdb_id, tmdb_id, title) and we try to fill `csfd_id`
right away, so the row shows the ČSFD badge on its first appearance
in the UI rather than waiting up to a week for the next batch run.

Two-step resolution:
  1. Wikidata SPARQL — IMDb→Q-item→P2529 (csfd_id). Same logic and same
     veto rules as the bulk resolver. TMDB fallback when IMDb missed.
  2. cs.wiki externallinks fallback — when Wikidata returned a Q-item
     with a cs.wiki sitelink but no P2529, fetch the article via the
     MediaWiki API and regex-match `csfd.cz/film/{id}` in its external
     links. Same path as scripts/resolve-csfd-via-cswiki.py, just
     compressed into one SPARQL query (adds `?cswikiTitle` as OPTIONAL)
     so we don't pay for a second round-trip.

The helper is intentionally tolerant: any Wikidata error, network
timeout, sanity-check mismatch, or duplicate entity returns silently
without raising. The weekly resolver will pick the row up on its next
sweep and either resolve it or log it to `csfd_id_resolution_review`,
so we never block the import path on Wikidata availability.

Single-row queries are cheap for Wikidata (~150 ms each) and the
auto-import only adds a few dozen new rows per day, so we don't bother
with batching here — that's the bulk resolver's job.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import unicodedata
from dataclasses import dataclass

import psycopg2
import requests

log = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
SPARQL_TIMEOUT_SECONDS = 15
MEDIAWIKI_ENDPOINT = "https://cs.wikipedia.org/w/api.php"
MEDIAWIKI_TIMEOUT_SECONDS = 10

USER_AGENT = os.environ.get(
    "CR_CSFD_RESOLVER_USER_AGENT",
    "cr-csfd-resolver/0.1 (https://ceskarepublika.wiki; "
    "noreply@ceskarepublika.wiki)",
)

# Maps cr table → Wikidata TMDB property. Films use P4947 (movie ID),
# series + tv_shows both use P4983 (TV ID) — same as the bulk resolver.
_TMDB_PROP = {
    "films": "P4947",
    "series": "P4983",
    "tv_shows": "P4983",
}

# Per-thread session so concurrent enrichers don't share a connection.
# auto-import is single-threaded today but the prehraj.to discover path
# uses a per-cluster thread pool, so play it safe.
_tls = threading.local()


def _session() -> requests.Session:
    s = getattr(_tls, "session", None)
    if s is None:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        _tls.session = s
    return s


# Cyrillic-Latin confusables that show up in some Czech-encoded titles
# (mostly mid-word substitutions of Latin look-alikes with Cyrillic
# code points). Same map as scripts/resolve-csfd-via-cswiki.py — shared
# behaviour so the inline gate doesn't reject titles the bulk resolver
# would accept.
_CONFUSABLES = str.maketrans({
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x",
    "А": "a", "Е": "e", "О": "o", "Р": "p", "С": "c", "У": "y", "Х": "x",
})


def _normalise(s: str | None) -> str:
    if not s:
        return ""
    s = s.translate(_CONFUSABLES).lower()
    decomposed = unicodedata.normalize("NFKD", s)
    no_diacritics = "".join(c for c in decomposed if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", no_diacritics)


def _one_match(label: str | None, n_title: str) -> bool:
    if not label:
        return False
    n_label = _normalise(label)
    if len(n_label) < 3:
        return False
    return n_label == n_title or n_label in n_title or n_title in n_label


def _labels_agree(label_cs: str | None,
                  label_en: str | None,
                  p1476: str | None,
                  alt_cs: list[str],
                  alt_en: list[str],
                  title: str | None) -> bool:
    """Same veto/positive logic as the bulk resolver: labelCs is the
    only signal that can VETO; everything else is positive-only."""
    n_title = _normalise(title)
    if len(n_title) < 3:
        return True
    if label_cs and not _one_match(label_cs, n_title):
        return False
    for candidate in (label_cs, label_en, p1476, *alt_cs, *alt_en):
        if _one_match(candidate, n_title):
            return True
    return not label_cs


def _build_query(*, imdb_id: str | None, tmdb_id: int | None,
                 tmdb_prop: str | None) -> str | None:
    # Adding `?cswikiTitle` as OPTIONAL lets the cs.wiki fallback share
    # this single SPARQL round-trip — no second query needed when P2529
    # is missing but a Czech Wikipedia article exists. Same shape as the
    # bulk extractor in resolve-csfd-via-cswiki.py.
    if imdb_id:
        return f"""
SELECT ?item ?csfd ?labelCs ?labelEn ?p1476 ?cswikiTitle
       (GROUP_CONCAT(DISTINCT ?altCs; separator="|") AS ?altCsList)
       (GROUP_CONCAT(DISTINCT ?altEn; separator="|") AS ?altEnList)
WHERE {{
  ?item wdt:P345 "{imdb_id}" .
  OPTIONAL {{ ?item wdt:P2529 ?csfd . }}
  OPTIONAL {{ ?item rdfs:label ?labelCs . FILTER(LANG(?labelCs) = "cs") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P1476 ?p1476 . }}
  OPTIONAL {{ ?item skos:altLabel ?altCs . FILTER(LANG(?altCs) = "cs") }}
  OPTIONAL {{ ?item skos:altLabel ?altEn . FILTER(LANG(?altEn) = "en") }}
  OPTIONAL {{ ?sitelink schema:about ?item ;
                       schema:isPartOf <https://cs.wikipedia.org/> ;
                       schema:name ?cswikiTitle . }}
}}
GROUP BY ?item ?csfd ?labelCs ?labelEn ?p1476 ?cswikiTitle
"""
    if tmdb_id and tmdb_prop:
        return f"""
SELECT ?item ?csfd ?labelCs ?labelEn ?p1476 ?cswikiTitle
       (GROUP_CONCAT(DISTINCT ?altCs; separator="|") AS ?altCsList)
       (GROUP_CONCAT(DISTINCT ?altEn; separator="|") AS ?altEnList)
WHERE {{
  ?item wdt:{tmdb_prop} "{tmdb_id}" .
  OPTIONAL {{ ?item wdt:P2529 ?csfd . }}
  OPTIONAL {{ ?item rdfs:label ?labelCs . FILTER(LANG(?labelCs) = "cs") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P1476 ?p1476 . }}
  OPTIONAL {{ ?item skos:altLabel ?altCs . FILTER(LANG(?altCs) = "cs") }}
  OPTIONAL {{ ?item skos:altLabel ?altEn . FILTER(LANG(?altEn) = "en") }}
  OPTIONAL {{ ?sitelink schema:about ?item ;
                       schema:isPartOf <https://cs.wikipedia.org/> ;
                       schema:name ?cswikiTitle . }}
}}
GROUP BY ?item ?csfd ?labelCs ?labelEn ?p1476 ?cswikiTitle
"""
    return None


# Outcome of a single Wikidata lookup. `vetoed=True` means Wikidata
# returned a candidate that failed sanity / duplicate / label checks →
# do NOT try fallback paths, the bulk resolver owns this row. A `miss`
# (csfd_id=None, cswiki_title=None, vetoed=False) means no signal at
# all and the next fallback (TMDB path) is legitimate. `cswiki_title`
# carries the Czech Wikipedia article name when Wikidata knows the
# item but has no P2529 — the cs.wiki path can then check externallinks
# for a `csfd.cz/film/{id}` reference.
@dataclass(frozen=True)
class _WdResult:
    csfd_id: int | None = None
    cswiki_title: str | None = None
    vetoed: bool = False


_MISS_RESULT = _WdResult()
_VETOED_RESULT = _WdResult(vetoed=True)


def _query_csfd(*, imdb_id: str | None, tmdb_id: int | None,
                tmdb_prop: str | None, title: str | None) -> _WdResult:
    """Run one SPARQL query. Returns a populated `_WdResult` carrying
    csfd_id and/or cs.wiki article title (or `vetoed=True` when the
    response is ambiguous / label-mismatched). The caller decides
    whether to fall through to a second path."""
    query = _build_query(imdb_id=imdb_id, tmdb_id=tmdb_id, tmdb_prop=tmdb_prop)
    if not query:
        return _MISS_RESULT
    try:
        r = _session().post(
            SPARQL_ENDPOINT,
            data={"query": query, "format": "json"},
            headers={"Accept": "application/sparql-results+json"},
            timeout=SPARQL_TIMEOUT_SECONDS,
        )
    except requests.RequestException as e:
        log.debug("csfd lookup network error for imdb=%s tmdb=%s: %s",
                  imdb_id, tmdb_id, e)
        return _MISS_RESULT
    if r.status_code != 200:
        log.debug("csfd lookup HTTP %s for imdb=%s tmdb=%s",
                  r.status_code, imdb_id, tmdb_id)
        return _MISS_RESULT
    try:
        bindings = r.json().get("results", {}).get("bindings", [])
    except ValueError:
        return _MISS_RESULT
    if not bindings:
        return _MISS_RESULT
    # Detect duplicate Wikidata entities by counting distinct `?item` URIs.
    # GROUP_CONCAT on labels can otherwise collapse two entities into one
    # binding row when their selected scalar fields happen to match — same
    # pre-#732 bug the bulk resolver had.
    distinct_items = {b.get("item", {}).get("value") for b in bindings
                      if b.get("item", {}).get("value")}
    if len(distinct_items) > 1:
        log.info("csfd lookup ambiguous (%d Wikidata entities) for "
                 "imdb=%s tmdb=%s", len(distinct_items), imdb_id, tmdb_id)
        return _VETOED_RESULT
    # Adding `?cswikiTitle` to the SELECT can legitimately produce more
    # than one binding row for a single Wikidata entity (one per
    # sitelink/label combo). Multi-binding for one item is OK only if
    # the meaningful scalars agree across rows — distinct non-empty
    # values of `?csfd` or `?cswikiTitle` would force us to pick one
    # arbitrarily and could silently write an unrelated csfd_id.
    distinct_csfd = {b.get("csfd", {}).get("value") for b in bindings
                     if b.get("csfd", {}).get("value")}
    if len(distinct_csfd) > 1:
        log.info("csfd lookup ambiguous (%d csfd_ids in bindings) for "
                 "imdb=%s tmdb=%s", len(distinct_csfd), imdb_id, tmdb_id)
        return _VETOED_RESULT
    distinct_cswiki = {b.get("cswikiTitle", {}).get("value") for b in bindings
                       if b.get("cswikiTitle", {}).get("value")}
    if len(distinct_cswiki) > 1:
        log.info("csfd lookup ambiguous (%d cs.wiki sitelinks in bindings) "
                 "for imdb=%s tmdb=%s", len(distinct_cswiki),
                 imdb_id, tmdb_id)
        return _VETOED_RESULT
    row = next(
        (b for b in bindings if b.get("cswikiTitle", {}).get("value")),
        bindings[0],
    )
    if not _labels_agree(
        row.get("labelCs", {}).get("value"),
        row.get("labelEn", {}).get("value"),
        row.get("p1476", {}).get("value"),
        [p for p in (row.get("altCsList", {}).get("value") or "").split("|") if p],
        [p for p in (row.get("altEnList", {}).get("value") or "").split("|") if p],
        title,
    ):
        log.info("csfd lookup label-mismatch for imdb=%s tmdb=%s title=%r "
                 "labelCs=%r — skipping, will be flagged on weekly resolver",
                 imdb_id, tmdb_id, title,
                 row.get("labelCs", {}).get("value"))
        return _VETOED_RESULT
    cswiki_title = row.get("cswikiTitle", {}).get("value") or None
    csfd_raw = row.get("csfd", {}).get("value")
    if not csfd_raw:
        # Wikidata knows the item but it has no P2529 yet. If the same
        # Q-item has a cs.wiki article, the caller can try externallinks
        # there; otherwise it's a plain miss that the next path may try.
        return _WdResult(cswiki_title=cswiki_title)
    try:
        csfd_id = int(csfd_raw)
    except ValueError:
        return _VETOED_RESULT
    return _WdResult(csfd_id=csfd_id, cswiki_title=cswiki_title)


# Regex matches both /film/{id}/... and /film/{id}-slug/... shapes
# (legacy ČSFD URLs and the current canonical form).
_CSFD_FILM_RE = re.compile(r"csfd\.cz/film/(\d+)")


def _fetch_cswiki_csfd_id(cswiki_title: str) -> int | None:
    """Fetch external links on the cs.wiki article and return the first
    csfd.cz/film/{id} found. None on any error — Wikipedia being slow
    or returning an unexpected payload must never break the import."""
    try:
        r = _session().get(
            MEDIAWIKI_ENDPOINT,
            params={
                "action": "parse",
                "page": cswiki_title,
                "prop": "externallinks",
                "format": "json",
                "formatversion": "2",
                "redirects": "1",
            },
            headers={"Accept": "application/json"},
            timeout=MEDIAWIKI_TIMEOUT_SECONDS,
        )
    except requests.RequestException as e:
        log.debug("cs-wiki fetch network error for %r: %s", cswiki_title, e)
        return None
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except ValueError:
        return None
    if "error" in data:
        return None
    links = data.get("parse", {}).get("externallinks", []) or []
    for link in links:
        m = _CSFD_FILM_RE.search(link)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                continue
    return None


def _cswiki_title_matches_row(cswiki_title: str,
                              title: str | None,
                              original_title: str | None) -> bool:
    """Defense against pathological Wikidata sitelinks: confirm the
    cs.wiki article title normalised-matches the row's title or
    original_title. Strips a trailing `(film, 1999)`-style disambiguator
    from the article title. If the row provides no title at all, accept
    (Wikidata already passed label-agreement, so this is best-effort)."""
    cs_norm = _normalise(re.sub(r"\s*\([^)]*\)\s*$", "", cswiki_title))
    if not cs_norm:
        return False
    t_norm = _normalise(title) if title else ""
    o_norm = _normalise(original_title) if original_title else ""
    if not t_norm and not o_norm:
        return True
    return cs_norm == t_norm or cs_norm == o_norm


def lookup_and_write_csfd(
    conn: psycopg2.extensions.connection,
    *,
    table: str,
    row_id: int,
    imdb_id: str | None,
    tmdb_id: int | None,
    title: str | None,
    original_title: str | None = None,
) -> int | None:
    """Resolve `csfd_id` via Wikidata (with cs.wiki externallinks
    fallback) for a single freshly-inserted row and UPDATE it. Never
    raises — Wikidata or Wikipedia being slow / down / wrong must not
    break the import path. Returns the written csfd_id, or None if
    nothing was written.

    Idempotent: the UPDATE filters on `csfd_id IS NULL`, so calling
    this after a manual fix is a no-op."""
    if table not in _TMDB_PROP:
        log.warning("lookup_and_write_csfd: unsupported table %r", table)
        return None
    if not (imdb_id or tmdb_id):
        return None
    try:
        # Cheap DB-side guard BEFORE the network call. The existing-row
        # callers (enricher's "updated_film", ensure_series's lookup
        # branches) rely on this helper to no-op when csfd_id is already
        # populated. After the bulk backfill that's the common case, so
        # skipping the SPARQL request when the column is non-NULL saves
        # ~150 ms per row AND avoids burning Wikidata quota on rows that
        # cannot be updated.
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT csfd_id FROM {table} WHERE id = %s", (row_id,))
            row = cur.fetchone()
            if row is None or row[0] is not None:
                return None

        # IMDb path is the strict authority — a veto here (label
        # mismatch / duplicate Wikidata entity) means we must NOT fall
        # back to TMDB, because the bulk resolver would mark the row as
        # resolved/rejected and the TMDB path could otherwise paper over
        # a real disagreement by writing a different ID.
        result = _query_csfd(
            imdb_id=imdb_id,
            tmdb_id=tmdb_id,
            tmdb_prop=_TMDB_PROP[table],
            title=title,
        )
        if result.vetoed:
            return None
        csfd_id = result.csfd_id
        cswiki_title = result.cswiki_title
        source = "wikidata" if csfd_id else None
        # If imdb_id wasn't provided, the SPARQL call above already used
        # TMDB so there's no second pass to try.
        tmdb_tried = not imdb_id

        # cs.wiki fallback on the IMDb pass's sitelink. Wikidata knows
        # the item and labels agreed, but P2529 isn't filled in. The
        # Czech Wikipedia article often links to ČSFD in its external-
        # links section even when the property hasn't been mirrored back
        # to Wikidata. Title gate is cheap insurance against a broken
        # sitelink mapping the wrong article to this Q-item.
        if csfd_id is None and cswiki_title:
            if _cswiki_title_matches_row(cswiki_title, title, original_title):
                csfd_id = _fetch_cswiki_csfd_id(cswiki_title)
                if csfd_id is not None:
                    source = "cswiki"
            else:
                log.info("cs-wiki title mismatch for %s.id=%d imdb=%s "
                         "title=%r cswiki=%r — skipping IMDb-pass fallback",
                         table, row_id, imdb_id, title, cswiki_title)

        # Still nothing — try TMDB pass (Wikidata via P4947/P4983), then
        # cs.wiki again if that pass surfaces a different sitelink.
        # Mirrors the bulk resolver's IMDb-first / TMDB-fallback shape
        # and prevents IMDb-pass cs.wiki misses from short-circuiting
        # the TMDB attempt.
        if csfd_id is None and not tmdb_tried and tmdb_id:
            result = _query_csfd(
                imdb_id=None,
                tmdb_id=tmdb_id,
                tmdb_prop=_TMDB_PROP[table],
                title=title,
            )
            if result.vetoed:
                return None
            if result.csfd_id is not None:
                csfd_id = result.csfd_id
                source = "wikidata(tmdb)"
            elif result.cswiki_title and result.cswiki_title != cswiki_title:
                if _cswiki_title_matches_row(
                    result.cswiki_title, title, original_title
                ):
                    csfd_id = _fetch_cswiki_csfd_id(result.cswiki_title)
                    if csfd_id is not None:
                        source = "cswiki(tmdb)"
                else:
                    log.info("cs-wiki title mismatch for %s.id=%d tmdb=%s "
                             "title=%r cswiki=%r — skipping TMDB-pass fallback",
                             table, row_id, tmdb_id, title, result.cswiki_title)

        if csfd_id is None:
            return None
        with conn.cursor() as cur:
            # `csfd_id IS NULL` guards against double-writes if the row
            # was filled by the weekly resolver between INSERT and now.
            # Sibling-collision check uses the same NOT EXISTS shape as
            # the bulk resolver — a different row already owning that
            # csfd_id means the proposal is suspicious; leave it for
            # manual review.
            cur.execute(
                f"UPDATE {table} SET csfd_id = %s "
                f"WHERE id = %s AND csfd_id IS NULL "
                f"  AND NOT EXISTS (SELECT 1 FROM {table} t2 "
                f"                  WHERE t2.csfd_id = %s AND t2.id <> %s)",
                (csfd_id, row_id, csfd_id, row_id),
            )
            if cur.rowcount:
                log.info("csfd lookup wrote csfd_id=%d for %s.id=%d "
                         "(imdb=%s tmdb=%s source=%s)",
                         csfd_id, table, row_id, imdb_id, tmdb_id, source)
                return csfd_id
        return None
    except Exception as e:  # noqa: BLE001 — must never bubble to import path
        log.warning("csfd lookup unexpected error for %s.id=%d "
                    "imdb=%s tmdb=%s: %s",
                    table, row_id, imdb_id, tmdb_id, e)
        return None
