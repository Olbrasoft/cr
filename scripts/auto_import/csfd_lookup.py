"""Single-row Wikidata SPARQL lookup for `csfd_id` (epic #730).

The bulk resolver (scripts/resolve-csfd-via-wikidata.py) runs weekly via
systemd timer and clears the backlog. This module is the *immediate*
path used during auto-import: after a new film/series/tv_show row is
INSERTed, the enricher calls `lookup_and_write_csfd` with the freshly
created row's (imdb_id, tmdb_id, title) and we try to fill `csfd_id`
right away, so the row shows the ČSFD badge on its first appearance
in the UI rather than waiting up to a week for the next batch run.

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

import psycopg2
import requests

log = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
SPARQL_TIMEOUT_SECONDS = 15

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


def _normalise(s: str | None) -> str:
    if not s:
        return ""
    decomposed = unicodedata.normalize("NFKD", s.lower())
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
    if imdb_id:
        return f"""
SELECT ?item ?csfd ?labelCs ?labelEn ?p1476
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
}}
GROUP BY ?item ?csfd ?labelCs ?labelEn ?p1476
"""
    if tmdb_id and tmdb_prop:
        return f"""
SELECT ?item ?csfd ?labelCs ?labelEn ?p1476
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
}}
GROUP BY ?item ?csfd ?labelCs ?labelEn ?p1476
"""
    return None


# Tri-state outcome of a single Wikidata lookup. The caller distinguishes
# `miss` (no Wikidata mapping → try fallback path) from `vetoed` (Wikidata
# returned a candidate that failed sanity / duplicate checks → do NOT try
# the fallback, the bulk resolver owns this row). Conflating the two would
# let the TMDB fallback overwrite a row that the IMDb pass explicitly
# rejected on label mismatch.
_MISS = "miss"
_VETOED = "vetoed"


def _query_csfd(*, imdb_id: str | None, tmdb_id: int | None,
                tmdb_prop: str | None, title: str | None) -> int | str:
    """Run one SPARQL query. Returns a csfd_id (int) on success, or one
    of the sentinels `_MISS` / `_VETOED`. The caller decides whether to
    fall through to a second path based on which sentinel comes back."""
    query = _build_query(imdb_id=imdb_id, tmdb_id=tmdb_id, tmdb_prop=tmdb_prop)
    if not query:
        return _MISS
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
        return _MISS
    if r.status_code != 200:
        log.debug("csfd lookup HTTP %s for imdb=%s tmdb=%s",
                  r.status_code, imdb_id, tmdb_id)
        return _MISS
    try:
        bindings = r.json().get("results", {}).get("bindings", [])
    except ValueError:
        return _MISS
    if not bindings:
        return _MISS
    # Detect duplicate Wikidata entities by counting distinct `?item` URIs.
    # GROUP_CONCAT on labels can otherwise collapse two entities into one
    # binding row when their selected scalar fields happen to match — same
    # pre-#732 bug the bulk resolver had.
    distinct_items = {b.get("item", {}).get("value") for b in bindings
                      if b.get("item", {}).get("value")}
    if len(distinct_items) > 1 or len(bindings) > 1:
        # Skip silently and VETO — the bulk resolver logs this to the
        # review queue on its next sweep and the fallback path must not
        # paper over the ambiguity by writing a different ID.
        log.info("csfd lookup ambiguous (%d Wikidata entities) for "
                 "imdb=%s tmdb=%s", len(distinct_items) or len(bindings),
                 imdb_id, tmdb_id)
        return _VETOED
    row = bindings[0]
    csfd_raw = row.get("csfd", {}).get("value")
    if not csfd_raw:
        # Wikidata knows the item but it has no P2529 yet — that's a
        # plain miss, the fallback path is still legitimate.
        return _MISS
    try:
        csfd_id = int(csfd_raw)
    except ValueError:
        return _VETOED
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
        return _VETOED
    return csfd_id


def lookup_and_write_csfd(
    conn: psycopg2.extensions.connection,
    *,
    table: str,
    row_id: int,
    imdb_id: str | None,
    tmdb_id: int | None,
    title: str | None,
) -> int | None:
    """Resolve `csfd_id` via Wikidata for a single freshly-inserted row
    and UPDATE it. Never raises — Wikidata being slow / down / wrong
    must not break the import path. Returns the written csfd_id, or
    None if nothing was written.

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

        # IMDb path is the strict authority — a `_VETOED` here (label
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
        if result == _VETOED:
            return None
        csfd_id: int | None = None
        if isinstance(result, int):
            csfd_id = result
        elif result == _MISS and imdb_id and tmdb_id:
            # Plain miss → try TMDB path. Same two-pass as the bulk
            # resolver (`scripts/resolve-csfd-via-wikidata.py`).
            result = _query_csfd(
                imdb_id=None,
                tmdb_id=tmdb_id,
                tmdb_prop=_TMDB_PROP[table],
                title=title,
            )
            if isinstance(result, int):
                csfd_id = result
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
                         "(imdb=%s tmdb=%s)",
                         csfd_id, table, row_id, imdb_id, tmdb_id)
                return csfd_id
        return None
    except Exception as e:  # noqa: BLE001 — must never bubble to import path
        log.warning("csfd lookup unexpected error for %s.id=%d "
                    "imdb=%s tmdb=%s: %s",
                    table, row_id, imdb_id, tmdb_id, e)
        return None
