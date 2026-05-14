#!/usr/bin/env python3
"""Resolve `csfd_id` for cr.{films,series,tv_shows} via Wikidata SPARQL
(epic #730, sub-issue #732, reconcile pass #740).

Three modes:

  * default                  — fill NULL csfd_id from Wikidata (#732).
  * --reconcile              — walk rows that ALREADY have a csfd_id
                                and queue every disagreement with
                                Wikidata into csfd_id_reconcile_review
                                (#740). No source-table writes.
  * --apply-safe-rewrites    — pure DB pass: walk pending_review rows
                                in csfd_id_reconcile_review, UPDATE
                                source rows where labelCs ≈ cr.title
                                after normalisation. Reversible from
                                the audit log alone.

Background: ČSFD has no public API, no IMDb-search, and TMDB does not
expose ČSFD in its external_ids payload. Wikidata is the only public
open dataset that cross-references IMDb (P345) ↔ TMDB movie (P4947) ↔
TMDB TV (P4983) ↔ ČSFD (P2529). This script reads cr rows that lack a
`csfd_id` and asks Wikidata in batches.

Two passes per table:

  1. Primary  — IMDb path. For rows with `imdb_id IS NOT NULL`, batch
     ~200 tt-IDs into a VALUES clause and query `?film wdt:P345 ?imdb`
     + OPTIONAL P2529. The IMDb namespace in Wikidata is unified
     across movies and TV, so this works for all three tables.

  2. Fallback — TMDB path. For rows that the primary pass could not
     resolve AND have `tmdb_id IS NOT NULL`, query `wdt:P4947` (films)
     or `wdt:P4983` (series, tv_shows). Bare INTEGER `tmdb_id` in cr
     doesn't encode movie-vs-TV so the source table picks the right
     property.

Sanity check: when Wikidata returns a Czech `?labelCs` we compare it to
the cr row's title (normalised — lowercase, diacritics stripped,
non-alphanumerics removed). On a strong mismatch we DO NOT write the
csfd_id; instead we log the (cr_id, wikidata_qid, proposed_csfd_id,
cr_title, wikidata_label_cs) tuple to `csfd_id_resolution_review` for
manual triage. When labelCs is missing we accept the proposal — Wikidata
has only an IMDb→item match for that QID, which is itself a strong
positive signal.

Operational: writes a `csfd_id_resolution_runs` row at start (`status=
running`), updates it at finish with counters + status (`ok`, `error`,
or `partial`). First prod run MUST use --dry-run; the run row is tagged
`dry_run = TRUE` and review entries that the dry-run produces are
inserted into `csfd_id_resolution_review` linked back to that run, so
the maintainer can inspect proposed writes (and the rejection queue) via
`SELECT … WHERE run_id = <the dry-run id>`. After review, re-run without
--dry-run to commit. A weekly systemd timer keeps the columns up-to-date
as new rows land via auto-import.

Usage:
    DATABASE_URL=postgres://... \\
        python3 scripts/resolve-csfd-via-wikidata.py \\
            [--table films|series|tv_shows|all] \\
            [--limit N] \\
            [--batch-size 200] \\
            [--dry-run] \\
            [--reconcile | --apply-safe-rewrites]
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import os
import re
import sys
import time
import unicodedata
from typing import Iterable

import psycopg2
import psycopg2.extras
import requests

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Wikidata's user-agent policy: identify the operator + a contact URL.
# Bare `python-requests/2.x` gets aggressive 429s. Operators can override
# the contact via `CR_CSFD_RESOLVER_USER_AGENT` in /opt/cr/.env if they
# want their own e-mail on the trail; the default uses a project mailbox
# so we don't publish a personal address from this repo.
USER_AGENT = os.environ.get(
    "CR_CSFD_RESOLVER_USER_AGENT",
    "cr-csfd-resolver/0.1 (https://ceskarepublika.wiki; "
    "noreply@ceskarepublika.wiki)",
)

DEFAULT_BATCH_SIZE = 200
# Wikidata normally allows ~60 req/min on the public endpoint. During
# the active wdqs outage (May 2026) it is throttled to 1 req/min. We
# start at 1 s between batches and back off exponentially on 429.
BASE_SLEEP_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 300.0
SPARQL_TIMEOUT_SECONDS = 60

# Maps a cr table name → (TMDB Wikidata property, human label).
# Films use the *movie* TMDB property; series + tv_shows both use the
# *TV* TMDB property (cr keeps them in separate tables but TMDB itself
# doesn't distinguish — same /tv/ namespace).
TMDB_WIKIDATA_PROP = {
    "films": "P4947",
    "series": "P4983",
    "tv_shows": "P4983",
}

# `films` stores release year in `year`; `series` / `tv_shows` use
# `first_air_year`. Resolver doesn't act on the value yet (the sanity
# check is title-only), but it's logged into the review table so a
# maintainer can spot year mismatches at a glance.
YEAR_COL = {
    "films": "year",
    "series": "first_air_year",
    "tv_shows": "first_air_year",
}


def _normalise(s: str | None) -> str:
    """Lowercase, strip Czech diacritics, strip non-alphanumerics. Used
    to compare Wikidata's `?labelCs` to `cr.{title}` without tripping on
    formatting differences."""
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


def _label_matches(labels: list[str | None], title: str | None) -> bool:
    """Decide whether Wikidata's labels agree with `cr.title`.

    `labels[0]` is `labelCs` (the only signal allowed to VETO — Czech
    is the language we serve in, so a present Czech label that
    disagrees is a strong negative). The rest (labelEn, P1476,
    altLabels) are positive signals only: any single match anywhere
    is enough to accept. We do NOT veto on English/original mismatch
    because cr.title is often a legitimate Czech translation that
    Wikidata simply doesn't carry as `labelCs`."""
    n_title = _normalise(title)
    if len(n_title) < 3:
        return True
    label_cs = labels[0] if labels else None
    # Veto gate FIRST. A present labelCs that disagrees rejects the
    # mapping outright — a later positive hit on labelEn / aliases must
    # not override it, otherwise an English re-use of the same title
    # (e.g. Czech "Hrdinové" matching the English label of a different
    # show) silently writes the wrong csfd_id.
    if label_cs and not _one_match(label_cs, n_title):
        return False
    # Positive: any label matches → accept.
    for lbl in labels:
        if _one_match(lbl, n_title):
            return True
    # Negative gate: only labelCs vetoes. If absent, accept on faith
    # (the IMDb→item match is itself strong evidence).
    return not label_cs


def _sparql_query(session: requests.Session, query: str) -> dict:
    """POST a SPARQL query, retrying on 429 with exponential backoff."""
    backoff = BASE_SLEEP_SECONDS
    while True:
        r = session.post(
            SPARQL_ENDPOINT,
            data={"query": query, "format": "json"},
            headers={"Accept": "application/sparql-results+json"},
            timeout=SPARQL_TIMEOUT_SECONDS,
        )
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
            wait = min(wait, MAX_BACKOFF_SECONDS)
            logging.warning("Wikidata 429 — sleeping %.0f s then retrying", wait)
            time.sleep(wait)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            continue
        if r.status_code >= 500:
            wait = min(backoff, MAX_BACKOFF_SECONDS)
            logging.warning("Wikidata %d — sleeping %.0f s then retrying",
                            r.status_code, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            continue
        r.raise_for_status()


def _build_imdb_query(imdb_ids: list[str]) -> str:
    values = " ".join(f'"{i}"' for i in imdb_ids)
    # We fetch Czech + English labels and aliases plus the "original
    # title" (P1476). cr.title can be in any of those, so the sanity
    # check needs all four signals to avoid flagging legit translations
    # as mismatches. GROUP_CONCAT keeps the result one row per IMDb ID.
    return f"""
SELECT ?imdb ?csfd ?item ?labelCs ?labelEn ?p1476
       (GROUP_CONCAT(DISTINCT ?altCs; separator="|") AS ?altCsList)
       (GROUP_CONCAT(DISTINCT ?altEn; separator="|") AS ?altEnList)
WHERE {{
  VALUES ?imdb {{ {values} }}
  ?item wdt:P345 ?imdb .
  OPTIONAL {{ ?item wdt:P2529 ?csfd . }}
  OPTIONAL {{ ?item rdfs:label ?labelCs . FILTER(LANG(?labelCs) = "cs") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P1476 ?p1476 . }}
  OPTIONAL {{ ?item skos:altLabel ?altCs . FILTER(LANG(?altCs) = "cs") }}
  OPTIONAL {{ ?item skos:altLabel ?altEn . FILTER(LANG(?altEn) = "en") }}
}}
GROUP BY ?imdb ?csfd ?item ?labelCs ?labelEn ?p1476
"""


def _build_tmdb_query(tmdb_ids: list[int], prop: str) -> str:
    values = " ".join(f'"{i}"' for i in tmdb_ids)
    return f"""
SELECT ?tmdb ?csfd ?item ?labelCs ?labelEn ?p1476
       (GROUP_CONCAT(DISTINCT ?altCs; separator="|") AS ?altCsList)
       (GROUP_CONCAT(DISTINCT ?altEn; separator="|") AS ?altEnList)
WHERE {{
  VALUES ?tmdb {{ {values} }}
  ?item wdt:{prop} ?tmdb .
  OPTIONAL {{ ?item wdt:P2529 ?csfd . }}
  OPTIONAL {{ ?item rdfs:label ?labelCs . FILTER(LANG(?labelCs) = "cs") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P1476 ?p1476 . }}
  OPTIONAL {{ ?item skos:altLabel ?altCs . FILTER(LANG(?altCs) = "cs") }}
  OPTIONAL {{ ?item skos:altLabel ?altEn . FILTER(LANG(?altEn) = "en") }}
}}
GROUP BY ?tmdb ?csfd ?item ?labelCs ?labelEn ?p1476
"""


def _chunked(seq: list, size: int) -> Iterable[list]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _index_results(bindings: list[dict], key_var: str) -> dict[str, list[dict]]:
    """Wikidata may return multiple ?item rows per external ID (rare —
    duplicate entities) or one row with multiple labels. Group rows by
    the external-ID variable so the caller can detect duplicates."""
    grouped: dict[str, list[dict]] = collections.defaultdict(list)
    for row in bindings:
        ext_id = row[key_var]["value"]
        grouped[ext_id].append(row)
    return grouped


def _extract_labels(row: dict) -> list[str | None]:
    """Return the Wikidata labels in priority order: `labelCs` first
    (may be None — the resolver uses position 0 as the veto signal),
    followed by `labelEn`, P1476 original title, then alt-labels."""
    out: list[str | None] = []
    out.append(row.get("labelCs", {}).get("value") or None)
    for key in ("labelEn", "p1476"):
        v = row.get(key, {}).get("value")
        if v:
            out.append(v)
    for key in ("altCsList", "altEnList"):
        v = row.get(key, {}).get("value")
        if v:
            out.extend(p for p in v.split("|") if p)
    return out


def _resolve_batch_imdb(
    session: requests.Session,
    rows: list[tuple[int, str, int | None, str | None, int | None]],
) -> dict[str, dict]:
    """Resolve a batch of cr rows that have an `imdb_id`. Returns a map
    of imdb_id → {qid, csfd_id, labels, duplicates}."""
    imdb_ids = [r[1] for r in rows]
    query = _build_imdb_query(imdb_ids)
    payload = _sparql_query(session, query)
    grouped = _index_results(payload["results"]["bindings"], "imdb")
    out: dict[str, dict] = {}
    for imdb_id, hits in grouped.items():
        # Distinct ?item entities for the same IMDb ID (rare but
        # possible). Caller should flag those rather than write.
        unique_items = {h["item"]["value"] for h in hits}
        first = hits[0]
        out[imdb_id] = {
            "qid": first["item"]["value"].rsplit("/", 1)[-1],
            "csfd_id": first.get("csfd", {}).get("value"),
            "labels": _extract_labels(first),
            "duplicates": len(unique_items) > 1,
        }
    return out


def _resolve_batch_tmdb(
    session: requests.Session,
    table: str,
    rows: list[tuple],
) -> dict[int, dict]:
    """Resolve a batch of cr rows that have a `tmdb_id`. Callers pass
    the full source-row tuple (id, imdb_id, tmdb_id, title, year[, …]);
    the helper reads the tmdb_id at index 2. The original signature
    declared a 4-tuple and indexed `r[1]`, which silently fed IMDb IDs
    into the TMDB-property SPARQL query — every call returned zero hits
    (PR #741, Copilot review). Returns a map of tmdb_id → info."""
    prop = TMDB_WIKIDATA_PROP[table]
    tmdb_ids = [r[2] for r in rows]
    query = _build_tmdb_query(tmdb_ids, prop)
    payload = _sparql_query(session, query)
    grouped = _index_results(payload["results"]["bindings"], "tmdb")
    out: dict[int, dict] = {}
    for tmdb_id_str, hits in grouped.items():
        unique_items = {h["item"]["value"] for h in hits}
        first = hits[0]
        out[int(tmdb_id_str)] = {
            "qid": first["item"]["value"].rsplit("/", 1)[-1],
            "csfd_id": first.get("csfd", {}).get("value"),
            "labels": _extract_labels(first),
            "duplicates": len(unique_items) > 1,
        }
    return out


def _open_run(conn, dry_run: bool, mode: str = "resolve") -> int | None:
    """Insert a run row at status=running. Returns the row id."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO csfd_id_resolution_runs (status, dry_run, mode) "
                "VALUES ('running', %s, %s) RETURNING id",
                (dry_run, mode),
            )
            run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    except psycopg2.Error as e:
        conn.rollback()
        logging.warning(
            "csfd_id_resolution_runs INSERT failed (migration 075/077?): %s", e)
        return None


def _close_run(
    conn,
    run_id: int | None,
    status: str,
    counts: dict,
    per_table: dict,
    error_message: str | None,
) -> None:
    if run_id is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE csfd_id_resolution_runs
                SET finished_at        = clock_timestamp(),
                    status             = %s,
                    processed          = %s,
                    resolved_via_imdb  = %s,
                    resolved_via_tmdb  = %s,
                    sanity_rejected    = %s,
                    unresolved         = %s,
                    per_table          = %s::jsonb,
                    error_message      = %s
                WHERE id = %s
                """,
                (
                    status,
                    counts.get("processed", 0),
                    counts.get("resolved_via_imdb", 0),
                    counts.get("resolved_via_tmdb", 0),
                    counts.get("sanity_rejected", 0),
                    counts.get("unresolved", 0),
                    json.dumps(per_table),
                    error_message,
                    run_id,
                ),
            )
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.warning("csfd_id_resolution_runs UPDATE failed: %s", e)


def _log_review(
    conn,
    run_id: int | None,
    dry_run: bool,
    table: str,
    row_id: int,
    cr_imdb_id: str | None,
    cr_tmdb_id: int | None,
    cr_title: str | None,
    cr_year: int | None,
    qid: str,
    proposed_csfd_id: int | None,
    label_cs: str | None,
    reason: str,
) -> None:
    if run_id is None:
        return
    if dry_run:
        # Echo to the log too — operators sometimes work from the
        # journalctl stream rather than psql. The DB row below is still
        # written so `SELECT … WHERE run_id = <dry_run id>` returns the
        # full queue.
        logging.info(
            "DRY-REVIEW: %s.id=%s reason=%s qid=%s proposed=%s "
            "label=%r ~ title=%r",
            table, row_id, reason, qid, proposed_csfd_id,
            label_cs, cr_title)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO csfd_id_resolution_review
                    (run_id, source_table, source_row_id,
                     cr_imdb_id, cr_tmdb_id, cr_title, cr_year,
                     wikidata_qid, proposed_csfd_id, wikidata_label_cs, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, table, row_id, cr_imdb_id, cr_tmdb_id, cr_title,
                 cr_year, qid, proposed_csfd_id, label_cs, reason),
            )
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.warning("csfd_id_resolution_review INSERT failed: %s", e)


def _process_table(
    conn,
    session: requests.Session,
    table: str,
    run_id: int | None,
    *,
    limit: int,
    batch_size: int,
    dry_run: bool,
) -> dict[str, int]:
    """Run the IMDb pass + TMDB fallback pass for one cr table. Returns
    a Counter-style dict with the standard keys."""
    counts: dict[str, int] = collections.Counter()
    cur = conn.cursor()

    # Pull the entire backlog up front. The backlog is bounded (~22 k
    # rows max across all tables) so memory is a non-issue.
    year_col = YEAR_COL[table]
    sql = (
        f"SELECT id, imdb_id, tmdb_id, title, {year_col} FROM {table} "
        "WHERE csfd_id IS NULL "
        "  AND (imdb_id IS NOT NULL OR tmdb_id IS NOT NULL) "
        "ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {limit}"
    cur.execute(sql)
    all_rows = cur.fetchall()
    counts["processed"] = len(all_rows)
    logging.info("[%s] %d rows to resolve%s",
                 table, len(all_rows), " (DRY RUN)" if dry_run else "")
    if not all_rows:
        return counts

    # ---- Pass 1: IMDb → ČSFD via P345 ----
    imdb_rows = [r for r in all_rows if r[1]]
    resolved_ids: set[int] = set()
    update_sql = (
        f"UPDATE {table} SET csfd_id = %s WHERE id = %s "
        "  AND csfd_id IS NULL "
        f"  AND NOT EXISTS (SELECT 1 FROM {table} t2 "
        "      WHERE t2.csfd_id = %s AND t2.id <> %s)"
    )

    for chunk in _chunked(imdb_rows, batch_size):
        results = _resolve_batch_imdb(session, chunk)
        for row in chunk:
            row_id, imdb_id, tmdb_id, title, year = row
            info = results.get(imdb_id)
            if not info:
                continue
            if info["duplicates"]:
                counts["sanity_rejected"] += 1
                resolved_ids.add(row_id)
                _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                            title, year, info["qid"],
                            int(info["csfd_id"]) if info["csfd_id"] else None,
                            "; ".join(l for l in info["labels"][:3] if l) or None, "duplicate_wikidata_entity")
                continue
            csfd_raw = info["csfd_id"]
            if not csfd_raw:
                # Wikidata knows the item but has no P2529 — counts as
                # resolved-in-Wikidata-but-no-csfd, fall through to
                # leave row unresolved.
                continue
            try:
                csfd_id = int(csfd_raw)
            except ValueError:
                logging.warning(
                    "Wikidata returned non-numeric P2529 %r for IMDb %s "
                    "— logging to review", csfd_raw, imdb_id)
                counts["sanity_rejected"] += 1
                resolved_ids.add(row_id)
                _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                            title, year, info["qid"], None, "; ".join(l for l in info["labels"][:3] if l) or None,
                            "non_numeric_csfd")
                continue
            if not _label_matches(info["labels"], title):
                counts["sanity_rejected"] += 1
                resolved_ids.add(row_id)
                _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                            title, year, info["qid"], csfd_id, "; ".join(l for l in info["labels"][:3] if l) or None,
                            "label_mismatch")
                continue
            if dry_run:
                # Mirror the NOT EXISTS clause from update_sql so dry-run
                # counters don't overreport "resolved" — proposals that
                # would collide with a sibling csfd_id are classified as
                # csfd_collision_in_cr exactly as the real-run UPDATE.
                cur.execute(
                    f"SELECT 1 FROM {table} "
                    "WHERE csfd_id = %s AND id <> %s LIMIT 1",
                    (csfd_id, row_id),
                )
                if cur.fetchone():
                    counts["sanity_rejected"] += 1
                    resolved_ids.add(row_id)
                    _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                                title, year, info["qid"], csfd_id,
                                "; ".join(l for l in info["labels"][:3] if l) or None, "csfd_collision_in_cr")
                    continue
                counts["resolved_via_imdb"] += 1
                resolved_ids.add(row_id)
                logging.info(
                    "DRY: %s.id=%s imdb=%s → csfd=%s (label=%r ~ title=%r)",
                    table, row_id, imdb_id, csfd_id,
                    "; ".join(l for l in info["labels"][:3] if l) or None, title)
            else:
                cur.execute(update_sql, (csfd_id, row_id, csfd_id, row_id))
                if cur.rowcount:
                    counts["resolved_via_imdb"] += 1
                    resolved_ids.add(row_id)
                else:
                    # Either someone else won the race or csfd_id is
                    # already used by a sibling row.
                    counts["sanity_rejected"] += 1
                    resolved_ids.add(row_id)
                    _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                                title, year, info["qid"], csfd_id,
                                "; ".join(l for l in info["labels"][:3] if l) or None, "csfd_collision_in_cr")
        if not dry_run:
            conn.commit()
        time.sleep(BASE_SLEEP_SECONDS)

    # ---- Pass 2: TMDB → ČSFD fallback ----
    tmdb_rows = [r for r in all_rows
                 if r[2] is not None and r[0] not in resolved_ids]
    for chunk in _chunked(tmdb_rows, batch_size):
        results = _resolve_batch_tmdb(session, table, chunk)
        for row in chunk:
            row_id, imdb_id, tmdb_id, title, year = row
            info = results.get(tmdb_id)
            if not info:
                continue
            if info["duplicates"]:
                counts["sanity_rejected"] += 1
                resolved_ids.add(row_id)
                _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                            title, year, info["qid"],
                            int(info["csfd_id"]) if info["csfd_id"] else None,
                            "; ".join(l for l in info["labels"][:3] if l) or None, "duplicate_wikidata_entity")
                continue
            csfd_raw = info["csfd_id"]
            if not csfd_raw:
                continue
            try:
                csfd_id = int(csfd_raw)
            except ValueError:
                counts["sanity_rejected"] += 1
                resolved_ids.add(row_id)
                _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                            title, year, info["qid"], None, "; ".join(l for l in info["labels"][:3] if l) or None,
                            "non_numeric_csfd")
                continue
            if not _label_matches(info["labels"], title):
                counts["sanity_rejected"] += 1
                resolved_ids.add(row_id)
                _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                            title, year, info["qid"], csfd_id, "; ".join(l for l in info["labels"][:3] if l) or None,
                            "label_mismatch")
                continue
            if dry_run:
                cur.execute(
                    f"SELECT 1 FROM {table} "
                    "WHERE csfd_id = %s AND id <> %s LIMIT 1",
                    (csfd_id, row_id),
                )
                if cur.fetchone():
                    counts["sanity_rejected"] += 1
                    resolved_ids.add(row_id)
                    _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                                title, year, info["qid"], csfd_id,
                                "; ".join(l for l in info["labels"][:3] if l) or None, "csfd_collision_in_cr")
                    continue
                counts["resolved_via_tmdb"] += 1
                resolved_ids.add(row_id)
                logging.info(
                    "DRY: %s.id=%s tmdb=%s → csfd=%s (label=%r ~ title=%r)",
                    table, row_id, tmdb_id, csfd_id,
                    "; ".join(l for l in info["labels"][:3] if l) or None, title)
            else:
                cur.execute(update_sql, (csfd_id, row_id, csfd_id, row_id))
                if cur.rowcount:
                    counts["resolved_via_tmdb"] += 1
                    resolved_ids.add(row_id)
                else:
                    counts["sanity_rejected"] += 1
                    resolved_ids.add(row_id)
                    _log_review(conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                                title, year, info["qid"], csfd_id,
                                "; ".join(l for l in info["labels"][:3] if l) or None, "csfd_collision_in_cr")
        if not dry_run:
            conn.commit()
        time.sleep(BASE_SLEEP_SECONDS)

    counts["unresolved"] = counts["processed"] - len(resolved_ids)
    logging.info("[%s] Done — %s", table, dict(counts))
    return counts


def _log_reconcile(
    conn,
    run_id: int | None,
    dry_run: bool,
    table: str,
    row_id: int,
    cr_imdb_id: str | None,
    cr_tmdb_id: int | None,
    cr_title: str | None,
    cr_year: int | None,
    cr_csfd_id: int,
    qid: str,
    wikidata_csfd_id: int | None,
    label_cs: str | None,
    reason: str,
) -> None:
    """Insert a disagreement row into csfd_id_reconcile_review. The
    write happens for both dry-run and real-run mode — the table is
    a record-of-disagreement, not a write queue. --apply-safe-rewrites
    later consumes pending_review rows to perform the actual UPDATE."""
    if run_id is None:
        return
    if dry_run:
        logging.info(
            "DRY-RECONCILE: %s.id=%s cr_csfd=%s ↔ wd_csfd=%s reason=%s "
            "qid=%s label_cs=%r ~ title=%r",
            table, row_id, cr_csfd_id, wikidata_csfd_id, reason,
            qid, label_cs, cr_title)
    try:
        with conn.cursor() as cur:
            # ON CONFLICT keys off the partial-unique index added in
            # migration 078: (source_table, source_row_id) WHERE
            # action_taken = 'pending_review'. Re-running --reconcile
            # before --apply-safe-rewrites consumes the queue is now
            # idempotent — a duplicate proposal silently skips instead
            # of stacking up extra pending rows that would skew the
            # apply pass (PR #741, Copilot review).
            cur.execute(
                """
                INSERT INTO csfd_id_reconcile_review
                    (run_id, source_table, source_row_id,
                     cr_imdb_id, cr_tmdb_id, cr_title, cr_year,
                     cr_csfd_id, wikidata_qid, wikidata_csfd_id,
                     wikidata_label_cs, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_table, source_row_id)
                    WHERE action_taken = 'pending_review'
                    DO NOTHING
                """,
                (run_id, table, row_id, cr_imdb_id, cr_tmdb_id, cr_title,
                 cr_year, cr_csfd_id, qid, wikidata_csfd_id, label_cs,
                 reason),
            )
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logging.warning("csfd_id_reconcile_review INSERT failed: %s", e)


def _process_table_reconcile(
    conn,
    session: requests.Session,
    table: str,
    run_id: int | None,
    *,
    limit: int,
    batch_size: int,
    dry_run: bool,
) -> dict[str, int]:
    """Reconcile pass: walk csfd_id IS NOT NULL rows, compare cr.csfd_id
    vs Wikidata P2529, queue disagreements into csfd_id_reconcile_review.

    Never writes to the source table — the actual rewrite is gated on
    --apply-safe-rewrites. Counters used:
      processed             — rows scanned
      resolved_via_imdb     — rows where Wikidata had a P345 hit
      resolved_via_tmdb     — rows where Wikidata had a P4947/P4983 hit
                              (after IMDb pass found no match)
      sanity_rejected       — rows queued into reconcile_review
                              (this includes both auto-rewriteable and
                              human-triage cases — the split happens
                              later in --apply-safe-rewrites)
      unresolved            — rows scanned but Wikidata had no match
                              at all (cr.csfd_id stays untouched)
    """
    counts: dict[str, int] = collections.Counter()
    cur = conn.cursor()

    year_col = YEAR_COL[table]
    sql = (
        f"SELECT id, imdb_id, tmdb_id, title, {year_col}, csfd_id FROM {table} "
        "WHERE csfd_id IS NOT NULL "
        "  AND (imdb_id IS NOT NULL OR tmdb_id IS NOT NULL) "
        "ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {limit}"
    cur.execute(sql)
    all_rows = cur.fetchall()
    counts["processed"] = len(all_rows)
    logging.info("[%s] reconcile: %d rows to check%s",
                 table, len(all_rows), " (DRY RUN)" if dry_run else "")
    if not all_rows:
        return counts

    # Track rows for which the IMDb pass already returned a verdict
    # (matched-and-agreed, matched-and-disagreed, or matched-but-no-P2529).
    # Anything left uncovered after IMDb falls through to the TMDB pass.
    handled: set[int] = set()

    def _classify(
        row,
        wikidata_csfd_raw,
        info_qid,
        info_labels,
        info_duplicates,
        match_path: str,
    ) -> None:
        """Compare wd.csfd vs cr.csfd_id, write to review if disagreeing.
        `match_path` is 'imdb' or 'tmdb' — only used to pick the counter."""
        row_id, imdb_id, tmdb_id, title, year, cr_csfd_id = row
        if match_path == "imdb":
            counts["resolved_via_imdb"] += 1
        else:
            counts["resolved_via_tmdb"] += 1
        label_cs = info_labels[0] if info_labels else None
        # Check duplicates FIRST. _resolve_batch_*_ only returns the
        # first binding's csfd_id (`first.get("csfd", …)`), so if there
        # are multiple Wikidata items for one external ID and the
        # first happens to have no P2529 while another duplicate does,
        # falling through to the missing-P2529 branch would silently
        # drop a real ambiguity that deserves manual triage. Ordering
        # duplicate-check before the missing-csfd check fixes that
        # (PR #741, Copilot review).
        if info_duplicates:
            counts["sanity_rejected"] += 1
            wikidata_csfd_id: int | None = None
            if wikidata_csfd_raw:
                try:
                    wikidata_csfd_id = int(wikidata_csfd_raw)
                except ValueError:
                    wikidata_csfd_id = None
            _log_reconcile(
                conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                title, year, cr_csfd_id, info_qid, wikidata_csfd_id,
                label_cs, "duplicate_wikidata_entity")
            return
        if not wikidata_csfd_raw:
            counts["sanity_rejected"] += 1
            _log_reconcile(
                conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                title, year, cr_csfd_id, info_qid, None, label_cs,
                "wikidata_missing_p2529")
            return
        try:
            wikidata_csfd_id = int(wikidata_csfd_raw)
        except ValueError:
            counts["sanity_rejected"] += 1
            _log_reconcile(
                conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                title, year, cr_csfd_id, info_qid, None, label_cs,
                "non_numeric_csfd")
            return
        if wikidata_csfd_id == cr_csfd_id:
            # Match — Wikidata confirms cr's existing value. No action.
            return
        # Disagreement. Decide whether the labelCs sanity check would
        # later let --apply-safe-rewrites auto-fix this. The check is
        # the SAME function used by the fill-NULL pass, so a rewrite
        # has at least as much evidence as a fresh write.
        if _label_matches(info_labels, title):
            counts["sanity_rejected"] += 1
            _log_reconcile(
                conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                title, year, cr_csfd_id, info_qid, wikidata_csfd_id,
                label_cs, "wikidata_disagrees")
        else:
            # Wikidata disagrees AND labelCs doesn't match cr.title —
            # too risky to auto-rewrite. Queue for human triage with a
            # different reason so --apply-safe-rewrites skips it.
            counts["sanity_rejected"] += 1
            _log_reconcile(
                conn, run_id, dry_run, table, row_id, imdb_id, tmdb_id,
                title, year, cr_csfd_id, info_qid, wikidata_csfd_id,
                label_cs, "label_mismatch_blocked_rewrite")

    # ---- Pass 1: IMDb → ČSFD via P345 ----
    imdb_rows = [r for r in all_rows if r[1]]
    for chunk in _chunked(imdb_rows, batch_size):
        results = _resolve_batch_imdb(session, chunk)
        for row in chunk:
            row_id, imdb_id = row[0], row[1]
            info = results.get(imdb_id)
            if not info:
                continue
            handled.add(row_id)
            _classify(row, info["csfd_id"], info["qid"], info["labels"],
                      info["duplicates"], "imdb")
        time.sleep(BASE_SLEEP_SECONDS)

    # ---- Pass 2: TMDB → ČSFD fallback ----
    tmdb_rows = [r for r in all_rows
                 if r[2] is not None and r[0] not in handled]
    for chunk in _chunked(tmdb_rows, batch_size):
        results = _resolve_batch_tmdb(session, table, chunk)
        for row in chunk:
            row_id, _imdb_id, tmdb_id = row[0], row[1], row[2]
            info = results.get(tmdb_id)
            if not info:
                continue
            handled.add(row_id)
            _classify(row, info["csfd_id"], info["qid"], info["labels"],
                      info["duplicates"], "tmdb")
        time.sleep(BASE_SLEEP_SECONDS)

    counts["unresolved"] = counts["processed"] - len(handled)
    logging.info("[%s] reconcile done — %s", table, dict(counts))
    return counts


def _apply_safe_rewrites(
    conn,
    run_id: int | None,
    *,
    dry_run: bool,
    tables: list[str] | None = None,
    limit: int = 0,
) -> dict[str, int]:
    """Walk csfd_id_reconcile_review pending_review rows and apply the
    auto-rewrite policy from #740: when labelCs (already normalised
    against cr.title at queue time, so a 'wikidata_disagrees' tag is
    sufficient evidence) matches, UPDATE source row.

    The UPDATE is guarded by `csfd_id = <original cr_csfd_id>` so a
    manual fix between the dry-run queue and the apply pass is not
    clobbered — in that case the rewrite is silently dropped and the
    review row is marked 'manual_resolved' on a separate clean-up pass.

    Counters:
      reviewed            — pending_review rows looked at
      rewrote             — UPDATE actually changed a row
      stale_no_op         — the row's csfd_id changed since the dry-run
                             (manual fix or another reconcile pass)
      skipped_unsafe      — review row failed the strict-rewrite gate
                             (reason != wikidata_disagrees, OR labelCs
                             absent, OR labelCs doesn't match cr.title)
      collision           — proposed csfd_id is already used by a sibling
                             row in the same table (would violate uniqueness)

    Strict-rewrite gate (#740): the reconcile classification logs
    `wikidata_disagrees` whenever Wikidata returned a different P2529
    and the existing fill-NULL labels-pass accepted it — which for
    backwards-compat purposes ALSO accepts `labelCs IS NULL` (the
    bulk resolver trusts a bare IMDb match as evidence). That permissive
    behaviour is fine when writing into a NULL, but overwriting a
    pre-existing (possibly human-curated) value needs harder evidence.
    Apply therefore re-checks at write time: labelCs MUST be present and
    match cr.title after normalisation. Everything else stays
    pending_review for human triage.
    """
    counts: dict[str, int] = collections.Counter()
    cur = conn.cursor()

    # Honour --table and --limit even in apply mode. The fill-NULL pass
    # and reconcile pass both filter on these flags; silently applying
    # to every table when a maintainer typed `--table films --limit 5`
    # would be a data-changing surprise (PR #741, Copilot review).
    where_clauses = ["action_taken = 'pending_review'"]
    params: list = []
    if tables:
        where_clauses.append("source_table = ANY(%s)")
        params.append(tables)
    sql = (
        "SELECT id, source_table, source_row_id, "
        "       cr_csfd_id, wikidata_csfd_id, reason, "
        "       wikidata_label_cs, cr_title "
        "FROM csfd_id_reconcile_review "
        f"WHERE {' AND '.join(where_clauses)} "
        "ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql, params)
    review_rows = cur.fetchall()
    logging.info("apply-safe-rewrites: %d pending_review rows%s%s",
                 len(review_rows),
                 f" (tables={tables})" if tables else "",
                 " (DRY RUN)" if dry_run else "")

    for (rev_id, src_table, src_row_id, cr_csfd_id, wd_csfd_id,
         reason, wd_label_cs, cr_title) in review_rows:
        counts["reviewed"] += 1
        if reason != "wikidata_disagrees" or wd_csfd_id is None:
            counts["skipped_unsafe"] += 1
            continue
        # Strict gate: labelCs must be present AND match cr.title after
        # normalisation. Bypassed only when cr.title itself is missing /
        # too short to compare meaningfully (rare).
        if wd_label_cs is None:
            counts["skipped_unsafe"] += 1
            continue
        if cr_title and not _one_match(wd_label_cs, _normalise(cr_title)):
            counts["skipped_unsafe"] += 1
            continue
        # Atomic UPDATE: the row is rewritten only if (a) csfd_id is
        # still the same value we proposed against (no race with a
        # manual fix or another reconcile pass), AND (b) no sibling
        # in the same table already holds the proposed csfd_id. The
        # NOT EXISTS predicate runs INSIDE the same UPDATE so a
        # concurrent writer can't slip a colliding row between a
        # separate SELECT and the UPDATE (PR #741, Copilot review).
        update_sql = (
            f"UPDATE {src_table} "
            "SET csfd_id = %s "
            "WHERE id = %s AND csfd_id = %s "
            f"  AND NOT EXISTS (SELECT 1 FROM {src_table} t2 "
            "      WHERE t2.csfd_id = %s AND t2.id <> %s)"
        )
        if dry_run:
            # Dry-run still needs to distinguish would-rewrite vs.
            # would-collide vs. stale, but it CANNOT execute the
            # UPDATE. Mirror the same predicate as a SELECT so the
            # counters line up with what the real pass would do.
            cur.execute(
                f"SELECT EXISTS(SELECT 1 FROM {src_table} "
                "  WHERE id = %s AND csfd_id = %s) AS still_matches, "
                f"  EXISTS(SELECT 1 FROM {src_table} "
                "  WHERE csfd_id = %s AND id <> %s) AS collides",
                (src_row_id, cr_csfd_id, wd_csfd_id, src_row_id),
            )
            still_matches, collides = cur.fetchone()
            if collides:
                counts["collision"] += 1
            elif still_matches:
                counts["rewrote"] += 1
                logging.info(
                    "DRY: %s.id=%s csfd %s → %s",
                    src_table, src_row_id, cr_csfd_id, wd_csfd_id)
            else:
                counts["stale_no_op"] += 1
            continue
        cur.execute(update_sql,
                    (wd_csfd_id, src_row_id, cr_csfd_id,
                     wd_csfd_id, src_row_id))
        if cur.rowcount == 1:
            counts["rewrote"] += 1
            cur.execute(
                "UPDATE csfd_id_reconcile_review "
                "SET action_taken = 'auto_rewritten', "
                "    rewritten_at = clock_timestamp() "
                "WHERE id = %s",
                (rev_id,),
            )
        else:
            # 0 rows updated: either csfd_id changed (manual fix /
            # newer reconcile pass) OR a sibling now holds the
            # proposed csfd_id. Disambiguate so the audit log is
            # honest about which case happened.
            cur.execute(
                f"SELECT EXISTS(SELECT 1 FROM {src_table} "
                "  WHERE csfd_id = %s AND id <> %s)",
                (wd_csfd_id, src_row_id),
            )
            (collides,) = cur.fetchone()
            if collides:
                counts["collision"] += 1
                cur.execute(
                    "UPDATE csfd_id_reconcile_review "
                    "SET action_taken = 'kept_original' "
                    "WHERE id = %s",
                    (rev_id,),
                )
            else:
                counts["stale_no_op"] += 1
                cur.execute(
                    "UPDATE csfd_id_reconcile_review "
                    "SET action_taken = 'manual_resolved' "
                    "WHERE id = %s",
                    (rev_id,),
                )
        conn.commit()

    if dry_run:
        conn.rollback()
    logging.info("apply-safe-rewrites done — %s", dict(counts))
    return counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="all",
                        choices=["films", "series", "tv_shows", "all"])
    parser.add_argument("--limit", type=int, default=0, help="0 = all")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--reconcile", action="store_true",
        help="Walk csfd_id IS NOT NULL rows, queue disagreements with "
             "Wikidata into csfd_id_reconcile_review (#740). No source "
             "writes — apply happens via --apply-safe-rewrites.")
    mode_group.add_argument(
        "--apply-safe-rewrites", action="store_true",
        help="Apply auto-rewrite policy: UPDATE source rows where the "
             "review queue has a wikidata_disagrees row whose labelCs "
             "matches cr.title. Reversible from the audit log.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        raise SystemExit("DATABASE_URL required")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    tables = (["films", "series", "tv_shows"]
              if args.table == "all" else [args.table])

    # --apply-safe-rewrites is a pure DB pass — no SPARQL, no per-table
    # iteration. The review queue already carries Wikidata's verdict.
    if args.apply_safe_rewrites:
        run_id = _open_run(conn, dry_run=args.dry_run, mode="reconcile")
        status = "ok"
        error_message: str | None = None
        try:
            counts = _apply_safe_rewrites(
                conn, run_id,
                dry_run=args.dry_run,
                tables=tables if args.table != "all" else None,
                limit=args.limit,
            )
        except Exception as exc:  # noqa: BLE001
            status = "error"
            error_message = repr(exc)
            logging.exception("apply-safe-rewrites aborted")
            counts = collections.Counter()
        # Re-map apply-pass counters onto the runs-table column layout
        # so the existing dashboard query keeps working. `processed` is
        # the queue size, `resolved_via_imdb` records actual rewrites,
        # `sanity_rejected` aggregates collision + stale + skipped.
        totals_for_run = {
            "processed": counts.get("reviewed", 0),
            "resolved_via_imdb": counts.get("rewrote", 0),
            "resolved_via_tmdb": 0,
            "sanity_rejected": (counts.get("collision", 0)
                                + counts.get("skipped_unsafe", 0)
                                + counts.get("stale_no_op", 0)),
            "unresolved": 0,
        }
        _close_run(conn, run_id, status, totals_for_run,
                   {"__apply__": dict(counts)}, error_message)
        logging.info("apply-safe-rewrites totals — %s", dict(counts))
        return 0 if status != "error" else 1

    mode = "reconcile" if args.reconcile else "resolve"
    run_id = _open_run(conn, dry_run=args.dry_run, mode=mode)
    totals: dict[str, int] = collections.Counter()
    per_table: dict[str, dict] = {}
    status = "ok"
    error_message = None

    processor = _process_table_reconcile if args.reconcile else _process_table

    try:
        for t in tables:
            c = processor(
                conn, session, t, run_id,
                limit=args.limit,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
            )
            per_table[t] = dict(c)
            for k, v in c.items():
                totals[k] += v
    except Exception as exc:  # noqa: BLE001 — top-level run-log handler
        status = "error"
        error_message = repr(exc)
        logging.exception("Resolver aborted")
    finally:
        if status == "ok" and totals.get("unresolved", 0) > 0:
            # "partial" is the standard rating_sync_runs idiom for "ran
            # to completion but some rows were not refreshed".
            status = "partial"
        _close_run(conn, run_id, status, dict(totals), per_table, error_message)

    logging.info("Grand totals — %s", dict(totals))
    return 0 if status != "error" else 1


if __name__ == "__main__":
    sys.exit(main())
