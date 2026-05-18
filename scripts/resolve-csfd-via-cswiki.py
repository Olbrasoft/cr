#!/usr/bin/env python3
"""Resolve missing `csfd_id` via Czech Wikipedia external links — a
last-resort path for rows where the main Wikidata resolver couldn't
find a P2529 link (#732 leaves them as `wikidata_missing_p2529` or
`unresolved`).

Why this exists: a Wikidata item that maps IMDb→Q-item but has no
P2529 sometimes still has a Czech Wikipedia article that links to
csfd.cz/film/{id} in its external-links section. Maintainers
occasionally fill those into Wikipedia but not Wikidata. This script
scrapes that residual signal.

Realistic recovery: probe on 100 random films showed ~1 % yield.
Expected total: a few dozen rows out of ~6.6k missing. Not worth a
full migration + audit-table redesign — runs as a discovery script
emitting a TSV that the maintainer can review and either apply via
`--apply` or hand-curate.

Two phases:

  1. Wikidata batch query (P345 → Q-item + cs-wiki sitelink). Endpoint
     is rate-limited to 1 req/min during the active wdqs outage so
     batches are 500 IDs at a time with a 65 s gap between batches.

  2. Per-Q-item-with-cs-wiki: fetch the article via MediaWiki API,
     parse `csfd.cz/film/(\\d+)` from externallinks. MediaWiki API
     has no throttle to worry about; 0.5 s gap is just politeness.

Safety gate (applied during --apply, not at extract):
  * the cs-wiki article title (a Czech title from Wikidata's sitelink)
    must normalised-match cr.title or cr.original_title — i.e. the
    article is *about* the film cr has, not a name collision.
  * proposed csfd_id must not already exist on a different cr row in
    the same table (sibling-collision guard, same as #740).

Usage:

    # Phase A — extract proposals (writes data/csfd-cswiki/{table}.tsv)
    DATABASE_URL=postgres://cr:cr@prod-host/cr \\
        python3 scripts/resolve-csfd-via-cswiki.py extract \\
            [--table films|series|tv_shows|all] \\
            [--limit N]

    # Phase B — apply proposals after manual review
    DATABASE_URL=postgres://cr:cr@prod-host/cr \\
        python3 scripts/resolve-csfd-via-cswiki.py apply \\
            [--table films|series|tv_shows|all] \\
            [--dry-run]
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
import urllib.parse
import urllib.request
import urllib.error
import json
from pathlib import Path

import psycopg2

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
MEDIAWIKI_API = "https://cs.wikipedia.org/w/api.php"

USER_AGENT = os.environ.get(
    "CR_CSFD_RESOLVER_USER_AGENT",
    "cr-csfd-cswiki/0.1 (https://ceskarepublika.wiki; "
    "noreply@ceskarepublika.wiki)",
)

# Wikidata throttling: 1 req/min during the active wdqs outage (May
# 2026, same as the main resolver). Batches up to ~500 IDs work.
SPARQL_BATCH = 500
SPARQL_INTERBATCH_SLEEP = 65.0

# cs.wiki is unmetered but be a polite citizen.
MEDIAWIKI_SLEEP = 0.5

OUT_DIR = Path("data/csfd-cswiki")

TABLES = ("films", "series", "tv_shows")


# ---------------------------------------------------------------------------
# Normalisation (lifted from resolve-csfd-via-wikidata.py)
# ---------------------------------------------------------------------------

# Cyrillic-Latin confusables that show up in Czech-encoded titles
# (mostly diacritic variants). Same map as the main resolver.
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


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _sparql_post(query: str, max_retries: int = 6) -> dict:
    backoff = 5.0
    for attempt in range(max_retries):
        try:
            data = urllib.parse.urlencode(
                {"query": query, "format": "json"}
            ).encode("utf-8")
            req = urllib.request.Request(
                SPARQL_ENDPOINT,
                data=data,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/sparql-results+json",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = e.headers.get("Retry-After")
                wait = (
                    float(retry_after)
                    if retry_after and retry_after.isdigit()
                    else backoff
                )
                wait = min(wait, 70.0)
                logging.warning(
                    "Wikidata 429 (attempt %d/%d) — sleep %.0fs",
                    attempt + 1, max_retries, wait,
                )
                time.sleep(wait)
                backoff = min(backoff * 2, 70.0)
                continue
            if e.code >= 500:
                logging.warning(
                    "Wikidata %d (attempt %d/%d) — sleep %.0fs",
                    e.code, attempt + 1, max_retries, backoff,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 70.0)
                continue
            raise
    raise RuntimeError("Wikidata SPARQL retries exhausted")


def _mediawiki_get(params: dict) -> dict:
    url = MEDIAWIKI_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


# ---------------------------------------------------------------------------
# Wikidata + cs-wiki lookup
# ---------------------------------------------------------------------------


def wikidata_imdb_to_qid_and_cswiki(
    imdb_ids: list[str],
) -> dict[str, tuple[str | None, str | None]]:
    """Batch IMDb → (qid, cs-wiki article title). Title is None when the
    Wikidata item has no cs-wiki sitelink."""
    if not imdb_ids:
        return {}
    values = " ".join(f'"{x}"' for x in imdb_ids)
    q = f"""
SELECT ?imdb ?item ?cswikiTitle WHERE {{
  VALUES ?imdb {{ {values} }}
  ?item wdt:P345 ?imdb .
  OPTIONAL {{
    ?sitelink schema:about ?item ;
              schema:isPartOf <https://cs.wikipedia.org/> ;
              schema:name ?cswikiTitle .
  }}
}}"""
    out: dict[str, tuple[str | None, str | None]] = {}
    data = _sparql_post(q)
    for b in data["results"]["bindings"]:
        imdb = b["imdb"]["value"]
        qid = b.get("item", {}).get("value", "").split("/")[-1] or None
        cs = b.get("cswikiTitle", {}).get("value")
        # Multiple rows can come back if Wikidata has duplicates;
        # prefer the first row that *has* a cs-wiki sitelink.
        if imdb not in out or (cs and not out[imdb][1]):
            out[imdb] = (qid, cs)
    return out


def cswiki_csfd_id(title: str) -> int | None:
    """Fetch externallinks from the cs-wiki article and return the
    first csfd.cz/film/{id} found. None when the article doesn't
    exist, doesn't link to ČSFD, or the API call fails."""
    params = {
        "action": "parse",
        "page": title,
        "prop": "externallinks",
        "format": "json",
        "formatversion": "2",
        "redirects": "1",
    }
    try:
        data = _mediawiki_get(params)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        logging.warning("cs-wiki fetch failed for %r: HTTP %d", title, e.code)
        return None
    except Exception as e:
        logging.warning("cs-wiki fetch failed for %r: %s", title, e)
        return None
    if "error" in data:
        # missingtitle, normally
        return None
    links = data.get("parse", {}).get("externallinks", []) or []
    for l in links:
        m = re.search(r"csfd\.cz/film/(\d+)", l)
        if m:
            return int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------


def fetch_rows(conn, table: str, limit: int | None) -> list[tuple]:
    """Returns rows missing csfd_id but with imdb_id."""
    year_col = "year" if table == "films" else "first_air_year"
    sql = (
        f"SELECT id, imdb_id, tmdb_id, title, original_title, "
        f"{year_col} AS year FROM {table} "
        f"WHERE csfd_id IS NULL AND imdb_id IS NOT NULL "
        f"ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {limit}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def csfd_id_already_used(conn, table: str, csfd_id: int) -> int | None:
    """Returns the id of any other row in the same table that already
    has this csfd_id. None if free."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {table} WHERE csfd_id = %s LIMIT 1",
            (csfd_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Extract phase
# ---------------------------------------------------------------------------


def extract(args, conn) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tables = TABLES if args.table == "all" else (args.table,)
    for table in tables:
        rows = fetch_rows(conn, table, args.limit)
        logging.info("table=%s rows=%d", table, len(rows))
        if not rows:
            continue

        imdb_ids = [r[1] for r in rows if r[1]]
        wd: dict[str, tuple[str | None, str | None]] = {}
        for i in range(0, len(imdb_ids), SPARQL_BATCH):
            chunk = imdb_ids[i:i + SPARQL_BATCH]
            logging.info(
                "wikidata batch %d/%d (size=%d)",
                i // SPARQL_BATCH + 1,
                (len(imdb_ids) + SPARQL_BATCH - 1) // SPARQL_BATCH,
                len(chunk),
            )
            wd.update(wikidata_imdb_to_qid_and_cswiki(chunk))
            if i + SPARQL_BATCH < len(imdb_ids):
                time.sleep(SPARQL_INTERBATCH_SLEEP)

        path = OUT_DIR / f"{table}.tsv"
        with path.open("w", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow([
                "row_id", "imdb_id", "tmdb_id", "title", "original_title",
                "year", "qid", "cs_wiki_title", "proposed_csfd_id",
                "norm_title_match", "norm_original_match",
            ])
            counts = {"qid": 0, "cswiki": 0, "csfd": 0, "match": 0}
            for row_id, imdb_id, tmdb_id, title, orig, year in rows:
                qid, cs = wd.get(imdb_id, (None, None))
                if qid:
                    counts["qid"] += 1
                if not cs:
                    continue
                counts["cswiki"] += 1
                time.sleep(MEDIAWIKI_SLEEP)
                csfd = cswiki_csfd_id(cs)
                if not csfd:
                    continue
                counts["csfd"] += 1
                cs_norm = normalise(re.sub(r"\s*\([^)]*\)\s*$", "", cs))
                t_match = bool(title) and normalise(title) == cs_norm
                o_match = bool(orig) and normalise(orig) == cs_norm
                if t_match or o_match:
                    counts["match"] += 1
                w.writerow([
                    row_id, imdb_id or "", tmdb_id or "", title or "",
                    orig or "", year or "", qid or "", cs,
                    csfd, "1" if t_match else "0",
                    "1" if o_match else "0",
                ])
        logging.info(
            "table=%s qid=%d cswiki=%d csfd_link=%d title_match=%d → %s",
            table, counts["qid"], counts["cswiki"], counts["csfd"],
            counts["match"], path,
        )


# ---------------------------------------------------------------------------
# Apply phase
# ---------------------------------------------------------------------------


def apply_(args, conn) -> None:
    tables = TABLES if args.table == "all" else (args.table,)
    grand = {"applied": 0, "skipped_collision": 0, "skipped_no_match": 0,
             "skipped_already_set": 0, "missing_file": 0}
    for table in tables:
        path = OUT_DIR / f"{table}.tsv"
        if not path.exists():
            logging.warning("no proposals file for %s — run `extract` first", table)
            grand["missing_file"] += 1
            continue
        with path.open() as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        logging.info("table=%s proposals=%d", table, len(rows))
        for r in rows:
            row_id = int(r["row_id"])
            csfd = int(r["proposed_csfd_id"])
            if r["norm_title_match"] != "1" and r["norm_original_match"] != "1":
                grand["skipped_no_match"] += 1
                continue
            collision = csfd_id_already_used(conn, table, csfd)
            if collision and collision != row_id:
                logging.info(
                    "skip %s.id=%s csfd=%d — taken by id=%d",
                    table, row_id, csfd, collision,
                )
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
                grand["applied"] += 1
                logging.info(
                    "apply %s.id=%s ← csfd_id=%d (cs-wiki: %r)",
                    table, row_id, csfd, r["cs_wiki_title"],
                )
        if args.dry_run:
            conn.rollback()
            logging.info("--dry-run → rolled back %s", table)
        else:
            conn.commit()
    logging.info("summary: %s", grand)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("extract", help="Phase A: write proposals TSV")
    e.add_argument(
        "--table", choices=("films", "series", "tv_shows", "all"),
        default="all",
    )
    e.add_argument("--limit", type=int, default=None)

    a = sub.add_parser("apply", help="Phase B: UPDATE rows from TSV")
    a.add_argument(
        "--table", choices=("films", "series", "tv_shows", "all"),
        default="all",
    )
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
