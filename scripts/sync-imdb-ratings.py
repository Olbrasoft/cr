#!/usr/bin/env python3
"""Refresh `imdb_rating` + `imdb_votes` for films, series and tv_shows from
the public IMDb datasets TSV (#690 — sub-issue of #588).

IMDb publishes daily snapshots of every title's average rating and vote
count at https://datasets.imdbws.com/title.ratings.tsv.gz . The file is
~10 MB compressed, contains ~1.5 M rows, and has no API key or rate-limit
attached — perfect for a daily cron that updates our ~27 K titles.

Pipeline:
  1. Load every non-null `imdb_id` from films + series + tv_shows into a
     dict (id → table) so the match below is a single hash lookup per row.
  2. Conditional GET on the TSV using the cached Last-Modified header —
     if IMDb hasn't republished since the previous run, exit early without
     touching the DB.
  3. Stream the gzip body line-by-line; for each (tconst, avg, votes)
     where tconst is in our id-set, append to a per-table UPDATE batch.
  4. Single COMMIT at the end (UPDATEs run in 1 000-row chunks via
     psycopg2.execute_values).

Idempotent: re-running with no new TSV does zero DB writes (HTTP 304 →
exit before the UPDATE loop runs). Re-running with the same TSV after
`--force` re-applies the same values and bumps `imdb_rating_synced_at`.

Usage:
    DATABASE_URL=postgres://... python3 scripts/sync-imdb-ratings.py [--cache-dir DIR] [--dry-run] [--force]

`--cache-dir` defaults to `./data/imdb-cache/` (created on first run).
`--force` re-syncs from the cached TSV even when IMDb returns 304.
"""

from __future__ import annotations

import argparse
import gzip
import io
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

TSV_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
DEFAULT_CACHE_DIR = Path("data/imdb-cache")
HEADER_FILE_NAME = "title.ratings.headers"
TSV_FILE_NAME = "title.ratings.tsv.gz"

TABLES = ("films", "series", "tv_shows")


def _load_our_imdb_ids(cur) -> dict[str, str]:
    """Return imdb_id → table for every row we may want to update.

    A given tconst can only belong to one of (film, series, tv_show) in
    practice, so the dict-overwrite-on-collision behaviour is fine; we'd
    update both sides only if a curator stored the same tt-id on a film
    AND a series, which would be a data bug to fix separately.
    """
    mapping: dict[str, str] = {}
    for table in TABLES:
        cur.execute(
            f"SELECT imdb_id FROM {table} WHERE imdb_id IS NOT NULL"
        )
        for (imdb_id,) in cur:
            mapping[imdb_id] = table
    return mapping


def _download_tsv(cache_dir: Path) -> tuple[bytes | None, str]:
    """Download the TSV, using Last-Modified to short-circuit when fresh.

    Returns (body, last_modified). When the remote returns 304, `body` is
    None and the cached file is used instead (loaded by the caller).
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    header_path = cache_dir / HEADER_FILE_NAME
    cached_last_modified = ""
    if header_path.exists():
        cached_last_modified = header_path.read_text().strip()

    headers = {}
    if cached_last_modified:
        headers["If-Modified-Since"] = cached_last_modified

    r = requests.get(TSV_URL, headers=headers, timeout=60, stream=True)
    if r.status_code == 304:
        logging.info(
            "TSV unchanged since %s — using cached copy", cached_last_modified
        )
        return None, cached_last_modified
    r.raise_for_status()

    body = r.content
    last_modified = r.headers.get("Last-Modified", "")
    # Only overwrite the header cache when IMDb actually returned one —
    # otherwise we'd wipe a previously valid timestamp and break
    # conditional GET on subsequent runs.
    if last_modified:
        header_path.write_text(last_modified)
    (cache_dir / TSV_FILE_NAME).write_bytes(body)
    logging.info(
        "Downloaded TSV (%.1f MB, Last-Modified: %s)",
        len(body) / 1024 / 1024,
        last_modified or "<missing>",
    )
    return body, last_modified


def _load_cached_tsv(cache_dir: Path) -> bytes:
    p = cache_dir / TSV_FILE_NAME
    if not p.exists():
        raise SystemExit(
            f"304 from IMDb but cached TSV missing at {p} — clear cache and re-run"
        )
    return p.read_bytes()


def _stream_ratings(gz_body: bytes):
    """Yield (tconst, average_rating, num_votes) tuples from the gzip TSV.

    The header line is `tconst\taverageRating\tnumVotes`; the parse is
    intentionally permissive — a malformed row is skipped rather than
    aborting the whole sync.
    """
    with gzip.GzipFile(fileobj=io.BytesIO(gz_body)) as gz:
        # IMDb writes ASCII-only, but request the decode to be defensive.
        header = gz.readline().decode("utf-8").rstrip()
        if not header.startswith("tconst"):
            raise SystemExit(f"unexpected TSV header: {header!r}")
        for raw in gz:
            try:
                parts = raw.decode("utf-8").rstrip().split("\t")
                if len(parts) != 3:
                    continue
                tconst, avg, votes = parts
                yield tconst, float(avg), int(votes)
            except (ValueError, UnicodeDecodeError):
                continue


def _flush(cur, table: str, batch: list[tuple]) -> None:
    """Apply one batch of (rating, votes, imdb_id) updates for `table`."""
    if not batch:
        return
    # execute_values defaults to page_size=100, which would split each
    # flush into multiple round-trips; pass the real batch length so the
    # whole batch ships in one UPDATE.
    psycopg2.extras.execute_values(
        cur,
        f"""
        UPDATE {table} AS t
           SET imdb_rating = v.rating,
               imdb_votes = v.votes,
               imdb_rating_synced_at = now()
          FROM (VALUES %s) AS v(rating, votes, imdb_id)
         WHERE t.imdb_id = v.imdb_id
        """,
        batch,
        template="(%s, %s, %s)",
        page_size=len(batch),
    )


def sync(conn, cache_dir: Path, dry_run: bool, force: bool) -> dict[str, int]:
    cur = conn.cursor()
    mapping = _load_our_imdb_ids(cur)
    logging.info(
        "Loaded %d IMDb IDs from our DB (films+series+tv_shows)", len(mapping)
    )

    body, _ = _download_tsv(cache_dir)
    if body is None:
        if not force:
            logging.info(
                "TSV unchanged from previous run — exiting without touching DB "
                "(use --force to re-sync from cache anyway)"
            )
            return {t: 0 for t in TABLES}
        body = _load_cached_tsv(cache_dir)

    # One batch per table — keeps the UPDATE selective and lets us report
    # per-table counts at the end. 1000 is large enough to avoid overhead
    # per round-trip and small enough to stay friendly to the WAL.
    BATCH_SIZE = 1000
    batches: dict[str, list[tuple]] = {t: [] for t in TABLES}
    counts: dict[str, int] = {t: 0 for t in TABLES}

    started = time.monotonic()
    for tconst, avg, votes in _stream_ratings(body):
        table = mapping.get(tconst)
        if not table:
            continue
        batches[table].append((avg, votes, tconst))
        counts[table] += 1
        if len(batches[table]) >= BATCH_SIZE and not dry_run:
            _flush(cur, table, batches[table])
            batches[table].clear()

    if not dry_run:
        for table, batch in batches.items():
            _flush(cur, table, batch)
        conn.commit()
    else:
        logging.info("[dry-run] skipping UPDATE + COMMIT")

    elapsed = time.monotonic() - started
    logging.info(
        "Done in %.1fs — films: %d, series: %d, tv_shows: %d",
        elapsed,
        counts["films"],
        counts["series"],
        counts["tv_shows"],
    )
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--dry-run", action="store_true",
                        help="parse + match but do not UPDATE or COMMIT")
    parser.add_argument("--force", action="store_true",
                        help="re-sync from cached TSV even when IMDb returns 304")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn:
        raise SystemExit("DATABASE_URL required")

    conn = psycopg2.connect(dsn)
    try:
        sync(conn, Path(args.cache_dir), args.dry_run, args.force)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
