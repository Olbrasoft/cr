#!/usr/bin/env python3
"""Backfill TMDB covers to R2 for films imported from sledujteto.cz (#545).

The sledujteto bulk-import (`scripts/import-sledujteto-films.py`) wrote the
`films.tmdb_poster_path` column but did not upload the actual cover bytes
to R2 — the listing page therefore shows black placeholders until those
bytes land at `cr-images:films/{id}/cover.webp` + `cover-large.webp`.

For each film with `sledujteto_primary_file_id IS NOT NULL AND
tmdb_poster_path IS NOT NULL` this script:

  1. Fetches TMDB w780 poster (single HTTP GET, PIL-validated).
  2. Converts to WebP at two display sizes:
       - `{id}/cover.webp`        200x300 (listing thumbnails + detail poster)
       - `{id}/cover-large.webp`  780x1170 (lightbox zoom)
     Both WebP quality 85, method 6 — same as the auto-import pipeline.
  3. Uploads both to R2 under `cr-images/films/{id}/…` via
     `npx wrangler r2 object put --remote`. The `cr-img-proxy` Worker
     then serves them at `/img/films/{id}/cover.webp`, which is what the
     listing-page handler in `cr-web/src/handlers/films.rs` routes to.

Parallelised with a thread pool because the bottleneck is wrangler's
Node spin-up (≈500-800 ms/call). Local disk writes are incidental and
also serve as the idempotency check for re-runs — if both files exist
we skip the TMDB fetch and go straight to upload.

Usage:
  DATABASE_URL=postgres://cr:...@localhost:5433/cr \\
      python3 scripts/backfill-sledujteto-covers.py \\
          --out-dir data/movies/covers-webp \\
          --jobs 8 \\
          --limit 10
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import os
import subprocess
import sys
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. pip install psycopg2-binary",
          file=sys.stderr)
    sys.exit(2)

_SCRIPTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPTS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from scripts.auto_import.cover_downloader import (  # noqa: E402
    _cover_paths,
    download_cover,
)

log = logging.getLogger("backfill-sledujteto-covers")


def upload_cover_to_r2(
    film_id: int,
    cover_dir: Path,
    wrangler_cwd: Path,
) -> tuple[bool, str]:
    """Upload both cover files for a film to `cr-images/films/{id}/…`.

    Returns `(ok, message)`. The caller expects `ok=False` to mean
    "leave the local files alone so a later re-run can retry".
    """
    small_path, large_path = _cover_paths(cover_dir, film_id)
    if not (small_path.exists() and large_path.exists()):
        return False, f"local files missing for id={film_id}"

    for local_path, variant in [
        (small_path, "cover.webp"),
        (large_path, "cover-large.webp"),
    ]:
        key = f"cr-images/films/{film_id}/{variant}"
        # --file must be an absolute path: wrangler resolves it relative
        # to its own CWD (workers/img-proxy) which is not where the
        # WebP bytes actually live (data/movies/covers-webp/).
        abs_path = Path(local_path).resolve()
        result = subprocess.run(
            [
                "npx",
                "wrangler",
                "r2",
                "object",
                "put",
                key,
                f"--file={abs_path}",
                "--content-type=image/webp",
                "--remote",
            ],
            capture_output=True,
            text=True,
            cwd=wrangler_cwd,
        )
        # Wrangler prints "Upload complete!" on success and non-zero
        # return on failure. Capture stderr for debuggable log output.
        if result.returncode != 0 or "Upload complete" not in result.stdout:
            tail = (result.stderr or result.stdout or "").strip().splitlines()[-3:]
            return False, f"wrangler put failed for {key}: {' | '.join(tail)}"

    return True, "ok"


def process_film(
    film_id: int,
    poster_path: str,
    cover_dir: Path,
    wrangler_cwd: Path,
) -> tuple[int, str]:
    """Download → convert → upload one film. Thread-safe (download_cover
    buffers the TMDB response body so Pillow never touches a shared
    socket — see the #574 fix in cover_downloader.py)."""
    dl_result = download_cover(poster_path, film_id, cover_dir)
    if dl_result == "failed":
        return film_id, "download_failed"

    ok, msg = upload_cover_to_r2(film_id, cover_dir, wrangler_cwd)
    if not ok:
        log.warning("upload_failed id=%d: %s", film_id, msg)
        return film_id, "upload_failed"

    return film_id, "ok"


def fetch_candidate_films(dsn: str, limit: int | None) -> list[tuple[int, str]]:
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        # ORDER BY added_at DESC: freshly-imported sledujteto films are at
        # the top of /filmy-online/ (default sort by `added_at DESC`), so
        # processing them first fills visible thumbnails before older rows.
        # Covers backfilled for never-visited films are equally cached on R2
        # either way.
        sql = (
            "SELECT id, tmdb_poster_path FROM films "
            "WHERE sledujteto_primary_file_id IS NOT NULL "
            "AND tmdb_poster_path IS NOT NULL "
            "ORDER BY added_at DESC, id DESC"
        )
        if limit:
            sql += f" LIMIT {int(limit)}"
        cur.execute(sql)
        return list(cur.fetchall())
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--out-dir",
        default="data/movies/covers-webp",
        help="Local WebP cache dir (id-keyed: {id}/cover.webp + cover-large.webp)",
    )
    ap.add_argument(
        "--wrangler-cwd",
        default="workers/img-proxy",
        help="Directory containing wrangler.toml (binds cr-images bucket)",
    )
    ap.add_argument(
        "--jobs",
        type=int,
        default=4,
        help="Parallel worker count (default 4; bump to 8+ on fast network)",
    )
    ap.add_argument("--limit", type=int, help="Process only the first N films")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL env var is required")
        return 2

    cover_dir = Path(args.out_dir)
    cover_dir.mkdir(parents=True, exist_ok=True)
    wrangler_cwd = Path(args.wrangler_cwd).resolve()
    if not (wrangler_cwd / "wrangler.toml").is_file():
        log.error("wrangler.toml not found in %s", wrangler_cwd)
        return 2

    films = fetch_candidate_films(dsn, args.limit)
    log.info("processing %d films with %d workers", len(films), args.jobs)

    stats = {"ok": 0, "download_failed": 0, "upload_failed": 0}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futures = [
            ex.submit(process_film, fid, path, cover_dir, wrangler_cwd)
            for fid, path in films
        ]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            _fid, status = fut.result()
            stats[status] += 1
            if i % 25 == 0 or i == len(films):
                log.info(
                    "progress: %d/%d ok=%d dl_fail=%d up_fail=%d",
                    i,
                    len(films),
                    stats["ok"],
                    stats["download_failed"],
                    stats["upload_failed"],
                )

    log.info("DONE: %s", stats)
    return 0 if stats["ok"] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
