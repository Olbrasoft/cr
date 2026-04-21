#!/usr/bin/env python3
"""Backfill for issue #574 — re-download wrong-content covers from the
prehraj.to bulk import (#524) for the ~135 exotic-title cohort.

Root cause is the `stream=True` + `Image.open(r.raw)` pattern in
`scripts/auto_import/cover_downloader.py`, which under ThreadPoolExecutor
parallelism spliced bytes from concurrent TMDB responses — films got
WebPs of unrelated films' posters. That decoder call is now fed from
`BytesIO(r.content)` (single-owned buffer) in the same file.

What this script does:
  1. Select every `films` row with `cover_filename` of the "film-NNN"
     shape that the exotic-cohort fallback produced.
  2. For each, re-hit TMDB /movie/{tmdb_id}?language=en-US for the
     current `poster_path` (language=en-US guarantees a Latin-script
     poster where one exists; cs-CZ can echo the original-language
     poster, which is fine for accuracy but not the stated fix).
  3. Redownload via the fixed `download_cover` with `overwrite=True`
     into `data/movies/covers-webp/` so the local run has the corrected
     WebP on disk.
  4. Optionally upload both the `{cover_filename}.webp` and its
     `{cover_filename}-large.webp` sibling to R2 (`cr-images/films/…`)
     so the live site serves the corrected image.
  5. Optionally purge the CF cache for the affected URLs.

Usage:
    DATABASE_URL=... TMDB_API_KEY=... python3 scripts/redownload-exotic-covers.py --dry-run
    DATABASE_URL=... TMDB_API_KEY=... python3 scripts/redownload-exotic-covers.py --apply
    DATABASE_URL=... TMDB_API_KEY=... R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... \\
        R2_SECRET_ACCESS_KEY=... R2_BUCKET=cr-images \\
        CF_ZONE_ID=... CF_CACHE_PURGE_TOKEN=... \\
        python3 scripts/redownload-exotic-covers.py --apply --upload-r2 --purge-cf

Selection predicate: `cover_filename ~ '^film-[0-9]+$'`. This is the
exact shape the old enricher produced when `_slugify(title)` returned
an empty string (all exotic glyphs). It does not catch any legitimate
cover, because Latin slugs always contain at least one letter before
the first digit.

Re-run semantics: safe but not idempotent. Re-running `--apply` revisits
every row whose `cover_filename` still matches the selection predicate
and always passes `overwrite=True` to `download_cover`; if
`--upload-r2` and/or `--purge-cf` are set, those operations are repeated
too. That's intentional — the script is a one-shot data-repair tool, so
"run twice and nothing bad happens" (same bytes, same keys, same CF
purge calls) is the contract rather than "skip already-done rows".
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psycopg2
import requests

_SCRIPTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPTS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass

from scripts.auto_import.cover_downloader import download_cover  # noqa: E402

DB_URL = os.environ.get("DATABASE_URL", "").strip()
TMDB_KEY = os.environ.get("TMDB_API_KEY", "").strip()
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_SLEEP = 0.1

DEFAULT_OUT_DIR = _REPO_ROOT / "data" / "movies" / "covers-webp"


def _tmdb_poster_path(tmdb_id: int) -> str | None:
    """Return en-US poster_path (Latin where available) for a movie id."""
    try:
        r = requests.get(
            f"{TMDB_BASE}/movie/{tmdb_id}",
            params={"api_key": TMDB_KEY, "language": "en-US"},
            timeout=15,
        )
    except requests.RequestException as e:
        # Scrub — `requests.RequestException` can include the full URL with
        # `?api_key=...` in its repr, which we never want in stderr / logs.
        print(f"  [net] tmdb_id={tmdb_id}: {type(e).__name__}", file=sys.stderr)
        return None
    time.sleep(TMDB_SLEEP)
    if r.status_code != 200:
        print(f"  [HTTP {r.status_code}] tmdb_id={tmdb_id}", file=sys.stderr)
        return None
    try:
        return (r.json().get("poster_path") or "").strip() or None
    except ValueError:
        return None


def _rclone_r2_config() -> Path | None:
    """Write a throwaway rclone config for the `cr-images` bucket.

    Returns the path to a temp file the caller is responsible for cleaning
    up. Returns None if any of the R2_* env vars are missing.
    """
    account = os.environ.get("R2_ACCOUNT_ID", "").strip()
    key     = os.environ.get("R2_ACCESS_KEY_ID", "").strip()
    secret  = os.environ.get("R2_SECRET_ACCESS_KEY", "").strip()
    if not (account and key and secret):
        return None
    cfg = tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False)
    cfg.write(
        "[r2]\n"
        "type = s3\n"
        "provider = Cloudflare\n"
        f"access_key_id = {key}\n"
        f"secret_access_key = {secret}\n"
        f"endpoint = https://{account}.r2.cloudflarestorage.com\n"
        "region = auto\n"
    )
    cfg.close()
    return Path(cfg.name)


def _r2_upload(rclone_cfg: Path, local: Path, r2_key: str) -> bool:
    """Upload a single file with rclone. Returns True on success."""
    bucket = os.environ.get("R2_BUCKET", "cr-images").strip()
    try:
        subprocess.run(
            ["rclone", "--config", str(rclone_cfg),
             "copyto", "--s3-no-check-bucket",
             str(local), f"r2:{bucket}/{r2_key}"],
            check=True, capture_output=True, text=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [rclone] {r2_key}: {e.stderr.strip()[:200]}", file=sys.stderr)
        return False


def _cf_purge(urls: list[str]) -> bool:
    """Purge specific URLs from Cloudflare CDN cache.

    Batches of up to 30 per API call (CF limit). Returns True on full
    success.
    """
    zone = os.environ.get("CF_ZONE_ID", "").strip()
    token = os.environ.get("CF_CACHE_PURGE_TOKEN", "").strip()
    if not (zone and token):
        print("  [cf] CF_ZONE_ID / CF_CACHE_PURGE_TOKEN not set — skipping purge",
              file=sys.stderr)
        return False
    ok = True
    for i in range(0, len(urls), 30):
        batch = urls[i:i + 30]
        try:
            r = requests.post(
                f"https://api.cloudflare.com/client/v4/zones/{zone}/purge_cache",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json"},
                json={"files": batch},
                timeout=20,
            )
        except requests.RequestException as e:
            print(f"  [cf] purge batch {i}: {type(e).__name__}", file=sys.stderr)
            ok = False
            continue
        # CF normally returns JSON but on an edge failure it can reply with
        # an HTML error page. Guard the decode so one bad batch doesn't
        # abort the whole purge loop.
        try:
            success = bool(r.json().get("success"))
        except ValueError:
            success = False
        if r.status_code != 200 or not success:
            print(f"  [cf] purge batch {i}: HTTP {r.status_code} {r.text[:200]}",
                  file=sys.stderr)
            ok = False
    return ok


def main() -> int:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true",
                   help="List films matched by the selector; no DB/file writes.")
    g.add_argument("--apply", action="store_true",
                   help="Re-download posters and overwrite the local WebPs.")
    p.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR),
                   help=f"Local covers dir (default: {DEFAULT_OUT_DIR})")
    p.add_argument("--upload-r2", action="store_true",
                   help="Upload each fixed cover (small + large) to R2 "
                        "bucket cr-images under films/. Requires R2_* env.")
    p.add_argument("--purge-cf", action="store_true",
                   help="Purge the fixed URLs from Cloudflare CDN cache. "
                        "Requires CF_ZONE_ID + CF_CACHE_PURGE_TOKEN.")
    p.add_argument("--limit", type=int, default=0,
                   help="Stop after N films (0 = all). Handy for a smoke run.")
    args = p.parse_args()

    if not DB_URL:
        sys.exit("ERROR: DATABASE_URL env var required.")
    if not TMDB_KEY:
        sys.exit("ERROR: TMDB_API_KEY env var required.")
    out_dir = Path(args.out_dir)

    # autocommit=True — we only SELECT here, and the loop below then does
    # minutes of network I/O per row (TMDB + R2 upload). Holding an open
    # transaction across all of that would leave an idle-in-transaction
    # session pinning a snapshot and blocking vacuum on `films`.
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """SELECT id, slug, cover_filename, tmdb_id, title
             FROM films
            WHERE cover_filename ~ '^film-[0-9]+$'
              AND tmdb_id IS NOT NULL
         ORDER BY id""",
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if args.limit:
        rows = rows[:args.limit]
    print(f"Matched {len(rows)} films with exotic-cohort cover_filename.")

    if args.dry_run:
        for row_id, slug, cf, tmdb_id, title in rows[:20]:
            print(f"  id={row_id} cf={cf} tmdb={tmdb_id} slug={slug} title={title!r}")
        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more")
        print("\n[dry-run] no writes.")
        return 0

    # --apply path
    rclone_cfg = _rclone_r2_config() if args.upload_r2 else None
    if args.upload_r2 and rclone_cfg is None:
        sys.exit("ERROR: --upload-r2 needs R2_ACCOUNT_ID/R2_ACCESS_KEY_ID/"
                 "R2_SECRET_ACCESS_KEY env vars.")

    ok = failed = skipped = 0
    purged_urls: list[str] = []
    try:
        for row_id, slug, cf, tmdb_id, title in rows:
            poster = _tmdb_poster_path(tmdb_id)
            if not poster:
                print(f"  [skip] id={row_id} tmdb={tmdb_id}: no poster_path")
                skipped += 1
                continue
            result = download_cover(poster, cf, out_dir, overwrite=True)
            if not result:
                print(f"  [fail] id={row_id} cf={cf} tmdb={tmdb_id}: download_cover returned None")
                failed += 1
                continue
            small_path, large_path = result
            if rclone_cfg is not None:
                uploaded = True
                # The Rust handler serves small covers from
                # `{covers_dir}/{cover_filename}.webp` on disk but the R2
                # bucket uses `films/{cover_filename}.webp` for the
                # browser-facing URL. Mirror the name under `films/`.
                if not _r2_upload(rclone_cfg, small_path, f"films/{cf}.webp"):
                    uploaded = False
                # Large variant — the Rust handler caches under
                # `{covers_dir}/large/{slug}.webp` (by slug, not by
                # cover_filename), so R2 mirror goes to
                # `films/large/{slug}.webp`.
                if not _r2_upload(rclone_cfg, large_path, f"films/large/{slug}.webp"):
                    uploaded = False
                if not uploaded:
                    # Local file is fixed, but R2 upload is what prod serves
                    # from — a partial R2 write leaves the wrong content
                    # live, so count this as a failure. Skip CF purge so the
                    # still-wrong URL doesn't get pulled from cache.
                    print(f"  [warn] id={row_id} local OK, R2 upload partial — counting as failed")
                    failed += 1
                    continue
            ok += 1
            if args.purge_cf:
                purged_urls.append(f"https://ceskarepublika.wiki/filmy-online/{slug}.webp")
                purged_urls.append(f"https://ceskarepublika.wiki/filmy-online/{slug}-large.webp")
            if ok % 20 == 0:
                print(f"  [{ok + failed + skipped}/{len(rows)}] ok={ok} fail={failed} skip={skipped}",
                      flush=True)
    finally:
        if rclone_cfg is not None and rclone_cfg.exists():
            rclone_cfg.unlink(missing_ok=True)

    print(f"\nDONE: ok={ok} fail={failed} skip={skipped}")

    if args.purge_cf and purged_urls:
        # Batch in 30s — CF limit.
        print(f"Purging {len(purged_urls)} URLs from CF cache...")
        _cf_purge(purged_urls)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
