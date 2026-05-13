#!/usr/bin/env python3
"""Backfill person profile photos to R2 for series cast + crew.

The `people` table stores `profile_filename = 'p{tmdb_id}.webp'` for every
person TMDB knew had a profile photo, but the bytes were never persisted on
production — `/opt/cr/data/series/people/` is empty and never bind-mounted
into the container, so `series_person_image` returned 404 for every photo.

This script mirrors the films/series cover pipeline (`backfill-sledujteto-
covers.py`):

  1. SELECT id, tmdb_id, profile_filename FROM people WHERE
     profile_filename IS NOT NULL — these are TMDB-knew-they-had-a-photo
     rows. If TMDB no longer returns one we'll NULL the column so the
     template renders the placeholder card.
  2. For each, GET /person/{tmdb_id} from TMDB to discover the current
     `profile_path` (we never stored it; only the synthesized filename).
  3. Download `https://image.tmdb.org/t/p/w185{profile_path}`, validate
     it's a portrait image, convert to WebP 200×300 quality 85.
  4. Upload to R2 at `cr-images:people/{tmdb_id}.webp` via the S3-compatible
     API (boto3 + R2 access key, same auth path used elsewhere). The
     `cr-img-proxy` Worker serves it at `/img/people/{tmdb_id}.webp`,
     which is what the patched `series_person_image` handler proxies.
  5. On failure (TMDB has no photo anymore, download/upload error) the
     row's `profile_filename` is set to NULL so the template falls
     through to the initials placeholder.

Local-file cache at `--out-dir` (default `data/series/people/`) is purely
an idempotency hint — re-running skips the TMDB fetch if the WebP exists
on disk. The R2 upload still runs (cheap; wrangler put is idempotent).

Usage:
  DATABASE_URL=postgres://...@localhost/cr_dev \\
  TMDB_API_KEY=... \\
  R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... \\
      python3 scripts/backfill-person-photos.py \\
          --jobs 8 \\
          --limit 50

The R2_* env vars come straight from `.env` (loaded by python-dotenv when
invoking via `dotenv run`). They authorize against the `cr-images` bucket
via the S3-compatible endpoint, mirroring `backup-db.sh`.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import io
import logging
import os
import sys
import time
from pathlib import Path

try:
    import boto3
    import psycopg2
    import requests
    from botocore.config import Config as BotoConfig
except ImportError as e:
    print(f"ERROR: missing dependency ({e.name}). "
          "pip install psycopg2-binary requests boto3",
          file=sys.stderr)
    sys.exit(2)

try:
    from PIL import Image
except ImportError:
    print("ERROR: Pillow not installed. pip install Pillow", file=sys.stderr)
    sys.exit(2)

log = logging.getLogger("backfill-person-photos")

TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p"
DEFAULT_TIMEOUT = 20

# Portrait integrity bounds. TMDB w185 is ~185×278; w300 is ~300×450. We
# accept anything taller than wide with a reasonable minimum side. 1×1
# tracking GIFs and landscape stills get rejected.
_MIN_SIDE = 80
_MIN_ASPECT = 0.4   # w/h — portrait
_MAX_ASPECT = 1.1   # near-square OK


# Sentinel signalling "TMDB explicitly says this person has no photo".
# Distinct from None (transient error) so the caller doesn't permanently
# NULL `profile_filename` for what's actually a network blip / 5xx /
# rate-limit exhaustion.
_NO_PHOTO = "__no_photo__"


def _fetch_tmdb_profile_path(
    session: requests.Session, tmdb_id: int, api_key: str
) -> str | None:
    """GET /person/{tmdb_id} → one of:
        - profile path string (e.g. "/abc.jpg") — TMDB has a photo
        - `_NO_PHOTO`                           — TMDB definitively has no photo
                                                  (HTTP 200 + profile_path is null/missing,
                                                  or HTTP 404)
        - None                                  — transient error; the row should
                                                  stay marked as "still needs a
                                                  photo" so a later re-run retries.

    Distinguishing transient errors from confirmed-missing is what keeps
    a TMDB outage from permanently wiping `profile_filename` rows the
    template uses to decide whether to render the WebP or the initials
    placeholder.
    """
    url = f"{TMDB_API_BASE}/person/{tmdb_id}"
    for attempt in range(3):
        try:
            r = session.get(url, params={"api_key": api_key}, timeout=DEFAULT_TIMEOUT)
        except requests.RequestException as e:
            log.warning("tmdb_id=%d /person fetch failed: %s",
                        tmdb_id, type(e).__name__)
            time.sleep(2 ** attempt)
            continue
        if r.status_code == 404:
            # Person id is gone from TMDB — definitively no photo to fetch.
            return _NO_PHOTO
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            log.warning("tmdb_id=%d rate-limited; sleeping %ds", tmdb_id, wait)
            time.sleep(wait)
            continue
        if r.status_code != 200:
            log.warning("tmdb_id=%d /person returned HTTP %d", tmdb_id, r.status_code)
            return None  # transient — caller keeps filename, retry next run
        try:
            data = r.json()
        except ValueError:
            return None  # transient — malformed body
        path = data.get("profile_path")
        # 200 + null profile_path is TMDB definitively saying "no photo".
        return path if path else _NO_PHOTO
    # Out of retries on network failures — leave the row alone.
    return None


def _download_and_convert(
    session: requests.Session,
    profile_path: str,
    tmdb_id: int,
    out_path: Path,
) -> bool:
    """Fetch w185 from TMDB → WebP 200×300 → out_path. Returns success."""
    url = f"{TMDB_IMG_BASE}/w185{profile_path}"
    try:
        r = session.get(url, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as e:
        log.warning("tmdb_id=%d image fetch failed: %s",
                    tmdb_id, type(e).__name__)
        return False
    if r.status_code != 200 or len(r.content) < 500:
        log.warning("tmdb_id=%d image HTTP %d (size=%d)",
                    tmdb_id, r.status_code, len(r.content))
        return False

    try:
        img = Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception as e:
        log.warning("tmdb_id=%d decode failed: %s", tmdb_id, e)
        return False

    w, h = img.size
    if w < _MIN_SIDE or h < _MIN_SIDE:
        log.warning("tmdb_id=%d image too small: %dx%d", tmdb_id, w, h)
        return False
    aspect = w / h
    if not (_MIN_ASPECT <= aspect <= _MAX_ASPECT):
        log.warning("tmdb_id=%d aspect out of range: %dx%d (%.2f)",
                    tmdb_id, w, h, aspect)
        return False

    # 200×300 portrait — matches the cards on series detail (.person-photo
    # is `aspect-ratio: 2/3` so this is exact). Pillow's thumbnail keeps
    # the larger dim within the box, preserving aspect.
    img.thumbnail((200, 300), Image.LANCZOS)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, "WEBP", quality=85, method=6)
    return True


def _make_r2_client() -> "boto3.client":
    account_id = os.environ.get("R2_ACCOUNT_ID", "").strip()
    key_id = os.environ.get("R2_ACCESS_KEY_ID", "").strip()
    secret = os.environ.get("R2_SECRET_ACCESS_KEY", "").strip()
    if not (account_id and key_id and secret):
        raise SystemExit(
            "R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY required"
        )
    endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        # R2 requires `auto` for region in the S3 SDK.
        region_name="auto",
        # signature_version: R2 wants SigV4 (boto3 default).
        config=BotoConfig(signature_version="s3v4", retries={"max_attempts": 3}),
    )


def _upload_to_r2(s3, tmdb_id: int, local_path: Path) -> tuple[bool, str]:
    key = f"people/{tmdb_id}.webp"
    try:
        with open(local_path, "rb") as f:
            s3.put_object(
                Bucket="cr-images",
                Key=key,
                Body=f,
                ContentType="image/webp",
                CacheControl="public, max-age=31536000, immutable",
            )
    except Exception as e:
        return False, f"put_object failed for {key}: {type(e).__name__}: {e}"
    return True, "ok"


def _process_person(
    person_id: int,
    tmdb_id: int,
    out_dir: Path,
    s3,
    api_key: str,
) -> tuple[int, str]:
    """Returns (person_id, status). Only `no_tmdb_photo` is treated as a
    definitive negative result that the caller propagates to NULL the
    `people.profile_filename` row. Every other failure (transient TMDB,
    download/decode hiccup, R2 outage) leaves the filename intact so a
    future re-run can fill it in. The four-way split lets us report
    cleanly in the summary line.

        'ok'                 — R2 upload succeeded
        'no_tmdb_photo'      — TMDB confirms no photo (200+null, or 404)
                               → caller NULLs the row
        'tmdb_error'         — transient TMDB error (retry next run, keep filename)
        'download_failed'    — fetch or decode failed (retry next run, keep filename)
        'upload_failed'      — R2 put_object failed (retry next run, keep filename)
    """
    out_path = out_dir / f"{tmdb_id}.webp"
    session = requests.Session()

    if not out_path.exists():
        profile_path = _fetch_tmdb_profile_path(session, tmdb_id, api_key)
        if profile_path is None:
            return person_id, "tmdb_error"
        if profile_path == _NO_PHOTO:
            return person_id, "no_tmdb_photo"
        if not _download_and_convert(session, profile_path, tmdb_id, out_path):
            return person_id, "download_failed"

    # Dry-run validates the TMDB → WebP pipeline without touching R2.
    if s3 is None:
        return person_id, "ok"

    ok, msg = _upload_to_r2(s3, tmdb_id, out_path)
    if not ok:
        log.warning("upload_failed tmdb_id=%d: %s", tmdb_id, msg)
        return person_id, "upload_failed"
    return person_id, "ok"


def _fetch_candidates(
    dsn: str, limit: int | None
) -> list[tuple[int, int]]:
    """Return [(person_id, tmdb_id), ...] of rows still needing a photo.

    Joins to series_actors/directors so we process people who are actually
    referenced first — orphaned rows (no series_id link) wait. Frequency
    of citation (more series = more visible) breaks ties.
    """
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        sql = (
            "SELECT p.id, p.tmdb_id FROM people p "
            "LEFT JOIN ("
            "  SELECT person_id, COUNT(*) AS n FROM series_actors GROUP BY 1"
            "  UNION ALL "
            "  SELECT person_id, COUNT(*) AS n FROM series_directors GROUP BY 1"
            ") refs ON refs.person_id = p.id "
            "WHERE p.profile_filename IS NOT NULL AND p.tmdb_id IS NOT NULL "
            "GROUP BY p.id, p.tmdb_id "
            "ORDER BY SUM(refs.n) DESC NULLS LAST, p.id"
        )
        if limit is not None:
            cur.execute(sql + " LIMIT %s", (int(limit),))
        else:
            cur.execute(sql)
        return list(cur.fetchall())
    finally:
        conn.close()


def _null_filename(dsn: str, person_ids: list[int]) -> None:
    if not person_ids:
        return
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE people SET profile_filename = NULL WHERE id = ANY(%s)",
            (person_ids,),
        )
        conn.commit()
        log.info("nulled profile_filename for %d persons (no R2 photo)",
                 len(person_ids))
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--out-dir", default="data/series/people",
        help="Local WebP cache dir (filenames: {tmdb_id}.webp)",
    )
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--limit", type=int, help="Process only first N rows")
    ap.add_argument("--dry-run", action="store_true",
                    help="Skip R2 upload and DB writes (test TMDB pipeline only)")
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
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not api_key:
        log.error("TMDB_API_KEY env var is required")
        return 2

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    s3 = None if args.dry_run else _make_r2_client()

    rows = _fetch_candidates(dsn, args.limit)
    log.info("processing %d persons with %d workers", len(rows), args.jobs)

    stats = {
        "ok": 0, "no_tmdb_photo": 0,
        "tmdb_error": 0, "download_failed": 0, "upload_failed": 0,
    }
    null_ids: list[int] = []

    if args.dry_run:
        log.warning("--dry-run: no R2 upload, no DB writes")

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
        futures = [
            ex.submit(_process_person, pid, tmdb_id, out_dir, s3, api_key)
            for pid, tmdb_id in rows
        ]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            person_id, status = fut.result()
            stats[status] += 1
            # Only confirmed-missing-on-TMDB NULLs the row. Transient
            # statuses (tmdb_error / download_failed / upload_failed) keep
            # `profile_filename` so the next backfill run retries them; a
            # TMDB outage that produced thousands of tmdb_error responses
            # would otherwise permanently wipe valid photo references.
            if status == "no_tmdb_photo":
                null_ids.append(person_id)
            if i % 50 == 0 or i == len(rows):
                log.info(
                    "progress %d/%d ok=%d no_tmdb=%d tmdb_err=%d dl_fail=%d up_fail=%d",
                    i, len(rows), stats["ok"], stats["no_tmdb_photo"],
                    stats["tmdb_error"], stats["download_failed"],
                    stats["upload_failed"],
                )

    if not args.dry_run:
        _null_filename(dsn, null_ids)

    log.info("DONE: %s", stats)
    # Non-zero exit means something needs attention next run: TMDB
    # outage, R2 token rotated, disk full. 'no_tmdb_photo' is expected
    # steady-state for actors TMDB depublished.
    transient = (
        stats["tmdb_error"] + stats["download_failed"] + stats["upload_failed"]
    )
    return 0 if transient == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
