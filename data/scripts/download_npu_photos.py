#!/usr/bin/env python3
"""Download NPÚ monument preview photos and convert to WebP.

Downloads from https://iispp.npu.cz/mis_public/preview.htm?id={mediumId}
Saves as data/images/landmarks/{catalog_id}.webp
Supports resume — skips already downloaded files.
"""

import os
import subprocess
import sys
import time
import psycopg2

STAGING_URL = os.environ.get("STAGING_DATABASE_URL", "postgres://cr_dev_user:cr_dev_2026@localhost/cr_staging")
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "images", "landmarks")
UA = "Mozilla/5.0 (X11; Linux x86_64) CeskaRepublikaWiki/1.0"


def download_and_convert(medium_id, catalog_id):
    webp_path = os.path.join(OUT_DIR, f"{catalog_id}.webp")
    if os.path.exists(webp_path) and os.path.getsize(webp_path) > 100:
        return "skip"

    jpg_path = os.path.join(OUT_DIR, f"{catalog_id}.jpg")
    url = f"https://iispp.npu.cz/mis_public/preview.htm?id={medium_id}"

    try:
        result = subprocess.run(
            ["curl", "-s", "-L", "--max-time", "45", "-o", jpg_path, "-H", f"User-Agent: {UA}", url],
            capture_output=True, timeout=60,
        )
    except (subprocess.TimeoutExpired, Exception):
        if os.path.exists(jpg_path):
            os.remove(jpg_path)
        return "fail"

    if not os.path.exists(jpg_path) or os.path.getsize(jpg_path) < 500:
        if os.path.exists(jpg_path):
            os.remove(jpg_path)
        return "fail"

    # Convert to WebP
    try:
        conv = subprocess.run(
            ["cwebp", "-q", "85", "-quiet", jpg_path, "-o", webp_path],
            capture_output=True, timeout=60,
        )
    except subprocess.TimeoutExpired:
        if os.path.exists(jpg_path):
            os.remove(jpg_path)
        return "fail"

    if os.path.exists(webp_path) and os.path.getsize(webp_path) > 100:
        os.remove(jpg_path)
        return "ok"
    else:
        if os.path.exists(jpg_path):
            os.remove(jpg_path)
        return "fail"


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    cur.execute(
        "SELECT catalog_id, medium_id FROM npu_monuments WHERE medium_id IS NOT NULL ORDER BY catalog_id"
    )
    rows = cur.fetchall()
    conn.close()

    total = len(rows)
    downloaded = 0
    skipped = 0
    failed = 0

    print(f"Photos to process: {total}", flush=True)

    for i, (catalog_id, medium_id) in enumerate(rows):
        status = download_and_convert(medium_id, catalog_id)

        if status == "ok":
            downloaded += 1
        elif status == "skip":
            skipped += 1
        else:
            failed += 1

        if (i + 1) % 500 == 0:
            print(
                f"  Progress: {i+1}/{total} "
                f"(downloaded: {downloaded}, skipped: {skipped}, failed: {failed})",
                flush=True,
            )

        if status != "skip":
            time.sleep(0.3)

    print(
        f"\nDone! Downloaded: {downloaded}, Skipped: {skipped}, "
        f"Failed: {failed}, Total: {total}",
        flush=True,
    )


if __name__ == "__main__":
    main()
