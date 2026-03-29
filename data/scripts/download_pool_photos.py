#!/usr/bin/env python3
"""Download pool photos from jduplavat.cz.

Photos saved as: data/images/pools/{slug}-{n}.webp
Maintains link to pool data via slug naming.
"""

import os
import re
import time
import requests
import psycopg2
from bs4 import BeautifulSoup

STAGING_URL = "postgresql:///cr_staging"
BASE_URL = "https://jduplavat.cz"
OUT_DIR = "/home/jirka/Olbrasoft/cr/data/images/pools"
UA = "Mozilla/5.0 (X11; Linux x86_64) CeskaRepublikaWiki/1.0"
HEADERS = {"User-Agent": UA}


def download_pool_photos(slug):
    """Scrape and download photos for a single pool."""
    url = f"{BASE_URL}/place/{slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return 0
    except Exception:
        return 0

    # Find all pool images
    soup = BeautifulSoup(resp.text, "html.parser")
    img_urls = []
    for img in soup.select("img"):
        src = img.get("src", "")
        if "/uploads/pools/" in src and "image" in src:
            if src.startswith("/"):
                src = BASE_URL + src
            img_urls.append(src)

    # Also check for images in srcset or data-src
    for img in soup.select("img[data-src]"):
        src = img.get("data-src", "")
        if "/uploads/pools/" in src:
            if src.startswith("/"):
                src = BASE_URL + src
            img_urls.append(src)

    # Deduplicate
    img_urls = list(dict.fromkeys(img_urls))

    downloaded = 0
    for i, img_url in enumerate(img_urls):
        out_path = os.path.join(OUT_DIR, f"{slug}-{i+1}.webp")
        if os.path.exists(out_path) and os.path.getsize(out_path) > 500:
            downloaded += 1
            continue

        try:
            img_resp = requests.get(img_url, headers=HEADERS, timeout=30)
            if img_resp.status_code == 200 and len(img_resp.content) > 500:
                # Check if already webp or needs conversion
                ext = img_url.rsplit(".", 1)[-1].lower()
                if ext == "webp":
                    with open(out_path, "wb") as f:
                        f.write(img_resp.content)
                    downloaded += 1
                else:
                    # Save as original format, then convert
                    tmp_path = out_path.replace(".webp", f".{ext}")
                    with open(tmp_path, "wb") as f:
                        f.write(img_resp.content)
                    # Convert to webp
                    os.system(f'cwebp -q 85 -quiet "{tmp_path}" -o "{out_path}" 2>/dev/null')
                    if os.path.exists(out_path) and os.path.getsize(out_path) > 100:
                        os.remove(tmp_path)
                        downloaded += 1
                    else:
                        # Keep original if conversion fails
                        os.rename(tmp_path, out_path)
                        downloaded += 1
        except Exception:
            continue

        time.sleep(0.5)

    return downloaded


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()
    cur.execute("SELECT slug, name FROM pools ORDER BY slug")
    pools = cur.fetchall()
    cur.close()
    conn.close()

    total = len(pools)
    print(f"Downloading photos for {total} pools...", flush=True)

    total_photos = 0
    pools_with_photos = 0

    for i, (slug, name) in enumerate(pools):
        # Skip if already have photos for this slug
        existing = [f for f in os.listdir(OUT_DIR) if f.startswith(f"{slug}-")] if os.path.exists(OUT_DIR) else []
        if existing:
            total_photos += len(existing)
            pools_with_photos += 1
            if (i + 1) % 20 == 0:
                print(f"  Progress: {i+1}/{total} (photos: {total_photos}, pools with photos: {pools_with_photos})", flush=True)
            continue

        count = download_pool_photos(slug)
        total_photos += count
        if count > 0:
            pools_with_photos += 1

        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{total} (photos: {total_photos}, pools with photos: {pools_with_photos})", flush=True)

        time.sleep(2)  # Polite pause

    print(f"\nDone! Total photos: {total_photos}, Pools with photos: {pools_with_photos}/{total}", flush=True)


if __name__ == "__main__":
    main()
