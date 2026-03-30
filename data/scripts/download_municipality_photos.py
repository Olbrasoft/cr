#!/usr/bin/env python3
"""Download main Wikipedia photo for each Czech municipality.

For each municipality:
1. Get main image from cs.wikipedia.org (pageimages API)
2. Get Czech description from commons.wikimedia.org (extmetadata)
3. Download original image, convert to WebP
4. Store metadata in cr_staging.municipality_photos

Safe to restart — skips already downloaded municipalities.
"""

import os
import re
import sys
import time
import unicodedata
import subprocess
import psycopg2
import requests

STAGING_URL = "postgresql:///cr_staging"
IMG_DIR = "/home/jirka/Olbrasoft/cr/data/images/municipalities_wiki"
USER_AGENT = "CeskaRepublikaWiki/1.0 (info@ceskarepublika.wiki)"
PAUSE = 1.0  # seconds between Wikipedia requests

WIKI_API = "https://cs.wikipedia.org/w/api.php"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"


def make_slug(name):
    """Generate URL slug: no diacritics, lowercase, hyphen-separated."""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    # Remove trailing numbers (year, index)
    s = re.sub(r"\s*\d+\s*$", "", s)
    # Remove parenthetical like (3)
    s = re.sub(r"\s*\([^)]*\)\s*", " ", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:80].rstrip("-")


def get_main_image(title):
    """Get main image URL and filename from Czech Wikipedia."""
    params = {
        "action": "query",
        "titles": title,
        "prop": "pageimages",
        "piprop": "original|name",
        "format": "json",
    }
    resp = requests.get(WIKI_API, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
    if resp.status_code != 200:
        return None, None, None, None

    data = resp.json()
    for _, page in data.get("query", {}).get("pages", {}).items():
        orig = page.get("original", {})
        source = orig.get("source")
        width = orig.get("width", 0)
        height = orig.get("height", 0)
        pageimage = page.get("pageimage", "")
        if source:
            return source, pageimage, width, height
    return None, None, None, None


def get_image_metadata(filename):
    """Get Czech description and ObjectName from Wikimedia Commons."""
    params = {
        "action": "query",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "extmetadata",
        "iiextmetadatalanguage": "cs",
        "format": "json",
    }
    resp = requests.get(COMMONS_API, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
    if resp.status_code != 200:
        return "", ""

    data = resp.json()
    for _, page in data.get("query", {}).get("pages", {}).items():
        meta = page.get("imageinfo", [{}])[0].get("extmetadata", {})
        description = meta.get("ImageDescription", {}).get("value", "")
        object_name = meta.get("ObjectName", {}).get("value", "")
        # Strip HTML
        description = re.sub(r"<[^>]+>", "", description).strip()
        return description, object_name
    return "", ""


def download_and_convert(url, output_path):
    """Download image and convert to WebP."""
    tmp_path = output_path + ".tmp"
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60, stream=True)
        if resp.status_code != 200:
            return False

        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)

        result = subprocess.run(
            ["convert", tmp_path, "-quality", "85", "-resize", "2048x2048>", output_path],
            capture_output=True, timeout=30,
        )
        os.unlink(tmp_path)
        return result.returncode == 0

    except Exception as e:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        print(f"    Download error: {e}")
        return False


def main():
    os.makedirs(IMG_DIR, exist_ok=True)

    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS municipality_photos (
            id SERIAL PRIMARY KEY,
            municipality_code TEXT NOT NULL,
            photo_index SMALLINT NOT NULL DEFAULT 1,
            slug TEXT NOT NULL,
            object_name TEXT,
            description TEXT,
            r2_key TEXT NOT NULL,
            source_url TEXT,
            wiki_filename TEXT,
            width SMALLINT,
            height SMALLINT,
            is_primary BOOLEAN DEFAULT true,
            UNIQUE (municipality_code, photo_index)
        )
    """)
    conn.commit()

    # Get all municipalities with Wikipedia titles
    cur.execute("""
        SELECT entity_code, title FROM wikipedia_raw
        WHERE entity_type = 'municipality'
        ORDER BY entity_code
    """)
    municipalities = cur.fetchall()

    # Already done
    cur.execute("SELECT municipality_code FROM municipality_photos")
    done = {r[0] for r in cur.fetchall()}

    remaining = [(code, title) for code, title in municipalities if code not in done]
    total = len(remaining)
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else total

    print(f"To process: {total} (limit: {limit}), already done: {len(done)}", flush=True)

    processed = 0
    downloaded = 0
    no_image = 0

    for code, wiki_title in remaining[:limit]:
        processed += 1

        # 1. Get main image from Wikipedia
        image_url, pageimage, w, h = get_main_image(wiki_title)
        time.sleep(PAUSE)

        if not image_url or not pageimage:
            no_image += 1
            if processed % 100 == 0:
                print(f"  {processed}/{min(total, limit)} (dl: {downloaded}, skip: {no_image})", flush=True)
            continue

        # 2. Get description from Commons
        description, object_name = get_image_metadata(pageimage)
        time.sleep(PAUSE)

        # 3. Build slug: prefer ObjectName (cleaner), fallback to description, then title
        slug_source = object_name or description or wiki_title.replace("_", " ")
        photo_slug = make_slug(slug_source)
        if not photo_slug:
            photo_slug = f"foto-{code}"

        # 4. Download and convert to WebP
        local_path = os.path.join(IMG_DIR, f"{code}-{photo_slug}.webp")
        r2_key = f"municipalities/{code}/{photo_slug}.webp"

        if download_and_convert(image_url, local_path):
            # Get actual dimensions from converted file
            try:
                from PIL import Image
                with Image.open(local_path) as img:
                    w, h = img.size
            except Exception:
                pass

            cur.execute("""
                INSERT INTO municipality_photos
                    (municipality_code, photo_index, slug, object_name, description,
                     r2_key, source_url, wiki_filename, width, height, is_primary)
                VALUES (%s, 1, %s, %s, %s, %s, %s, %s, %s, %s, true)
                ON CONFLICT (municipality_code, photo_index) DO NOTHING
            """, (code, photo_slug, object_name, description, r2_key,
                  image_url, pageimage, w, h))
            conn.commit()
            downloaded += 1
        else:
            no_image += 1

        if processed % 100 == 0:
            print(f"  {processed}/{min(total, limit)} (dl: {downloaded}, skip: {no_image})", flush=True)

    conn.close()
    print(f"\nDone! Processed: {processed}, Downloaded: {downloaded}, No image: {no_image}", flush=True)


if __name__ == "__main__":
    main()
