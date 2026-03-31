#!/usr/bin/env python3
"""Fetch hero photos for each region from Wikipedia.

For each region:
1. Get the main image from the region's Wikipedia page
2. Download the full-resolution image
3. Convert to WebP
4. Upload to R2 as a landmark photo
5. Store in landmark_photos table
6. Update region with hero_landmark_id reference
"""

import os
import re
import subprocess
import tempfile
import requests
import psycopg2
from urllib.parse import unquote

DEV_URL = os.environ.get("DATABASE_URL", "postgresql:///cr_dev")
WIKI_API = "https://cs.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "CeskaRepublikaWiki/1.0 (info@ceskarepublika.wiki)"}

# Region slug → (Wikipedia page, landmark search hint)
# Manually curated: which landmark does each region's Wikipedia image show
REGION_LANDMARKS = {
    "stredocesky-kraj": {"wiki": "Středočeský_kraj", "landmark_hint": "hrad Karlštejn", "landmark_slug": "hrad-karlstejn"},
    "jihocesky-kraj": {"wiki": "Jihočeský_kraj", "landmark_hint": "zámek Jindřichův Hradec", "landmark_slug": "zamek"},
    "karlovarsky-kraj": {"wiki": "Karlovarský_kraj", "landmark_hint": "Mlýnská kolonáda", "landmark_slug": "mlynska-kolonada"},
    "kraj-vysocina": {"wiki": "Kraj_Vysočina", "landmark_hint": "zámek Třebíč", "landmark_slug": None},
    "kralovehradecky-kraj": {"wiki": "Královéhradecký_kraj", "landmark_hint": "Hradec Králové", "landmark_slug": None},
    "liberecky-kraj": {"wiki": "Liberecký_kraj", "landmark_hint": "Ještěd", "landmark_slug": "jested"},
    "moravskoslezsky-kraj": {"wiki": "Moravskoslezský_kraj", "landmark_hint": "Krajský úřad Ostrava", "landmark_slug": None},
    "olomoucky-kraj": {"wiki": "Olomoucký_kraj", "landmark_hint": "hrad Šternberk", "landmark_slug": "hrad-sternberk"},
    "pardubicky-kraj": {"wiki": "Pardubický_kraj", "landmark_hint": "zámek Litomyšl", "landmark_slug": "zamek-litomysl"},
    "plzensky-kraj": {"wiki": "Plzeňský_kraj", "landmark_hint": "rotunda sv. Petra", "landmark_slug": None},
    "ustecky-kraj": {"wiki": "Ústecký_kraj", "landmark_hint": "Litoměřice náměstí", "landmark_slug": None},
    "zlinsky-kraj": {"wiki": "Zlínský_kraj", "landmark_hint": "zámek Kroměříž", "landmark_slug": "arcibiskupsky-zamek-kromeriz"},
    "hlavni-mesto-praha": {"wiki": "Praha", "landmark_hint": "Praha collage", "landmark_slug": None},
    "jihomoravsky-kraj": {"wiki": "Jihomoravský_kraj", "landmark_hint": "JMK collage", "landmark_slug": None},
}


def get_wiki_page_image(page_title):
    """Get the main image from a Wikipedia page."""
    params = {
        "action": "query",
        "titles": page_title,
        "prop": "pageimages",
        "piprop": "original",
        "format": "json",
    }
    resp = requests.get(WIKI_API, params=params, headers=HEADERS, timeout=30)
    data = resp.json()
    pages = data.get("query", {}).get("pages", {})
    for page in pages.values():
        original = page.get("original", {})
        if original:
            return original.get("source")
    return None


def get_image_description(filename):
    """Get image description from Wikimedia Commons."""
    params = {
        "action": "query",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "extmetadata",
        "format": "json",
    }
    resp = requests.get(
        "https://commons.wikimedia.org/w/api.php",
        params=params,
        headers=HEADERS,
        timeout=30,
    )
    data = resp.json()
    pages = data.get("query", {}).get("pages", {})
    for page in pages.values():
        info = page.get("imageinfo", [{}])[0]
        meta = info.get("extmetadata", {})
        desc = meta.get("ImageDescription", {}).get("value", "")
        desc = re.sub(r"<[^>]+>", "", desc).strip()
        return desc[:200] if desc else ""
    return ""


def slugify(text):
    """Convert text to URL-safe slug."""
    slug = text.lower()
    slug = re.sub(r"[^a-z0-9\-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def download_and_convert(url, output_path):
    """Download image and convert to WebP."""
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
        tmp.write(resp.content)
        tmp_path = tmp.name

    # Convert to WebP using cwebp or Python
    try:
        from PIL import Image
        img = Image.open(tmp_path)
        # Resize if too large (max 1920px wide)
        if img.width > 1920:
            ratio = 1920 / img.width
            img = img.resize((1920, int(img.height * ratio)), Image.LANCZOS)
        img.save(output_path, "WEBP", quality=82)
        width, height = img.size
        os.unlink(tmp_path)
        return width, height
    except Exception as e:
        os.unlink(tmp_path)
        raise e


def upload_to_r2(local_path, r2_key):
    """Upload file to R2 via rclone or AWS CLI."""
    # Use rclone if available, otherwise try aws s3
    try:
        subprocess.run(
            ["rclone", "copyto", local_path, f"r2:ceskarepublika/{r2_key}"],
            check=True,
            capture_output=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    # Try aws s3 cp
    endpoint = os.environ.get("R2_ENDPOINT", "")
    if endpoint:
        try:
            subprocess.run(
                [
                    "aws", "s3", "cp", local_path,
                    f"s3://ceskarepublika/{r2_key}",
                    "--endpoint-url", endpoint,
                ],
                check=True,
                capture_output=True,
            )
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass

    print(f"  WARNING: Could not upload to R2. File saved locally: {local_path}")
    return False


def find_landmark(cur, region_slug, landmark_slug):
    """Find landmark in database by slug, searching in the region's ORP areas."""
    if not landmark_slug:
        return None

    cur.execute(
        """SELECT l.id, l.npu_catalog_id, l.name, l.slug, o.slug as orp_slug
           FROM landmarks l
           JOIN municipalities m ON l.municipality_id = m.id
           JOIN orp o ON m.orp_id = o.id
           JOIN districts d ON o.district_id = d.id
           JOIN regions r ON d.region_id = r.id
           WHERE r.slug = %s AND l.slug = %s
           LIMIT 1""",
        (region_slug, landmark_slug),
    )
    row = cur.fetchone()
    if row:
        return {
            "id": row[0],
            "npu_catalog_id": row[1],
            "name": row[2],
            "slug": row[3],
            "orp_slug": row[4],
        }
    return None


def main():
    conn = psycopg2.connect(DEV_URL)
    cur = conn.cursor()

    output_dir = "/tmp/region-hero-photos"
    os.makedirs(output_dir, exist_ok=True)

    print("Fetching region hero photos from Wikipedia...\n")

    for region_slug, config in REGION_LANDMARKS.items():
        wiki_page = config["wiki"]
        landmark_slug = config.get("landmark_slug")

        print(f"=== {region_slug} ===")

        # Skip collages / non-landmark images
        if landmark_slug is None:
            print(f"  Skipping — no landmark mapping (collage or non-landmark image)")
            continue

        # Find landmark in DB
        landmark = find_landmark(cur, region_slug, landmark_slug)
        if not landmark:
            print(f"  Landmark '{landmark_slug}' not found in region {region_slug}")
            continue

        print(f"  Landmark: {landmark['name']} (NPÚ: {landmark['npu_catalog_id']})")

        # Get Wikipedia image URL
        image_url = get_wiki_page_image(wiki_page)
        if not image_url:
            print(f"  No Wikipedia image found")
            continue

        filename = unquote(image_url.split("/")[-1])
        slug = slugify(re.sub(r"\.[^.]+$", "", filename))
        description = get_image_description(filename)

        print(f"  Image: {filename}")
        print(f"  Slug: {slug}")

        # Check if already exists
        cur.execute(
            "SELECT id FROM landmark_photos WHERE npu_catalog_id = %s AND photo_index = 2",
            (landmark["npu_catalog_id"],),
        )
        if cur.fetchone():
            print(f"  Already exists — skipping")

            # Still update region hero reference
            cur.execute(
                "UPDATE regions SET hero_landmark_id = %s, hero_photo_index = 2 WHERE slug = %s",
                (landmark["id"], region_slug),
            )
            conn.commit()
            continue

        # Download and convert to WebP
        r2_key = f"landmarks/{landmark['npu_catalog_id']}-2.webp"
        local_path = os.path.join(output_dir, f"{landmark['npu_catalog_id']}-2.webp")

        try:
            width, height = download_and_convert(image_url, local_path)
            print(f"  Downloaded: {width}x{height}")
        except Exception as e:
            print(f"  Download failed: {e}")
            continue

        # Upload to R2
        uploaded = upload_to_r2(local_path, r2_key)

        # Insert into landmark_photos
        cur.execute(
            """INSERT INTO landmark_photos
               (npu_catalog_id, photo_index, slug, r2_key, description, source_url, width, height)
               VALUES (%s, 2, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (npu_catalog_id, photo_index) DO NOTHING""",
            (
                landmark["npu_catalog_id"],
                slug,
                r2_key,
                description,
                image_url,
                width,
                height,
            ),
        )

        # Update landmarks photo_count
        cur.execute(
            "UPDATE landmarks SET photo_count = photo_count + 1 WHERE id = %s",
            (landmark["id"],),
        )

        # Update region hero reference
        cur.execute(
            "UPDATE regions SET hero_landmark_id = %s, hero_photo_index = 2 WHERE slug = %s",
            (landmark["id"], region_slug),
        )

        conn.commit()
        print(f"  ✓ Stored: {r2_key}")
        if uploaded:
            print(f"  ✓ Uploaded to R2")

    # Summary
    cur.execute("SELECT COUNT(*) FROM landmark_photos")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM regions WHERE hero_landmark_id IS NOT NULL")
    regions_with_hero = cur.fetchone()[0]
    print(f"\nDone! {total} landmark photos, {regions_with_hero} regions with hero photo")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
