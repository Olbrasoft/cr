#!/usr/bin/env python3
"""Download detailed texts from NPÚ Památkový katalog API into cr_staging.npu_details.

For each monument with element_id, fetches:
- annotation (short summary)
- description (physical description)
- propertyHistoricalDevelopment (history)
- heritageValueEvaluation (heritage value)
- styles, elementTypes, objectName, protectedSince
- media count and image info

Skips already-fetched records. Safe to restart.
"""

import json
import time
import requests
import psycopg2
import psycopg2.extras

STAGING_URL = "postgresql:///cr_staging"
API_BASE = "https://pamatkovykatalog.cz/api/element"
UA = "Mozilla/5.0 (X11; Linux x86_64) CeskaRepublikaWiki/1.0"
HEADERS = {"User-Agent": UA, "Accept": "application/json"}


def fetch_element(element_id):
    """Fetch element details from NPÚ API."""
    url = f"{API_BASE}/{element_id}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        return None
    except (requests.RequestException, json.JSONDecodeError):
        return None


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    # Get all monuments with element_id that we haven't fetched yet
    cur.execute("""
        SELECT m.element_id, m.catalog_id, m.name
        FROM npu_monuments m
        WHERE m.element_id IS NOT NULL
          AND m.catalog_id NOT IN (SELECT catalog_id FROM npu_details)
        ORDER BY m.element_id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Elements to fetch: {total}", flush=True)

    downloaded = 0
    failed = 0

    for i, (element_id, catalog_id, name) in enumerate(rows):
        data = fetch_element(element_id)

        if data is None:
            failed += 1
            if (i + 1) % 200 == 0:
                print(
                    f"  Progress: {i+1}/{total} "
                    f"(downloaded: {downloaded}, failed: {failed})",
                    flush=True,
                )
            time.sleep(0.3)
            continue

        # Extract text fields
        annotation = (data.get("annotation") or "").strip() or None
        description = (data.get("description") or "").strip() or None
        hist_dev = (data.get("propertyHistoricalDevelopment") or "").strip() or None
        heritage = (data.get("heritageValueEvaluation") or "").strip() or None
        obj_name = (data.get("objectName") or "").strip() or None
        elem_types = (data.get("elementTypes") or "").strip() or None
        styles = (data.get("styles") or "").strip() or None
        protected = (data.get("protectedSince") or "").strip() or None
        image_url = (data.get("imageUrl") or "").strip() or None
        permanent_url = (data.get("permanentUrl") or "").strip() or None
        api_name = (data.get("name") or "").strip() or None

        # Extract image ID from imageUrl
        image_id = None
        if image_url and "id=" in image_url:
            image_id = image_url.split("id=")[-1]

        # Count media
        media_count = len(data.get("misMedia") or [])

        cur.execute("""
            INSERT INTO npu_details (
                catalog_id, npu_element_id, name, annotation, description,
                historical_development, heritage_value, object_name,
                element_types, styles, protected_since,
                image_id, image_url, permanent_url, media_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (catalog_id) DO NOTHING
        """, (
            catalog_id, element_id, api_name or name,
            annotation, description, hist_dev, heritage,
            obj_name, elem_types, styles, protected,
            image_id, image_url, permanent_url, media_count,
        ))

        downloaded += 1
        if downloaded % 100 == 0:
            conn.commit()

        if (i + 1) % 200 == 0:
            print(
                f"  Progress: {i+1}/{total} "
                f"(downloaded: {downloaded}, failed: {failed})",
                flush=True,
            )

        time.sleep(0.3)

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"\nDone! Downloaded: {downloaded}, Failed: {failed}, Total: {total}",
        flush=True,
    )


if __name__ == "__main__":
    main()
