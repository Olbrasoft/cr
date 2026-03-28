#!/usr/bin/env python3
"""Fetch NPÚ element IDs and image URLs by querying per municipality.

NPÚ search API limits to 250 results. By querying per municipality,
we stay well under this limit and get complete data.
"""

import json
import os
import sys
import time
import urllib.request
import psycopg2

STAGING_URL = os.environ.get("STAGING_DATABASE_URL", "postgres://cr_dev_user:cr_dev_2026@localhost/cr_staging")
UA = "CeskaRepublikaWiki/1.0 (info@ceskarepublika.wiki)"
BASE = "https://pamatkovykatalog.cz/api"


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    # Get all regions
    regions = fetch(f"{BASE}/search-form/regions")
    print(f"Regions: {len(regions)}", flush=True)

    total_matched = 0
    total_fetched = 0

    for region in regions:
        rid = region["id"]
        rname = region["name"]

        # Get counties
        try:
            counties = fetch(f"{BASE}/search-form/counties?regions={rid}")
        except:
            print(f"  {rname}: failed to get counties", flush=True)
            continue
        time.sleep(0.3)

        for county in counties:
            cid = county["id"]

            # Get municipalities
            try:
                munis = fetch(f"{BASE}/search-form/municipalities?counties={cid}")
            except:
                continue
            time.sleep(0.3)

            for muni in munis:
                mid = muni["id"]

                url = (
                    f"{BASE}/search/fulltext?type=uskp&mode=fulltext"
                    f"&region={rid}&county={cid}&municipality={mid}"
                    f"&isProtectedNow=1&mainObject=1"
                    f"&sort=podle-relevance&limit=250"
                )

                try:
                    data = fetch(url)
                except:
                    time.sleep(1)
                    continue

                for item in data.get("results", []):
                    eid = item.get("id")
                    cat = item.get("catalogNumber", "")
                    img = item.get("imageUrl", "")
                    mid_photo = item.get("mediumId", "")

                    if cat:
                        cur.execute(
                            "UPDATE npu_monuments SET element_id=%s, image_url=%s, medium_id=%s WHERE catalog_id=%s AND element_id IS NULL",
                            (int(eid) if eid else None, img or None, mid_photo or None, cat),
                        )
                        if cur.rowcount > 0:
                            total_matched += 1

                total_fetched += len(data.get("results", []))
                conn.commit()
                time.sleep(0.3)

        cur.execute("SELECT count(element_id) FROM npu_monuments WHERE element_id IS NOT NULL")
        current = cur.fetchone()[0]
        print(f"  {rname}: total_fetched={total_fetched}, matched={total_matched}, in_db={current}", flush=True)

    cur.execute("SELECT count(*), count(element_id), count(image_url) FROM npu_monuments")
    row = cur.fetchone()
    print(f"\nDone! {row[0]} total, {row[1]} with element_id, {row[2]} with image_url", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
