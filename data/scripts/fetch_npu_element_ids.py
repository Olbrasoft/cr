#!/usr/bin/env python3
"""Fetch element IDs and image URLs from NPÚ search API.

Uses region + county filtering to get all monuments (API limits to 250 per query).
Stores results in cr_staging.npu_monuments.
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


def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    # Get all regions
    regions = fetch_json(f"{BASE}/search-form/regions")
    print(f"Regions: {len(regions)}")

    total_fetched = 0
    total_matched = 0

    for region in regions:
        rid = region["id"]
        rname = region["name"]

        # Get counties for this region
        counties = fetch_json(f"{BASE}/search-form/counties?regions={rid}")
        time.sleep(0.5)

        for county in counties:
            cid = county["id"]
            cname = county["name"]

            page = 1
            county_total = 0

            while True:
                url = (
                    f"{BASE}/search/fulltext?type=uskp&mode=fulltext"
                    f"&region={rid}&county={cid}"
                    f"&isProtectedNow=1&mainObject=1"
                    f"&sort=podle-relevance&limit=250&page={page}"
                )

                try:
                    data = fetch_json(url)
                except Exception as e:
                    print(f"  ERROR {rname}/{cname} p{page}: {e}")
                    time.sleep(3)
                    break

                results = data.get("results", [])

                for item in results:
                    eid = item.get("id")
                    cat = item.get("catalogNumber", "")
                    img = item.get("imageUrl", "")
                    mid = item.get("mediumId", "")

                    if cat:
                        cur.execute(
                            "UPDATE npu_monuments SET element_id=%s, image_url=%s, medium_id=%s WHERE catalog_id=%s",
                            (int(eid) if eid else None, img or None, mid or None, cat),
                        )
                        if cur.rowcount > 0:
                            total_matched += 1

                conn.commit()
                county_total += len(results)
                total_fetched += len(results)

                if len(results) < 250:
                    break
                page += 1
                time.sleep(0.5)

            if county_total > 0:
                time.sleep(0.5)

        print(f"  {rname}: fetched={total_fetched}, matched={total_matched}")

    print(f"\nDone! Total fetched: {total_fetched}, matched: {total_matched}")

    cur.execute("SELECT count(*), count(element_id), count(image_url) FROM npu_monuments")
    row = cur.fetchone()
    print(f"NPÚ staging: {row[0]} total, {row[1]} with element_id, {row[2]} with image_url")

    conn.close()


if __name__ == "__main__":
    main()
