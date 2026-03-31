#!/usr/bin/env python3
"""Match landmarks to Wikidata items by USKP ID (npu_uskp_id ↔ P762 catalog_id).

Updates landmarks.wikidata_id and landmarks.wikipedia_url in cr_dev
using data from cr_staging.wikidata_landmarks.
"""

import os
import psycopg2
from psycopg2.extras import execute_batch

STAGING_URL = os.environ.get("STAGING_DATABASE_URL", "postgresql:///cr_staging")
DEV_URL = os.environ.get("DATABASE_URL", "postgresql:///cr_dev")


def main():
    print(f"Staging DB: {STAGING_URL}")
    print(f"Target DB:  {DEV_URL}")

    staging = psycopg2.connect(STAGING_URL)
    dev = psycopg2.connect(DEV_URL)

    s_cur = staging.cursor()
    d_cur = dev.cursor()

    # Get Wikidata items indexed by catalog_id
    s_cur.execute(
        "SELECT catalog_id, wikidata_id, wikipedia_url, latitude, longitude "
        "FROM wikidata_landmarks WHERE catalog_id IS NOT NULL"
    )
    wikidata = {}
    for row in s_cur.fetchall():
        wikidata[row[0]] = {
            "wikidata_id": row[1],
            "wikipedia_url": row[2],
            "latitude": row[3],
            "longitude": row[4],
        }
    print(f"Loaded {len(wikidata)} Wikidata items from staging")

    # Get landmarks with USKP ID
    d_cur.execute(
        "SELECT id, npu_uskp_id, wikidata_id, wikipedia_url "
        "FROM landmarks WHERE npu_uskp_id IS NOT NULL"
    )
    landmarks = d_cur.fetchall()
    print(f"Found {len(landmarks)} landmarks with npu_uskp_id")

    # Match
    updates = []
    matched_wiki = 0
    matched_wikidata = 0
    already_matched = 0

    for landmark_id, uskp_id, existing_wd, existing_wiki in landmarks:
        wd = wikidata.get(uskp_id)
        if not wd:
            continue

        matched_wikidata += 1
        new_wd_id = wd["wikidata_id"]
        new_wiki_url = wd["wikipedia_url"]

        if existing_wd == new_wd_id and existing_wiki == new_wiki_url:
            already_matched += 1
            continue

        if new_wiki_url:
            matched_wiki += 1

        updates.append((new_wd_id, new_wiki_url, landmark_id))

    print(f"\nMatching results:")
    print(f"  Matched to Wikidata: {matched_wikidata}")
    print(f"  With Wikipedia URL:  {matched_wiki}")
    print(f"  Already up-to-date:  {already_matched}")
    print(f"  To update:           {len(updates)}")

    if updates:
        execute_batch(
            d_cur,
            "UPDATE landmarks SET wikidata_id = %s, wikipedia_url = %s WHERE id = %s",
            updates,
            page_size=500,
        )
        dev.commit()
        print(f"\nUpdated {len(updates)} landmarks")
    else:
        print("\nNothing to update")

    # Report unmatched
    unmatched = len(landmarks) - matched_wikidata
    print(f"\nUnmatched landmarks: {unmatched}")

    s_cur.close()
    d_cur.close()
    staging.close()
    dev.close()


if __name__ == "__main__":
    main()
