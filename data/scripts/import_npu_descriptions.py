#!/usr/bin/env python3
"""Import rewritten NPÚ texts from cr_staging into cr_dev landmarks table.

Matches on npu_catalog_id and updates the npu_description column.
"""

import psycopg2

STAGING_URL = "postgresql:///cr_staging"
DEV_URL = "postgresql:///cr_dev"


def main():
    staging = psycopg2.connect(STAGING_URL)
    dev = psycopg2.connect(DEV_URL)

    s_cur = staging.cursor()
    d_cur = dev.cursor()

    # Get all rewritten texts from staging
    s_cur.execute("SELECT catalog_id, rewritten_text FROM npu_rewritten")
    rewritten = {row[0]: row[1] for row in s_cur.fetchall()}
    print(f"Loaded {len(rewritten)} rewritten texts from cr_staging")

    # Get landmarks with npu_catalog_id from dev
    d_cur.execute("SELECT id, npu_catalog_id FROM landmarks WHERE npu_catalog_id IS NOT NULL")
    landmarks = d_cur.fetchall()
    print(f"Found {len(landmarks)} landmarks with npu_catalog_id in cr_dev")

    updated = 0
    for landmark_id, catalog_id in landmarks:
        text = rewritten.get(catalog_id)
        if text:
            d_cur.execute(
                "UPDATE landmarks SET npu_description = %s WHERE id = %s",
                (text, landmark_id),
            )
            updated += 1

    dev.commit()
    print(f"Updated {updated} landmarks with rewritten descriptions")

    s_cur.close()
    d_cur.close()
    staging.close()
    dev.close()


if __name__ == "__main__":
    main()
