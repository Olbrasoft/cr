#!/usr/bin/env python3
"""Import rewritten NPÚ texts from cr_staging into cr_dev landmarks table.

Matches on npu_catalog_id and updates the npu_description column.
Uses bulk UPDATE for efficiency.

Env vars:
  STAGING_DATABASE_URL  (default: postgresql:///cr_staging)
  DATABASE_URL          (default: postgresql:///cr_dev)
"""

import os
import psycopg2
from psycopg2.extras import execute_batch


def main():
    staging_url = os.environ.get("STAGING_DATABASE_URL", "postgresql:///cr_staging")
    dev_url = os.environ.get("DATABASE_URL", "postgresql:///cr_dev")
    print(f"Staging DB: {staging_url}")
    print(f"Target DB:  {dev_url}")

    staging = psycopg2.connect(staging_url)
    dev = psycopg2.connect(dev_url)

    s_cur = staging.cursor()
    d_cur = dev.cursor()

    # Get all rewritten texts from staging
    s_cur.execute("SELECT catalog_id, rewritten_text FROM npu_rewritten")
    rewritten = {row[0]: row[1] for row in s_cur.fetchall()}
    print(f"Loaded {len(rewritten)} rewritten texts from staging")

    # Get landmarks with npu_catalog_id from target
    d_cur.execute("SELECT id, npu_catalog_id FROM landmarks WHERE npu_catalog_id IS NOT NULL")
    landmarks = d_cur.fetchall()
    print(f"Found {len(landmarks)} landmarks with npu_catalog_id")

    # Build batch of (text, id) tuples
    updates = [
        (rewritten[catalog_id], landmark_id)
        for landmark_id, catalog_id in landmarks
        if catalog_id in rewritten
    ]

    execute_batch(
        d_cur,
        "UPDATE landmarks SET npu_description = %s WHERE id = %s",
        updates,
        page_size=500,
    )

    dev.commit()
    print(f"Updated {len(updates)} landmarks with rewritten descriptions")

    s_cur.close()
    d_cur.close()
    staging.close()
    dev.close()


if __name__ == "__main__":
    main()
