#!/usr/bin/env python3
"""Scan local photos, extract dimensions, populate photo_metadata table.

Handles:
- Landmarks: {catalog_id}.webp or {catalog_id}_{index}.webp
- Pools: {slug}-{n}.webp

Updates photo_count on landmarks table.
Safe to re-run (ON CONFLICT DO NOTHING).
"""

import os
import sys
import psycopg2
from PIL import Image

DB_URL = "postgresql:///cr_dev"
LANDMARKS_DIR = "/home/jirka/Olbrasoft/cr/data/images/landmarks"
POOLS_DIR = "/home/jirka/Olbrasoft/cr/data/images/pools"


def scan_landmarks(conn):
    cur = conn.cursor()

    # Build catalog_id -> landmark_id map
    cur.execute("SELECT id, npu_catalog_id FROM landmarks WHERE npu_catalog_id IS NOT NULL")
    catalog_map = {row[1]: row[0] for row in cur.fetchall()}
    print(f"Landmarks with catalog_id: {len(catalog_map)}")

    files = sorted(f for f in os.listdir(LANDMARKS_DIR) if f.endswith(".webp"))
    print(f"Landmark photo files: {len(files)}")

    inserted = 0
    skipped = 0
    batch = []

    for i, filename in enumerate(files):
        filepath = os.path.join(LANDMARKS_DIR, filename)
        base = filename.rsplit(".", 1)[0]  # strip .webp

        # Parse: {catalog_id}.webp or {catalog_id}_{index}.webp
        if "_" in base:
            parts = base.rsplit("_", 1)
            catalog_id = parts[0]
            photo_index = int(parts[1])
        else:
            catalog_id = base
            photo_index = 1

        landmark_id = catalog_map.get(catalog_id)
        if not landmark_id:
            skipped += 1
            continue

        try:
            with Image.open(filepath) as img:
                width, height = img.size
        except Exception as e:
            print(f"  ERROR reading {filename}: {e}")
            skipped += 1
            continue

        file_size = os.path.getsize(filepath)
        r2_key = f"landmarks/{filename}"

        batch.append((landmark_id, photo_index, r2_key, width, height, file_size))
        inserted += 1

        if len(batch) >= 500:
            _insert_batch(cur, batch)
            conn.commit()
            batch = []
            print(f"  Landmarks: {inserted}/{len(files)} inserted...", flush=True)

    if batch:
        _insert_batch(cur, batch)
        conn.commit()

    # Update photo_count on landmarks
    cur.execute("""
        UPDATE landmarks l SET photo_count = COALESCE((
            SELECT COUNT(*) FROM photo_metadata pm
            WHERE pm.entity_type = 'landmark' AND pm.entity_id = l.id
        ), 0)
    """)
    conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM photo_metadata WHERE entity_type = 'landmark'")
    count = cur.fetchone()[0]
    print(f"Landmarks done: {inserted} inserted, {skipped} skipped, {count} total in DB")


def scan_pools(conn):
    cur = conn.cursor()

    cur.execute("SELECT id, slug FROM pools")
    slug_map = {row[1]: row[0] for row in cur.fetchall()}
    print(f"Pools in DB: {len(slug_map)}")

    files = sorted(f for f in os.listdir(POOLS_DIR) if f.endswith(".webp"))
    print(f"Pool photo files: {len(files)}")

    inserted = 0
    skipped = 0
    batch = []

    for filename in files:
        filepath = os.path.join(POOLS_DIR, filename)
        base = filename.rsplit(".", 1)[0]  # strip .webp

        # Parse: {slug}-{n}.webp — last segment after hyphen is the index
        parts = base.rsplit("-", 1)
        if len(parts) != 2 or not parts[1].isdigit():
            skipped += 1
            continue

        slug = parts[0]
        photo_index = int(parts[1])

        pool_id = slug_map.get(slug)
        if not pool_id:
            skipped += 1
            continue

        try:
            with Image.open(filepath) as img:
                width, height = img.size
        except Exception as e:
            print(f"  ERROR reading {filename}: {e}")
            skipped += 1
            continue

        file_size = os.path.getsize(filepath)
        r2_key = f"pools/{filename}"

        batch.append((pool_id, photo_index, r2_key, width, height, file_size))
        inserted += 1

        if len(batch) >= 500:
            _insert_batch_pool(cur, batch)
            conn.commit()
            batch = []

    if batch:
        _insert_batch_pool(cur, batch)
        conn.commit()

    count_row = cur.execute("SELECT COUNT(*) FROM photo_metadata WHERE entity_type = 'pool'")
    count = cur.fetchone()[0]
    print(f"Pools done: {inserted} inserted, {skipped} skipped, {count} total in DB")


def _insert_batch(cur, batch):
    for landmark_id, photo_index, r2_key, width, height, file_size in batch:
        cur.execute("""
            INSERT INTO photo_metadata (entity_type, entity_id, photo_index, r2_key, width, height, file_size)
            VALUES ('landmark', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity_type, entity_id, photo_index) DO NOTHING
        """, (landmark_id, photo_index, r2_key, width, height, file_size))


def _insert_batch_pool(cur, batch):
    for pool_id, photo_index, r2_key, width, height, file_size in batch:
        cur.execute("""
            INSERT INTO photo_metadata (entity_type, entity_id, photo_index, r2_key, width, height, file_size)
            VALUES ('pool', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity_type, entity_id, photo_index) DO NOTHING
        """, (pool_id, photo_index, r2_key, width, height, file_size))


if __name__ == "__main__":
    conn = psycopg2.connect(DB_URL)
    print("Scanning landmark photos...")
    scan_landmarks(conn)
    print()
    print("Scanning pool photos...")
    scan_pools(conn)
    conn.close()
    print("\nDone!")
