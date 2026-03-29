#!/usr/bin/env python3
"""Generate pool descriptions using Gemma 3 27B via Google Gemini API."""

from dotenv import load_dotenv
load_dotenv()
import os
import time
import requests
import psycopg2

STAGING_URL = "postgresql:///cr_staging"
GEMINI_KEYS = [
    os.environ.get("GEMINI_API_KEY_1", ""),
    os.environ.get("GEMINI_API_KEY_2", ""),
    os.environ.get("GEMINI_API_KEY_3", ""),
]
GEMINI_URL_TPL = "https://generativelanguage.googleapis.com/v1beta/models/gemma-3-27b-it:generateContent?key={}"


def generate_description(name, address, facilities, pool_length, types, raw_text, key_idx):
    type_labels = []
    if "aquapark" in types: type_labels.append("aquapark")
    if "indoor" in types: type_labels.append("krytý bazén")
    if "outdoor" in types: type_labels.append("venkovní koupaliště")
    if "natural" in types: type_labels.append("přírodní koupaliště")

    if raw_text and len(raw_text) > 100:
        prompt = f"""Napiš krátký popis (3-5 vět, česky) pro {name} ({', '.join(type_labels)}).

Adresa: {address or 'neuvedena'}
Délka bazénu: {pool_length or 'neuvedena'} m
Vybavení: {facilities or 'neuvedeno'}

Zde je text z oficiálních stránek tohoto zařízení:
{raw_text[:2000]}

Použij konkrétní informace z textu výše — rozměry bazénů, atrakce, služby, historii. Napiš to jako informativní článek pro turistického průvodce. Napiš POUZE popis, bez nadpisu."""
    else:
        prompt = f"""Napiš krátký popis (3-5 vět, česky) pro {name} ({', '.join(type_labels)}).

Adresa: {address or 'neuvedena'}
Délka bazénu: {pool_length or 'neuvedena'} m
Vybavení: {facilities or 'neuvedeno'}

Napiš to jako informativní článek pro turistického průvodce. Zmiň typ zařízení, vybavení a pro koho je vhodné. Napiš POUZE popis, bez nadpisu."""

GEMINI_KEYS = [
    os.environ.get("GEMINI_API_KEY_1", ""),
    os.environ.get("GEMINI_API_KEY_2", ""),
    os.environ.get("GEMINI_API_KEY_3", ""),
]
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 500},
    }

    try:
        resp = requests.post(url, json=payload, timeout=60)
        if resp.status_code == 429:
            time.sleep(30)
            return None
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        return None


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT p.slug, p.name, p.address, p.facilities, p.pool_length_m,
               p.is_aquapark, p.is_indoor, p.is_outdoor, p.is_natural,
               pt.raw_text
        FROM pools p
        LEFT JOIN pool_texts pt ON p.slug = pt.slug
        WHERE p.description IS NULL
        ORDER BY p.slug
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Generating descriptions for {total} pools...", flush=True)

    done = 0
    failed = 0

    for i, row in enumerate(rows):
        slug, name, address, facilities, pool_length = row[0], row[1], row[2], row[3], row[4]
        is_aquapark, is_indoor, is_outdoor, is_natural = row[5], row[6], row[7], row[8]
        raw_text = row[9]

        types = []
        if is_aquapark: types.append("aquapark")
        if is_indoor: types.append("indoor")
        if is_outdoor: types.append("outdoor")
        if is_natural: types.append("natural")

        desc = generate_description(name, address, facilities, pool_length, types, raw_text, i)

        if desc:
            cur.execute("UPDATE pools SET description = %s WHERE slug = %s", (desc, slug))
            done += 1
        else:
            failed += 1

        if (i + 1) % 10 == 0:
            conn.commit()
            print(f"  Progress: {i+1}/{total} (done: {done}, failed: {failed})", flush=True)

        time.sleep(3)

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone! Generated: {done}, Failed: {failed}", flush=True)


if __name__ == "__main__":
    main()
