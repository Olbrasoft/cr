#!/usr/bin/env python3
"""Compare Zen free models side-by-side on one NPÚ text."""

import json
import time
import requests
import psycopg2

ZEN_URL = "https://opencode.ai/zen/v1/chat/completions"
ZEN_KEY = "sk-av2Vuy1VWwqs6DMjKO0ZVkskgKTF1fSh9OgqaKOCuRk5GAuuLQ5fbLnzB8FguHfu"
STAGING_URL = "postgresql:///cr_staging"

SYSTEM_PROMPT = """Jsi odborný copywriter specializující se na české kulturní dědictví. Tvým úkolem je přepsat poskytnutý text o památce tak, aby:

1. Zachoval všechna fakta, data, rozměry a historické údaje beze změny
2. Přeformuloval věty jiným způsobem — změnil slovosled, použil synonyma, jiné větné konstrukce
3. Text zněl přirozeně česky, jako by ho napsal zkušený průvodce nebo historik
4. Výsledek nebyl považován vyhledávači za duplicitní obsah (jiná struktura vět, jiné formulace)
5. Zachoval odbornou terminologii (architektonické pojmy, historické názvy)

Napiš POUZE přepsaný text, bez komentářů nebo vysvětlení."""

MODELS = [
    "big-pickle",
    "nemotron-3-super-free",
    "mimo-v2-pro-free",
]


def rewrite(model, text):
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
        "max_tokens": 2000,
        "temperature": 0.7,
    }
    if model != "big-pickle":
        payload["reasoning_effort"] = "none"

    start = time.time()
    try:
        resp = requests.post(
            ZEN_URL,
            headers={"Authorization": f"Bearer {ZEN_KEY}", "Content-Type": "application/json"},
            json=payload,
            timeout=90,
        )
        dur = time.time() - start
        if resp.status_code != 200:
            return None, dur, f"HTTP {resp.status_code}"
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        return content, dur, None
    except Exception as e:
        return None, time.time() - start, str(e)


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    # Pick a well-known landmark with good text
    cur.execute("""
        SELECT catalog_id, name, annotation, description, historical_development
        FROM npu_details
        WHERE description IS NOT NULL AND LENGTH(description) > 300
          AND historical_development IS NOT NULL AND LENGTH(historical_development) > 200
        ORDER BY RANDOM()
        LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    catalog_id, name, annotation, description, history = row

    # Build source text
    source = ""
    if annotation:
        source += annotation.strip() + "\n\n"
    if description:
        source += description.strip() + "\n\n"
    if history:
        source += history.strip()
    source = source.strip()

    print("=" * 80)
    print(f"PAMÁTKA: {name} (catalog: {catalog_id})")
    print("=" * 80)
    print()
    print(">>> ORIGINÁL (NPÚ)")
    print("-" * 80)
    print(source)
    print()

    for model in MODELS:
        print(f">>> {model.upper()}")
        print("-" * 80)
        content, dur, err = rewrite(model, source)
        if err:
            print(f"CHYBA: {err} ({dur:.1f}s)")
        else:
            print(content)
            print(f"\n[{dur:.1f}s, {len(source)} → {len(content)} znaků]")
        print()
        time.sleep(3)  # Slušná pauza mezi požadavky


if __name__ == "__main__":
    main()
