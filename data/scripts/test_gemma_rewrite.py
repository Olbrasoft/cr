#!/usr/bin/env python3
"""Test Gemma 3 27B via Google Gemini API for rewriting NPÚ texts."""

from dotenv import load_dotenv
load_dotenv()
import os
import time
import requests
import psycopg2

GEMINI_KEY = os.environ.get("GEMINI_API_KEY_1", "")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemma-3-27b-it:generateContent?key={GEMINI_KEY}"
STAGING_URL = "postgresql:///cr_staging"

SYSTEM_PROMPT = """Jsi odborný copywriter specializující se na české kulturní dědictví. Tvým úkolem je přepsat poskytnutý text o památce tak, aby:

1. Zachoval všechna fakta, data, rozměry a historické údaje beze změny
2. Přeformuloval věty jiným způsobem — změnil slovosled, použil synonyma, jiné větné konstrukce
3. Text zněl přirozeně česky, jako by ho napsal zkušený průvodce nebo historik
4. Výsledek nebyl považován vyhledávači za duplicitní obsah (jiná struktura vět, jiné formulace)
5. Zachoval odbornou terminologii (architektonické pojmy, historické názvy)

Napiš POUZE přepsaný text, bez komentářů nebo vysvětlení."""


def rewrite(text):
    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\n" + text}]}
        ],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 2000,
        },
    }

    start = time.time()
    try:
        resp = requests.post(GEMINI_URL, json=payload, timeout=90)
        dur = time.time() - start

        if resp.status_code != 200:
            return None, dur, f"HTTP {resp.status_code}: {resp.text[:300]}"

        data = resp.json()
        content = data["candidates"][0]["content"]["parts"][0]["text"]
        usage = data.get("usageMetadata", {})

        return {
            "text": content,
            "input_tokens": usage.get("promptTokenCount", 0),
            "output_tokens": usage.get("candidatesTokenCount", 0),
        }, dur, None
    except Exception as e:
        return None, time.time() - start, str(e)


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT catalog_id, name, annotation, description, historical_development
        FROM npu_details
        WHERE description IS NOT NULL AND LENGTH(description) > 300
          AND historical_development IS NOT NULL AND LENGTH(historical_development) > 200
        ORDER BY RANDOM()
        LIMIT 3
    """)
    samples = cur.fetchall()
    cur.close()
    conn.close()

    for catalog_id, name, annotation, description, history in samples:
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
        print(">>> ORIGINÁL")
        print("-" * 80)
        print(source)
        print()

        result, dur, err = rewrite(source)

        print(">>> GEMMA 3 27B")
        print("-" * 80)
        if err:
            print(f"CHYBA: {err} ({dur:.1f}s)")
        else:
            print(result["text"])
            print(f"\n[{dur:.1f}s, {result['input_tokens']} in / {result['output_tokens']} out, {len(source)} → {len(result['text'])} znaků]")
        print()

        time.sleep(5)


if __name__ == "__main__":
    main()
