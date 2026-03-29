#!/usr/bin/env python3
"""Test Zen API free models for rewriting NPÚ landmark texts.

Tests Big Pickle, MiMo V2 Pro, Nemotron 3 Super, MiniMax M2.5 on sample texts.
Measures quality of Czech text rewriting for SEO uniqueness.
"""

from dotenv import load_dotenv
load_dotenv()
import os
import json
import time
import requests
import psycopg2

ZEN_URL = "https://opencode.ai/zen/v1/chat/completions"
ZEN_KEY = os.environ.get("ZEN_API_KEY", "")

STAGING_URL = "postgresql:///cr_staging"

SYSTEM_PROMPT = """Jsi odborný copywriter specializující se na české kulturní dědictví. Tvým úkolem je přepsat poskytnutý text o památce tak, aby:

1. Zachoval všechna fakta, data, rozměry a historické údaje beze změny
2. Přeformuloval věty jiným způsobem — změnil slovosled, použil synonyma, jiné větné konstrukce
3. Text zněl přirozeně česky, jako by ho napsal zkušený průvodce nebo historik
4. Výsledek nebyl považován vyhledávači za duplicitní obsah (jiná struktura vět, jiné formulace)
5. Zachoval odbornou terminologii (architektonické pojmy, historické názvy)

Napiš POUZE přepsaný text, bez komentářů nebo vysvětlení."""

FREE_MODELS = [
    "big-pickle",
    "mimo-v2-pro-free",
    "nemotron-3-super-free",
    "minimax-m2.5-free",
]


def test_model(model, text, name):
    """Test a single model with a text."""
    headers = {
        "Authorization": f"Bearer {ZEN_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
        "max_tokens": 2000,
        "temperature": 0.7,
    }

    # Add reasoning_effort=none for models that support it
    if model not in ("big-pickle",):
        payload["reasoning_effort"] = "none"

    start = time.time()
    try:
        resp = requests.post(ZEN_URL, headers=headers, json=payload, timeout=60)
        duration = time.time() - start

        if resp.status_code != 200:
            return {
                "model": model,
                "status": "error",
                "error": f"HTTP {resp.status_code}: {resp.text[:200]}",
                "duration": duration,
            }

        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {})

        return {
            "model": model,
            "status": "ok",
            "name": name,
            "duration": duration,
            "input_tokens": usage.get("prompt_tokens", 0),
            "output_tokens": usage.get("completion_tokens", 0),
            "output": content,
        }
    except Exception as e:
        return {
            "model": model,
            "status": "error",
            "error": str(e),
            "duration": time.time() - start,
        }


def main():
    # Get 3 sample texts from staging DB
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT catalog_id, name,
            COALESCE(annotation, '') || '\n\n' || COALESCE(description, '') || '\n\n' || COALESCE(historical_development, '')
        FROM npu_details
        WHERE description IS NOT NULL AND LENGTH(description) > 200
        ORDER BY RANDOM()
        LIMIT 3
    """)
    samples = cur.fetchall()
    cur.close()
    conn.close()

    print(f"Testing {len(FREE_MODELS)} models on {len(samples)} texts")
    print("=" * 80)

    for model in FREE_MODELS:
        print(f"\n{'=' * 80}")
        print(f"MODEL: {model}")
        print(f"{'=' * 80}")

        for catalog_id, name, text in samples:
            text = text.strip()
            if len(text) < 50:
                continue

            result = test_model(model, text, name)

            if result["status"] == "ok":
                print(f"\n--- {name} ({catalog_id}) ---")
                print(f"  Duration: {result['duration']:.1f}s")
                print(f"  Tokens: {result['input_tokens']} in / {result['output_tokens']} out")
                print(f"  Input length: {len(text)} chars")
                print(f"  Output length: {len(result['output'])} chars")
                print(f"  Output preview: {result['output'][:300]}...")
            else:
                print(f"\n--- {name} ({catalog_id}) ---")
                print(f"  ERROR: {result['error']}")
                print(f"  Duration: {result['duration']:.1f}s")

            time.sleep(1)

        time.sleep(2)


if __name__ == "__main__":
    main()
