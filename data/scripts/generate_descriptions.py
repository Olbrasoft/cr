#!/usr/bin/env python3
"""Generate original descriptions using Mercury 2 LLM from Wikipedia raw texts.

Uses 4 API keys in rotation to stay within rate limits.
Processes regions first, then municipalities.
Supports resume — skips entries that already have a description.
"""

import json
import os
import sys
import time
import urllib.request
import psycopg2

API_URL = "https://api.inceptionlabs.ai/v1/chat/completions"
API_KEYS = [
    "sk_0ea844c778068375477796de08beb55f",
    "sk_d063b01e2ec06e9b3e3baf5574750de1",
    "sk_0ef23c67ad2cea6de1ecc5c473f10719",
    "sk_b2384d3d73c736d5597841898b580c7d",
]

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://jirka@localhost/cr_dev")

REGION_PROMPT = """Jsi editor encyklopedického portálu o České republice. Napiš originální popis regionu "{title}" na základě následujících informací.

Pravidla:
- Piš v češtině, encyklopedickým stylem
- Text musí být ORIGINÁLNÍ - ne kopie vstupního textu, ale vlastní formulace se stejnými fakty
- Zahrň: polohu, rozlohu, počet obyvatel, hlavní město kraje, zajímavosti, charakteristické rysy
- NEzahrnuj: odkazy, reference, citace, čísla poznámek
- NEzahrnuj: seznamy obcí nebo okresů
- Délka: 3-5 odstavců (cca 500-800 slov)
- Formát: prostý text bez HTML, bez nadpisů, bez odrážek

Zdrojové informace:
{text}"""

MUNICIPALITY_PROMPT = """Jsi editor encyklopedického portálu o České republice. Napiš originální popis obce "{title}" na základě následujících informací.

Pravidla:
- Piš v češtině, encyklopedickým stylem
- Text musí být ORIGINÁLNÍ - ne kopie vstupního textu, ale vlastní formulace se stejnými fakty
- Zahrň: polohu, historii, zajímavosti, počet obyvatel, pamětihodnosti (pokud jsou zmíněny)
- NEzahrnuj: odkazy, reference, citace, čísla poznámek, seznamy
- Délka: 1-3 odstavce (cca 150-400 slov)
- Formát: prostý text bez HTML, bez nadpisů, bez odrážek

Zdrojové informace:
{text}"""


def call_mercury(prompt: str, key_index: int) -> tuple[str | None, dict]:
    """Call Mercury 2 API. Returns (content, usage)."""
    data = json.dumps({
        "model": "mercury-2",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 2000,
        "temperature": 0.7,
    }).encode()

    req = urllib.request.Request(API_URL, data=data, headers={
        "Authorization": f"Bearer {API_KEYS[key_index % len(API_KEYS)]}",
        "Content-Type": "application/json",
    })

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())
            content = result["choices"][0]["message"]["content"]
            usage = result.get("usage", {})
            return content, usage
    except Exception as e:
        print(f"  ERROR: {e}", file=sys.stderr)
        return None, {}


def process_regions(conn):
    """Generate descriptions for all 14 regions."""
    cur = conn.cursor()

    # Get regions that don't have a description yet
    cur.execute("""
        SELECT w.municipality_code, w.title, w.extract
        FROM wikipedia_raw w
        WHERE w.municipality_code LIKE 'region_%'
        ORDER BY w.municipality_code
    """)
    rows = cur.fetchall()
    print(f"Regions to process: {len(rows)}")

    total_tokens = 0
    for i, (code, title, raw_text) in enumerate(rows):
        region_code = code.replace("region_", "")

        # Check if region already has description
        cur.execute("SELECT description FROM regions WHERE region_code = %s", (region_code,))
        existing = cur.fetchone()
        if existing and existing[0]:
            print(f"  SKIP {title} (already has description)")
            continue

        # Truncate text to ~3000 chars to save tokens
        text = raw_text[:4000]
        prompt = REGION_PROMPT.format(title=title, text=text)

        content, usage = call_mercury(prompt, i)
        if content:
            cur.execute(
                "UPDATE regions SET description = %s WHERE region_code = %s",
                (content, region_code)
            )
            conn.commit()
            tokens = usage.get("total_tokens", 0)
            total_tokens += tokens
            print(f"  OK {title}: {len(content)} chars, {tokens} tokens")
        else:
            print(f"  FAIL {title}")

        time.sleep(2)  # Be conservative with rate limits

    print(f"Region total tokens: {total_tokens}")
    return total_tokens


def process_municipalities(conn, limit=None):
    """Generate descriptions for municipalities."""
    cur = conn.cursor()

    cur.execute("""
        SELECT w.municipality_code, w.title, w.extract
        FROM wikipedia_raw w
        WHERE w.municipality_code NOT LIKE 'region_%'
        AND w.municipality_code NOT IN (
            SELECT municipality_code FROM municipalities WHERE description IS NOT NULL
        )
        ORDER BY w.municipality_code
    """)
    rows = cur.fetchall()
    total = len(rows)
    if limit:
        rows = rows[:limit]
    print(f"Municipalities to process: {len(rows)} (of {total} remaining)")

    total_tokens = 0
    for i, (code, title, raw_text) in enumerate(rows):
        text = raw_text[:2000]  # Shorter for municipalities
        prompt = MUNICIPALITY_PROMPT.format(title=title, text=text)

        content, usage = call_mercury(prompt, i)
        if content:
            cur.execute(
                "UPDATE municipalities SET description = %s WHERE municipality_code = %s",
                (content, code)
            )
            conn.commit()
            tokens = usage.get("total_tokens", 0)
            total_tokens += tokens

            if (i + 1) % 50 == 0:
                print(f"  Progress: {i+1}/{len(rows)}, tokens: {total_tokens}")
        else:
            print(f"  FAIL {title}")

        time.sleep(1.5)  # Rotate keys, be conservative

    print(f"Municipality total tokens: {total_tokens}")
    return total_tokens


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "regions"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None

    conn = psycopg2.connect(DATABASE_URL)

    if mode == "regions":
        process_regions(conn)
    elif mode == "municipalities":
        process_municipalities(conn, limit)
    elif mode == "all":
        t1 = process_regions(conn)
        t2 = process_municipalities(conn, limit)
        print(f"\nGrand total tokens: {t1 + t2}")
    else:
        print(f"Usage: {sys.argv[0]} [regions|municipalities|all] [limit]")

    conn.close()


if __name__ == "__main__":
    main()
