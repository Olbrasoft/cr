#!/usr/bin/env python3
"""Generate perex (short summary, max 250 chars) for NPÚ landmarks using Gemma 3 27B.

Reads rewritten_text from npu_rewritten, sends to Gemini API for summarization,
stores result in the perex column (VARCHAR 256).

Rotates between N API keys with parallel requests.
Skips already-generated perexes. Safe to restart.
"""

from dotenv import load_dotenv
load_dotenv()
import os
import sys
import time
import requests
import psycopg2

STAGING_URL = os.environ.get("STAGING_DATABASE_URL", "postgresql:///cr_staging")

GEMINI_KEYS = [
    os.environ.get("GEMINI_API_KEY_1", ""),
    os.environ.get("GEMINI_API_KEY_2", ""),
    os.environ.get("GEMINI_API_KEY_3", ""),
    os.environ.get("GEMINI_API_KEY_4", ""),
]
GEMINI_KEYS = [k for k in GEMINI_KEYS if k]
if not GEMINI_KEYS:
    print("ERROR: No GEMINI_API_KEY_* environment variables set.", file=sys.stderr)
    sys.exit(1)
GEMINI_URL_TPL = "https://generativelanguage.googleapis.com/v1beta/models/gemma-3-27b-it:generateContent?key={}"

SYSTEM_PROMPT = """Vytvoř perex (krátké shrnutí) pro následující text o památce.

PRAVIDLA:
- Perex musí mít PŘESNĚ 200–250 znaků (včetně mezer)
- Musí to být 1–2 věty, které vystihují podstatu památky
- Zaměř se na: co to je, kde to stojí, čím je zajímavé/unikátní
- Piš v češtině, v oznamovacím způsobu, bez úvozovek
- NEPOUŽÍVEJ slova jako "perex", "shrnutí", "tento text"
- Napiš POUZE perex, nic jiného"""

PAUSE_BETWEEN_BATCHES = 3
RATE_LIMIT_PAUSE = 60


def generate_perex(text, key_index=0, max_retries=3):
    """Send text to Gemma 3 27B for perex generation."""
    key = GEMINI_KEYS[key_index % len(GEMINI_KEYS)]
    url = GEMINI_URL_TPL.format(key)

    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\n" + text}]}
        ],
        "generationConfig": {
            "temperature": 0.5,
            "maxOutputTokens": 200,
        },
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=payload, timeout=60)

            if resp.status_code == 429:
                wait = RATE_LIMIT_PAUSE * (attempt + 1)
                print(f"    429 rate limit (key {key_index}), waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                return None, f"HTTP {resp.status_code}: {resp.text[:200]}"

            data = resp.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return None, "No candidates (safety filter)"
            parts = candidates[0].get("content", {}).get("parts", [])
            if not parts:
                return None, "No content parts"
            content = parts[0].get("text", "").strip()
            if not content:
                return None, "Empty response"

            # Trim to 256 chars max (VARCHAR 256 limit)
            if len(content) > 256:
                # Try to cut at last sentence within 256 chars
                cut = content[:253]
                last_dot = cut.rfind(".")
                if last_dot > 150:
                    content = cut[: last_dot + 1]
                else:
                    content = cut + "..."

            return content, None

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            return None, str(e)

    return None, "Max retries exceeded (429)"


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 999999

    # Get texts that need perex
    cur.execute(
        "SELECT id, catalog_id, rewritten_text FROM npu_rewritten "
        "WHERE perex IS NULL AND rewritten_text IS NOT NULL "
        "AND LENGTH(rewritten_text) > 50 "
        "ORDER BY id LIMIT %s",
        (limit,),
    )
    rows = cur.fetchall()
    total = len(rows)
    print(f"Texts to summarize: {total}", flush=True)

    if total == 0:
        print("Nothing to do.", flush=True)
        return

    generated = 0
    failed = 0
    num_keys = len(GEMINI_KEYS)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    i = 0
    while i < total:
        batch = []
        for j in range(num_keys):
            idx = i + j
            if idx >= total:
                break
            row_id, catalog_id, text = rows[idx]
            batch.append((row_id, catalog_id, text, j))

        if not batch:
            i += num_keys
            continue

        with ThreadPoolExecutor(max_workers=num_keys) as executor:
            futures = {}
            for row_id, catalog_id, text, key_idx in batch:
                future = executor.submit(generate_perex, text, key_idx)
                futures[future] = (row_id, catalog_id)

            for future in as_completed(futures):
                row_id, catalog_id = futures[future]
                perex, err = future.result()

                if err:
                    print(f"  FAIL {catalog_id}: {err}", flush=True)
                    failed += 1
                else:
                    cur.execute(
                        "UPDATE npu_rewritten SET perex = %s WHERE id = %s",
                        (perex, row_id),
                    )
                    generated += 1

        conn.commit()
        i += num_keys

        if (generated + failed) % 60 == 0 and (generated + failed) > 0:
            print(
                f"  Progress: {generated + failed}/{total} "
                f"(generated: {generated}, failed: {failed})",
                flush=True,
            )

        time.sleep(PAUSE_BETWEEN_BATCHES)

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"\nDone! Generated: {generated}, Failed: {failed}, Total: {total}",
        flush=True,
    )


if __name__ == "__main__":
    main()
