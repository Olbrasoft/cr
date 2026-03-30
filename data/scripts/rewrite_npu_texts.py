#!/usr/bin/env python3
"""Rewrite NPÚ landmark texts using Gemini API (Gemma 3 27B).

Rotates between N API keys with parallel requests (one per key).
Retries on 429 with exponential backoff.
Saves to cr_staging.npu_rewritten table.
Skips already-rewritten texts. Safe to restart.
"""

from dotenv import load_dotenv
load_dotenv()
import os
import sys
import time
import requests
import psycopg2

STAGING_URL = "postgresql:///cr_staging"

# Google Gemini API keys (Gemma 3 27B, free tier: 14,400 RPD each)
GEMINI_KEYS = [
    os.environ.get("GEMINI_API_KEY_1", ""),
    os.environ.get("GEMINI_API_KEY_2", ""),
    os.environ.get("GEMINI_API_KEY_3", ""),
    os.environ.get("GEMINI_API_KEY_4", ""),
]
GEMINI_KEYS = [k for k in GEMINI_KEYS if k]  # filter empty
if not GEMINI_KEYS:
    print("ERROR: No GEMINI_API_KEY_* environment variables set. Exiting.", file=sys.stderr)
    sys.exit(1)
GEMINI_URL_TPL = "https://generativelanguage.googleapis.com/v1beta/models/gemma-3-27b-it:generateContent?key={}"

SYSTEM_PROMPT = """Jsi odborný copywriter specializující se na české kulturní dědictví. Přepiš poskytnutý text o památce podle těchto pravidel:

POVINNÉ:
- Zachovej VŠECHNA fakta, data, rozměry, jména osob a historické údaje beze změny
- Zachovej STEJNOU délku a podrobnost textu — nesmíš nic vynechat ani zkrátit
- Každý odstavec originálu přepiš jako samostatný odstavec

KLÍČOVÉ — JAK PŘEPISOVAT:
- KAŽDOU větu musíš přeformulovat jinak. NIKDY nekopíruj celá souvětí z originálu doslovně
- Změň slovosled vět — co bylo na začátku věty, dej doprostřed nebo na konec
- Použij jiné spojky, jiná příslovce, jiné větné konstrukce
- Rozděl dlouhá souvětí na kratší, nebo naopak spoj krátké věty do delších
- Odborné termíny (suprafenestra, rizalit, bosáž, klenba) zachovej, ale větu kolem nich postav jinak
- Místo "nachází se" použij "stojí", "je umístěn", "rozkládá se" — obměňuj slovesa
- Místo "je zaklenutý" použij "klenba pokrývá", "zastřešuje" apod.
- Historické pasáže přeformuluj — např. "v roce 1654 postavil palác" → "palác vznikl roku 1654"

CÍL: Text musí být natolik odlišný od originálu, aby ho vyhledávače NEPOVAŽOVALY za duplicitní obsah. Pokud bys porovnal originál a tvůj text větu po větě, žádné dvě věty by neměly být stejné.

Napiš POUZE přepsaný text, bez komentářů nebo vysvětlení."""

# Polite pause between batches of parallel requests (seconds)
PAUSE_BETWEEN_BATCHES = 3
# Extra pause after 429 rate limit
RATE_LIMIT_PAUSE = 60


def rewrite_with_retry(text, key_index=0, max_retries=3):
    """Send text to Gemma 3 27B via Gemini API for rewriting."""
    key = GEMINI_KEYS[key_index % len(GEMINI_KEYS)]
    url = GEMINI_URL_TPL.format(key)

    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\n" + text}]}
        ],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 4000,
        },
    }

    for attempt in range(max_retries):
        start = time.time()
        try:
            resp = requests.post(url, json=payload, timeout=120)
            duration_ms = int((time.time() - start) * 1000)

            if resp.status_code == 429:
                wait = RATE_LIMIT_PAUSE * (attempt + 1)
                print(f"    429 rate limit (key {key_index}), waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                return None, duration_ms, f"HTTP {resp.status_code}: {resp.text[:200]}"

            data = resp.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return None, duration_ms, "No candidates (safety filter)"
            parts = candidates[0].get("content", {}).get("parts", [])
            if not parts:
                return None, duration_ms, "No content parts"
            content = parts[0].get("text", "").strip()
            if not content:
                return None, duration_ms, "Empty response"

            usage = data.get("usageMetadata", {})
            return {
                "text": content,
                "input_tokens": usage.get("promptTokenCount", 0),
                "output_tokens": usage.get("candidatesTokenCount", 0),
                "duration_ms": duration_ms,
            }, duration_ms, None

        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            if attempt < max_retries - 1:
                time.sleep(10)
                continue
            return None, duration_ms, str(e)

    return None, 0, "Max retries exceeded (429)"


def build_source_text(row):
    """Build source text from NPÚ detail fields."""
    _, _, annotation, description, history = row
    parts = []
    if annotation and annotation.strip():
        parts.append(annotation.strip())
    if description and description.strip():
        parts.append(description.strip())
    if history and history.strip():
        parts.append(history.strip())
    return "\n\n".join(parts)


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 999999

    cur.execute("""
        SELECT d.catalog_id, d.name, d.annotation, d.description, d.historical_development
        FROM npu_details d
        WHERE (d.description IS NOT NULL AND LENGTH(d.description) > 100)
           OR (d.historical_development IS NOT NULL AND LENGTH(d.historical_development) > 100)
        ORDER BY d.catalog_id
    """)
    rows = cur.fetchall()

    # Filter out already rewritten
    cur.execute("SELECT catalog_id FROM npu_rewritten")
    done = {r[0] for r in cur.fetchall()}
    rows = [r for r in rows if r[0] not in done][:limit]

    total = len(rows)
    print(f"Texts to rewrite: {total}", flush=True)

    if total == 0:
        print("Nothing to do.", flush=True)
        return

    rewritten = 0
    failed = 0
    num_keys = len(GEMINI_KEYS)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Process in batches of num_keys (num_keys parallel requests, one per API key)
    i = 0
    while i < total:
        batch = []
        for j in range(num_keys):
            idx = i + j
            if idx >= total:
                break
            row = rows[idx]
            source = build_source_text(row)
            if len(source) < 50:
                continue
            batch.append((row[0], row[1], source, j))

        if not batch:
            i += num_keys
            continue

        # Send batch in parallel (one request per API key)
        with ThreadPoolExecutor(max_workers=num_keys) as executor:
            futures = {}
            for catalog_id, name, source, key_idx in batch:
                future = executor.submit(rewrite_with_retry, source, key_idx)
                futures[future] = (catalog_id, name, source)

            for future in as_completed(futures):
                catalog_id, name, source = futures[future]
                result, dur_ms, err = future.result()

                if err:
                    print(f"  FAIL {name}: {err}", flush=True)
                    failed += 1
                else:
                    cur.execute("""
                        INSERT INTO npu_rewritten (catalog_id, model, original_text, rewritten_text,
                                                   input_tokens, output_tokens, duration_ms)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (catalog_id) DO NOTHING
                    """, (
                        catalog_id, "gemma-3-27b-it", source, result["text"],
                        result["input_tokens"], result["output_tokens"],
                        result["duration_ms"],
                    ))
                    rewritten += 1

        conn.commit()
        i += num_keys

        if (rewritten + failed) % 30 == 0 and (rewritten + failed) > 0:
            print(
                f"  Progress: {rewritten + failed}/{total} "
                f"(rewritten: {rewritten}, failed: {failed})",
                flush=True,
            )

        # Polite pause between batches (3 requests done, wait before next 3)
        time.sleep(PAUSE_BETWEEN_BATCHES)

    conn.commit()
    cur.close()
    conn.close()

    print(
        f"\nDone! Rewritten: {rewritten}, Failed: {failed}, Total: {total}",
        flush=True,
    )


if __name__ == "__main__":
    main()
