#!/usr/bin/env python3
"""Generate unique Czech episode descriptions using Gemma 4.

Same pattern as generate-film-descriptions.py but for individual TV
episodes. Source = episodes.overview (TMDB CS when present, EN fallback).
Output → episodes.description (migration 037 added it as
`generated_description`, migration 051 renamed it to `description`
since the table never had a separate raw-description column).

Uses the 4 dev GEMINI_API_KEY_1..4 keys in parallel. With ~22k eligible
episodes and free-tier 1500 RPD per key, expect ~4 days of wall time.

Usage:
    python3 generate-episode-descriptions.py --test 5
    python3 generate-episode-descriptions.py --all
    python3 generate-episode-descriptions.py --all --dry-run
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import requests

# dotenv is optional — VPS relies on systemd/bash `source .env` instead
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    print("ERROR: DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
# Episodes where the CS overview is shorter than this threshold trigger
# an on-the-fly TMDB fetch for the EN overview, so Gemma gets enough
# source material to produce a distinctive Czech rewrite instead of a
# near-copy. Picked after observing many 80–130-char CS blurbs that are
# already the full localized synopsis.
SHORT_CS_THRESHOLD = 200

GEMINI_KEYS = [
    os.environ.get("GEMINI_API_KEY_1", ""),
    os.environ.get("GEMINI_API_KEY_2", ""),
    os.environ.get("GEMINI_API_KEY_3", ""),
    os.environ.get("GEMINI_API_KEY_4", ""),
]
GEMINI_KEYS = [k for k in GEMINI_KEYS if k]
if not GEMINI_KEYS:
    # Fall back to the single production key if the parallel dev keys
    # aren't present (so this script also runs on the VPS .env).
    single = os.environ.get("GEMINI_API_KEY", "").strip()
    if single:
        GEMINI_KEYS = [single]
if not GEMINI_KEYS:
    print("ERROR: No GEMINI_API_KEY* set", file=sys.stderr)
    sys.exit(1)

MODEL = "gemma-3-27b-it"
GEMINI_URL_TPL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}"
    f":generateContent?key={{}}"
)

PAUSE_BETWEEN_BATCHES = 3
RATE_LIMIT_PAUSE = 60


def fetch_tmdb_en_overview(tv_id: int, season: int, episode: int) -> str | None:
    """Best-effort EN overview fetch from TMDB for a specific episode.

    Called only when the stored CS overview is too short (< SHORT_CS_THRESHOLD).
    Returns None on any failure — the caller falls back to CS-only.
    """
    if not TMDB_API_KEY or tv_id is None:
        return None
    url = (
        f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}"
        f"?api_key={TMDB_API_KEY}&language=en-US"
    )
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        text = (r.json().get("overview") or "").strip()
        return text or None
    except requests.exceptions.RequestException:
        return None


def build_prompt(series_title: str, season: int, episode: int,
                 episode_name: str | None, cs_overview: str,
                 en_overview: str | None) -> str:
    ep_label = f"S{season:02d}E{episode:02d}"
    name_part = f' — „{episode_name}"' if episode_name else ""
    parts = [
        f"Toto jsou popisy epizody {ep_label}{name_part} ze seriálu "
        f"{series_title} z různých zdrojů:\n",
        f"Zdroj 1 (česky):\n{cs_overview}\n",
    ]
    if en_overview:
        parts.append(f"Zdroj 2 (anglicky):\n{en_overview}\n")
    parts.append(
        "Na základě uvedených popisů napiš JEDEN krátký originální český "
        "popis této epizody. Požadavky: 2-4 věty, 120-300 znaků, poutavý "
        "styl, vlastní formulace (ne kopie zdroje). Piš přímo o ději. "
        "Nekomentuj zadání, nepiš odrážky, nepiš nadpis. Odpověz pouze "
        "samotným textem popisu:"
    )
    return "\n".join(parts)


def call_gemma(prompt: str, key_index: int, max_retries: int = 3):
    key = GEMINI_KEYS[key_index % len(GEMINI_KEYS)]
    url = GEMINI_URL_TPL.format(key)
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 600},
    }
    for attempt in range(max_retries):
        start = time.time()
        try:
            resp = requests.post(url, json=payload, timeout=120)
            duration_ms = int((time.time() - start) * 1000)
            if resp.status_code == 429:
                wait = RATE_LIMIT_PAUSE * (attempt + 1)
                print(f"    429 (key {key_index}), waiting {wait}s", flush=True)
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                return None, duration_ms, f"HTTP {resp.status_code}"
            data = resp.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return None, duration_ms, "no candidates"
            parts = candidates[0].get("content", {}).get("parts", [])
            if not parts:
                return None, duration_ms, "no parts"
            content = parts[0].get("text", "").strip()
            if not content:
                return None, duration_ms, "empty"
            if content.startswith('"') and content.endswith('"'):
                content = content[1:-1]
            return content, duration_ms, None
        except requests.exceptions.RequestException as e:
            duration_ms = int((time.time() - start) * 1000)
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            return None, duration_ms, str(e)
    return None, 0, "max retries"


def get_episodes(conn, limit: int = 0):
    cur = conn.cursor()
    query = (
        "SELECT e.id, e.season, e.episode, e.episode_name, e.overview, "
        "       e.overview_en, s.title, s.tmdb_id "
        "FROM episodes e JOIN series s ON s.id = e.series_id "
        "WHERE e.overview IS NOT NULL AND e.overview != '' "
        "  AND (e.description IS NULL OR e.description = '') "
        "ORDER BY e.series_id, e.season, e.episode"
    )
    if limit > 0:
        query += f" LIMIT {limit}"
    cur.execute(query)
    return cur.fetchall()


def process_batch(batch, conn, dry_run: bool):
    cur = conn.cursor()
    results = []
    with ThreadPoolExecutor(max_workers=len(GEMINI_KEYS)) as executor:
        futures = {}
        for i, row in enumerate(batch):
            ep_id, season, ep_num, name, overview, existing_en, series_title, tv_id = row
            # Use cached EN from DB when we already fetched it before;
            # only hit TMDB when CS is short AND DB has no EN yet.
            en_overview = existing_en
            fetched_en_now = False
            if (
                not en_overview
                and overview
                and len(overview) < SHORT_CS_THRESHOLD
            ):
                en_overview = fetch_tmdb_en_overview(tv_id, season, ep_num)
                fetched_en_now = bool(en_overview)
                if en_overview and not dry_run:
                    cur.execute(
                        "UPDATE episodes SET overview_en = %s WHERE id = %s",
                        (en_overview, ep_id),
                    )
            prompt = build_prompt(series_title, season, ep_num, name, overview, en_overview)
            future = executor.submit(call_gemma, prompt, i)
            futures[future] = (
                ep_id, series_title, season, ep_num,
                bool(en_overview), fetched_en_now,
            )
        for future in as_completed(futures):
            ep_id, series_title, season, ep_num, used_en, fetched_en = futures[future]
            text, dur, error = future.result()
            en_tag = " +EN(fresh)" if fetched_en else (" +EN(cached)" if used_en else "")
            label = f"{series_title} S{season:02d}E{ep_num:02d}{en_tag}"
            if error:
                print(f"  FAIL: {label} — {error}", flush=True)
                results.append(False)
            else:
                if not dry_run:
                    cur.execute(
                        "UPDATE episodes SET description = %s WHERE id = %s",
                        (text, ep_id),
                    )
                print(f"  OK: {label} — {len(text)} chars, {dur}ms", flush=True)
                if dry_run:
                    print(f"      >>> {text[:160]}", flush=True)
                results.append(True)
    if not dry_run:
        conn.commit()
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", type=int, default=0)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(DB_URL)

    if args.test > 0:
        episodes = get_episodes(conn, limit=args.test)
    elif args.all:
        episodes = get_episodes(conn, limit=args.limit)
    else:
        ap.print_help()
        return

    total = len(episodes)
    if total == 0:
        print("Nothing to process")
        return
    print(f"Episodes to process: {total}")
    print(f"API keys: {len(GEMINI_KEYS)}")
    batches = (total + len(GEMINI_KEYS) - 1) // len(GEMINI_KEYS)
    est_s = batches * PAUSE_BETWEEN_BATCHES
    print(f"Estimated wall time: {est_s // 3600}h {(est_s % 3600) // 60}m")
    print()

    ok = 0
    fail = 0
    start = time.time()
    for batch_start in range(0, total, len(GEMINI_KEYS)):
        batch = episodes[batch_start:batch_start + len(GEMINI_KEYS)]
        for success in process_batch(batch, conn, args.dry_run):
            if success:
                ok += 1
            else:
                fail += 1
        done = batch_start + len(batch)
        pct = done * 100 // total
        elapsed = int(time.time() - start)
        eta = int(elapsed * (total - done) / max(done, 1))
        print(f"  [{done}/{total} {pct}%] ok={ok} fail={fail} "
              f"elapsed={elapsed}s eta={eta // 60}m", flush=True)
        if batch_start + len(GEMINI_KEYS) < total:
            time.sleep(PAUSE_BETWEEN_BATCHES)
    print(f"\nDone. ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
