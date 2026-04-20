#!/usr/bin/env python3
"""Generate unique Czech descriptions for films imported from prehraj.to (#527).

Target cohort (from #524): films whose `sktorrent_video_id` is NULL, have a
`prehrajto_primary_upload_id`, and whose `generated_description` is still NULL.
These are the ~8 784 new films created by `scripts/import-prehrajto-new-films.py`
with `description` copied verbatim from TMDB (cs-CZ `.overview` or fallback
en-US `.overview`). This script runs them through Gemma 4 (Gemini API) to
produce unique 150–400 char Czech paraphrases for SEO — avoiding duplicate
TMDB content across multiple Czech film sites.

Reuses the 4-key rotation + prompt builder from `generate-film-descriptions.py`
so we don't diverge on prompt tuning, and writes to `generated_description`
only (NOT also to `description` — the film-detail template already prefers
`generated_description` when set).

Usage:
    python3 scripts/generate-film-descriptions-prehrajto.py --dry-run --limit 5
    python3 scripts/generate-film-descriptions-prehrajto.py              # all
    python3 scripts/generate-film-descriptions-prehrajto.py --limit 100
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import psycopg2

# Reuse GEMINI_KEYS, build_prompt, call_gemma, PAUSE_BETWEEN_BATCHES from the
# existing batch script. Import via importlib because the filename has hyphens
# and isn't a valid module name for a plain `import`.
_HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location(
    "gen", _HERE / "generate-film-descriptions.py"
)
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)

DB_URL = os.environ.get(
    "DATABASE_URL", "postgres://cr_dev_user:cr_dev_2026@localhost/cr_dev"
)


COHORT_WHERE = (
    "sktorrent_video_id IS NULL "
    "AND prehrajto_primary_upload_id IS NOT NULL "
    "AND generated_description IS NULL "
    "AND description IS NOT NULL "
    "AND length(description) >= 30"
)


def fetch_cohort(conn, limit: int) -> list[tuple[int, str, int, str]]:
    cur = conn.cursor()
    q = f"""
        SELECT id, title, COALESCE(year, 0), description
          FROM films
         WHERE {COHORT_WHERE}
         ORDER BY id
    """
    if limit:
        q += f" LIMIT {limit}"
    cur.execute(q)
    return cur.fetchall()


def process_batch(batch, conn, dry_run: bool) -> tuple[int, int]:
    """Fire one Gemma call per row on its own worker key. Returns (ok, fail)."""
    ok = 0
    fail = 0
    cur = conn.cursor()
    keys = gen.GEMINI_KEYS
    with ThreadPoolExecutor(max_workers=len(keys)) as ex:
        futures = {}
        for i, (fid, title, year, desc) in enumerate(batch):
            sources = [("TMDB", desc)]
            prompt = gen.build_prompt(title, year, sources)
            futures[ex.submit(gen.call_gemma, prompt, i)] = (fid, title, year)

        for fut in as_completed(futures):
            fid, title, year = futures[fut]
            text, ms, err = fut.result()
            if err or not text:
                fail += 1
                print(f"  FAIL id={fid} {title} ({year}) — {err}", flush=True)
                continue
            ok += 1
            if not dry_run:
                cur.execute(
                    "UPDATE films SET generated_description = %s WHERE id = %s",
                    (text, fid),
                )
            print(
                f"  OK  id={fid} {title} ({year}) → {len(text)} chars, {ms}ms",
                flush=True,
            )
            if dry_run:
                print(f"      >>> {text[:160]}...", flush=True)

    if not dry_run:
        conn.commit()
    return ok, fail


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N films (0 = all eligible)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Don't write to DB — print generated text instead")
    args = ap.parse_args()

    if not gen.GEMINI_KEYS:
        print("ERROR: No GEMINI_API_KEY_* set in .env", file=sys.stderr)
        return 2

    conn = psycopg2.connect(DB_URL)
    try:
        rows = fetch_cohort(conn, args.limit)
        total = len(rows)
        if total == 0:
            print("Nothing to do — cohort empty. Either #524 hasn't imported "
                  "new films yet, or all of them already have "
                  "`generated_description`.")
            return 0

        keys = gen.GEMINI_KEYS
        est_batches = (total + len(keys) - 1) // len(keys)
        est_sec = est_batches * gen.PAUSE_BETWEEN_BATCHES
        print(f"Cohort size: {total}")
        print(f"API keys: {len(keys)}")
        print(f"Batch size: {len(keys)}, pause: {gen.PAUSE_BETWEEN_BATCHES}s")
        print(f"Estimated floor time: {est_sec // 3600}h "
              f"{(est_sec % 3600) // 60}m (excluding per-call latency)")

        ok_total = 0
        fail_total = 0
        start = time.time()
        for batch_start in range(0, total, len(keys)):
            batch = rows[batch_start:batch_start + len(keys)]
            ok, fail = process_batch(batch, conn, args.dry_run)
            ok_total += ok
            fail_total += fail

            done = ok_total + fail_total
            if done % 50 == 0 or done == total:
                elapsed = time.time() - start
                rate = done / elapsed * 3600 if elapsed > 0 else 0
                print(
                    f"\n--- Progress: {done}/{total} "
                    f"({ok_total} ok, {fail_total} fail, {rate:.0f}/h) ---\n",
                    flush=True,
                )
            if batch_start + len(keys) < total:
                time.sleep(gen.PAUSE_BETWEEN_BATCHES)

        elapsed = time.time() - start
        print(f"\nDone in {elapsed:.0f}s ({elapsed / 60:.1f}m). "
              f"OK={ok_total}, Fail={fail_total}, Total={total}")
        return 0 if fail_total == 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
