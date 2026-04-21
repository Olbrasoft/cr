#!/usr/bin/env python3
"""Three-step Gemma backfill for prehraj.to-only films (#524, #527).

The 8 774 films imported by scripts/import-prehrajto-new-films.py have raw
TMDB cs-CZ overview in cr_dev.films.description. This pipeline replaces that
with a unique Gemma-4 Czech paraphrase, while permanently preserving the
raw sources in cr_staging.films_gemma_queue.

Subcommands (run in order, each restartable):
    fetch  — pull TMDB cs-CZ + en-US overview via API, store in queue
    gemma  — send cs+en to Gemma 4, store result in queue.gemma_text
    apply  — write queue.gemma_text to cr_dev.films.description
    stats  — show queue progress

Usage:
    STAGING_DATABASE_URL=...  DATABASE_URL=...  TMDB_API_KEY=...  GEMINI_API_KEY_1..4=... \\
      python3 scripts/backfill_prehrajto_gemma.py fetch --limit 5 --dry-run
    python3 scripts/backfill_prehrajto_gemma.py fetch              # all pending
    python3 scripts/backfill_prehrajto_gemma.py gemma              # all pending
    python3 scripts/backfill_prehrajto_gemma.py apply              # all pending

Resume semantics:
    fetch   processes rows where tmdb_fetched_at IS NULL
    gemma   processes rows where tmdb_fetched_at IS NOT NULL AND gemma_text IS NULL
    apply   processes rows where gemma_text IS NOT NULL AND applied_to_dev_at IS NULL
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import psycopg2
import requests

_SCRIPTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPTS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass

from scripts.auto_import.gemma_writer import (  # noqa: E402
    build_prompt_film,
    call_gemma,
    load_keys,
)

STAGING_URL = os.environ.get("STAGING_DATABASE_URL", "").strip()
DEV_URL     = os.environ.get("DATABASE_URL", "").strip()
TMDB_KEY    = os.environ.get("TMDB_API_KEY", "")

if not STAGING_URL or not DEV_URL:
    sys.exit("ERROR: STAGING_DATABASE_URL and DATABASE_URL env vars required "
             "(set via .env or the shell). No credential fallback — this script "
             "refuses to guess passwords.")
TMDB_BASE   = "https://api.themoviedb.org/3"

# Per TMDB rate limits (40 req / 10s) — 0.1s sleep → ~10 req/s across one thread
TMDB_SLEEP  = 0.1

# Gemma generation knobs — mirrors generate-film-descriptions-prehrajto.py
GEMMA_PAUSE_BETWEEN_BATCHES = 3
GEMMA_MAX_RETRIES = 3


def _staging():
    return psycopg2.connect(STAGING_URL)


def _dev():
    return psycopg2.connect(DEV_URL)


# ---------------------------------------------------------------- step: fetch

def _tmdb_fetch(tmdb_id: int, lang: str) -> tuple[str | None, str | None]:
    """Return (overview, error). overview is None if TMDB has nothing for this lang."""
    try:
        r = requests.get(
            f"{TMDB_BASE}/movie/{tmdb_id}",
            params={"api_key": TMDB_KEY, "language": lang},
            timeout=15,
        )
    except requests.RequestException as e:
        return None, f"net: {e}"
    if r.status_code == 404:
        return None, "404"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    try:
        overview = (r.json().get("overview") or "").strip() or None
    except ValueError:
        return None, "invalid json"
    return overview, None


def cmd_fetch(args: argparse.Namespace) -> int:
    if not TMDB_KEY:
        print("ERROR: TMDB_API_KEY not set", file=sys.stderr)
        return 1

    conn = _staging()
    cur = conn.cursor()
    q = "SELECT film_id, tmdb_id, title FROM films_gemma_queue WHERE tmdb_fetched_at IS NULL ORDER BY film_id"
    if args.limit:
        q += f" LIMIT {args.limit}"
    cur.execute(q)
    rows = cur.fetchall()
    total = len(rows)
    print(f"[fetch] {total} films to fetch from TMDB", flush=True)
    if args.dry_run:
        print("  (dry-run — no DB writes)")
    done = failed = 0
    for i, (film_id, tmdb_id, title) in enumerate(rows, 1):
        cs, cs_err = _tmdb_fetch(tmdb_id, "cs-CZ")
        time.sleep(TMDB_SLEEP)
        en, en_err = _tmdb_fetch(tmdb_id, "en-US")
        time.sleep(TMDB_SLEEP)
        err_parts = [p for p in (cs_err, en_err) if p]
        err = "; ".join(err_parts) or None

        if args.dry_run:
            print(f"  [{i}/{total}] id={film_id} tmdb={tmdb_id} {title!r}")
            print(f"      cs ({len(cs or '')}): {(cs or '')[:80]!r}")
            print(f"      en ({len(en or '')}): {(en or '')[:80]!r}")
            if err: print(f"      err: {err}")
            continue

        cur.execute(
            """UPDATE films_gemma_queue
                  SET tmdb_cs = %s,
                      tmdb_en = %s,
                      tmdb_fetched_at = now(),
                      tmdb_fetch_error = %s
                WHERE film_id = %s""",
            (cs, en, err, film_id),
        )
        if cs or en:
            done += 1
        else:
            failed += 1
        if i % 50 == 0:
            conn.commit()
            print(f"  [{i}/{total}] done={done} failed={failed}", flush=True)
    conn.commit()
    print(f"[fetch] DONE total={total} done={done} failed={failed}")
    return 0


# ---------------------------------------------------------------- step: gemma

def _gemma_one(args_tuple):
    """Worker: takes (film_id, title, year, cs, en, key) → (film_id, text|None, err|None)."""
    film_id, title, year, cs, en, key = args_tuple
    sources = []
    if cs: sources.append(("TMDB CS", cs))
    if en: sources.append(("TMDB EN", en))
    if not sources:
        return film_id, None, "no sources"
    prompt = build_prompt_film(title, year, sources)
    for _ in range(GEMMA_MAX_RETRIES):
        text = call_gemma(prompt, key)
        if text:
            return film_id, text, None
    return film_id, None, "gemma failed after retries"


def cmd_gemma(args: argparse.Namespace) -> int:
    keys = load_keys()
    if not keys:
        print("ERROR: no GEMINI_API_KEY_* env vars", file=sys.stderr)
        return 1
    print(f"[gemma] using {len(keys)} Gemini key(s)")

    conn = _staging()
    cur = conn.cursor()
    q = """SELECT film_id, title, year, tmdb_cs, tmdb_en
             FROM films_gemma_queue
            WHERE tmdb_fetched_at IS NOT NULL
              AND gemma_text IS NULL
              AND (tmdb_cs IS NOT NULL OR tmdb_en IS NOT NULL)
         ORDER BY film_id"""
    if args.limit:
        q += f" LIMIT {args.limit}"
    cur.execute(q)
    rows = cur.fetchall()
    total = len(rows)
    print(f"[gemma] {total} films to process (batches of {len(keys)})", flush=True)
    done = failed = 0

    for batch_start in range(0, total, len(keys)):
        batch = rows[batch_start:batch_start + len(keys)]
        jobs = [(film_id, title, year or 0, cs, en, keys[idx % len(keys)])
                for idx, (film_id, title, year, cs, en) in enumerate(batch)]
        with ThreadPoolExecutor(max_workers=len(keys)) as ex:
            futures = [ex.submit(_gemma_one, j) for j in jobs]
            for fut in as_completed(futures):
                film_id, text, err = fut.result()
                if args.dry_run:
                    print(f"  film_id={film_id} err={err} text={(text or '')[:120]!r}")
                    continue
                cur.execute(
                    """UPDATE films_gemma_queue
                          SET gemma_text = %s,
                              gemma_generated_at = CASE WHEN %s IS NOT NULL THEN now() ELSE NULL END,
                              gemma_error = %s
                        WHERE film_id = %s""",
                    (text, text, err, film_id),
                )
                if text: done += 1
                else:    failed += 1
        conn.commit()
        if batch_start and batch_start % (len(keys) * 10) == 0:
            print(f"  [{batch_start + len(keys)}/{total}] done={done} failed={failed}", flush=True)
        time.sleep(GEMMA_PAUSE_BETWEEN_BATCHES)
    print(f"[gemma] DONE total={total} done={done} failed={failed}")
    return 0


# ---------------------------------------------------------------- step: apply

def cmd_apply(args: argparse.Namespace) -> int:
    staging = _staging()
    scur = staging.cursor()
    q = """SELECT film_id, gemma_text
             FROM films_gemma_queue
            WHERE gemma_text IS NOT NULL
              AND applied_to_dev_at IS NULL
         ORDER BY film_id"""
    if args.limit:
        q += f" LIMIT {args.limit}"
    scur.execute(q)
    rows = scur.fetchall()
    total = len(rows)
    print(f"[apply] {total} rows to copy queue.gemma_text → cr_dev.films.description")
    if args.dry_run:
        for film_id, text in rows[:5]:
            print(f"  id={film_id} text={(text or '')[:120]!r}")
        print("  (dry-run)")
        return 0

    dev = _dev()
    dcur = dev.cursor()
    done = 0
    for i, (film_id, text) in enumerate(rows, 1):
        dcur.execute("UPDATE films SET description = %s WHERE id = %s", (text, film_id))
        scur.execute(
            "UPDATE films_gemma_queue SET applied_to_dev_at = now() WHERE film_id = %s",
            (film_id,),
        )
        done += 1
        if i % 100 == 0:
            dev.commit(); staging.commit()
            print(f"  [{i}/{total}]", flush=True)
    dev.commit(); staging.commit()
    print(f"[apply] DONE total={total} done={done}")
    return 0


# ---------------------------------------------------------------- step: stats

def cmd_stats(args: argparse.Namespace) -> int:
    conn = _staging()
    cur = conn.cursor()
    cur.execute("""
        SELECT
          COUNT(*)                                                AS total,
          COUNT(*) FILTER (WHERE tmdb_fetched_at IS NOT NULL)     AS fetched,
          COUNT(*) FILTER (WHERE tmdb_fetch_error IS NOT NULL)    AS fetch_err,
          COUNT(*) FILTER (WHERE gemma_text IS NOT NULL)          AS gemma_done,
          COUNT(*) FILTER (WHERE gemma_error IS NOT NULL)         AS gemma_err,
          COUNT(*) FILTER (WHERE applied_to_dev_at IS NOT NULL)   AS applied
        FROM films_gemma_queue
    """)
    r = cur.fetchone()
    print(f"queue total:        {r[0]}")
    print(f"  tmdb fetched:     {r[1]}  (errors: {r[2]})")
    print(f"  gemma generated:  {r[3]}  (errors: {r[4]})")
    print(f"  applied to dev:   {r[5]}")
    return 0


# ---------------------------------------------------------------- main

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("fetch", "gemma", "apply"):
        s = sub.add_parser(name)
        s.add_argument("--limit", type=int, default=0, help="process at most N rows (0 = all)")
        s.add_argument("--dry-run", action="store_true")
    sub.add_parser("stats")
    args = p.parse_args()

    return {
        "fetch": cmd_fetch,
        "gemma": cmd_gemma,
        "apply": cmd_apply,
        "stats": cmd_stats,
    }[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
