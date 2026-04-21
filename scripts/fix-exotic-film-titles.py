#!/usr/bin/env python3
"""Backfill for issue #572 — rename films with unreadable foreign-script titles.

Finds every `films` row whose `title` contains non-Latin script glyphs (CJK,
Devanagari, Hangul, Cyrillic, Arabic, Thai, Hebrew). For each row:
  1. Pick new display title:
       - if `original_title` is Latin-only → use it
       - else fetch TMDB en-US title (if the row has `tmdb_id`)
       - else log & skip (reported in summary as "residue")
  2. Slugify the new title; suffix with `-YYYY` / `-N` on collision
     (per-table rule in CLAUDE.md — films.slug is unique across films only).
  3. UPDATE films SET title=..., slug=... WHERE id=...

Old slug is lost — no 301 redirects (per maintainer decision on issue #572).

Usage:
    DATABASE_URL=... TMDB_API_KEY=... python3 scripts/fix-exotic-film-titles.py --dry-run
    DATABASE_URL=... TMDB_API_KEY=... python3 scripts/fix-exotic-film-titles.py --apply

Resume semantics: idempotent. Running again after partial application only
touches rows whose title still matches the non-Latin pattern.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
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

from scripts.auto_import.enricher import _slugify  # noqa: E402
from scripts.auto_import.tmdb_resolver import _NON_LATIN_SCRIPT_RE  # noqa: E402

DB_URL = os.environ.get("DATABASE_URL", "").strip()
TMDB_KEY = os.environ.get("TMDB_API_KEY", "").strip()
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_SLEEP = 0.1


def _pick_new_title(cur, row_id: int, current_title: str, original_title: str | None,
                    tmdb_id: int | None, en_title_from_tmdb: dict[int, str]) -> str | None:
    """Return a readable Latin-script title for this row, or None if nothing works."""
    if original_title and not _NON_LATIN_SCRIPT_RE.search(original_title):
        return original_title
    if not tmdb_id:
        return None
    if tmdb_id in en_title_from_tmdb:
        return en_title_from_tmdb[tmdb_id]
    if not TMDB_KEY:
        return None
    try:
        r = requests.get(
            f"{TMDB_BASE}/movie/{tmdb_id}",
            params={"api_key": TMDB_KEY, "language": "en-US"},
            timeout=15,
        )
    except requests.RequestException as e:
        print(f"  [net] tmdb_id={tmdb_id}: {e}", file=sys.stderr)
        return None
    time.sleep(TMDB_SLEEP)
    if r.status_code != 200:
        print(f"  [HTTP {r.status_code}] tmdb_id={tmdb_id}", file=sys.stderr)
        return None
    try:
        data = r.json()
    except ValueError:
        return None
    en_title = (data.get("title") or "").strip()
    if not en_title or _NON_LATIN_SCRIPT_RE.search(en_title):
        return None
    en_title_from_tmdb[tmdb_id] = en_title
    return en_title


def _unique_slug(cur, base: str, year: int | None, self_id: int) -> str:
    """Free slug: base → base-YYYY → base-2 → base-3 …

    Ignores collisions with this row itself — we're about to overwrite it.
    """
    if not base:
        base = "film"
    cur.execute("SELECT 1 FROM films WHERE slug = %s AND id <> %s", (base, self_id))
    if not cur.fetchone():
        return base
    if year:
        candidate = f"{base}-{year}"
        cur.execute("SELECT 1 FROM films WHERE slug = %s AND id <> %s", (candidate, self_id))
        if not cur.fetchone():
            return candidate
    counter = 2
    while True:
        candidate = f"{base}-{counter}"
        cur.execute("SELECT 1 FROM films WHERE slug = %s AND id <> %s", (candidate, self_id))
        if not cur.fetchone():
            return candidate
        counter += 1


_NON_LATIN_REGEX_PG = (
    r"[\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF\u0900-\u097F"
    r"\u0600-\u06FF\u0400-\u04FF\u0500-\u052F\u05D0-\u05EA\u0E00-\u0E7F]"
)


def main() -> int:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true",
                   help="Print planned (old → new) renames without touching the DB.")
    g.add_argument("--apply", action="store_true",
                   help="Execute the UPDATEs in a single transaction.")
    args = p.parse_args()

    if not DB_URL:
        sys.exit("ERROR: DATABASE_URL env var required (set via .env or shell).")

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute(
        """SELECT id, slug, title, original_title, tmdb_id, year
             FROM films
            WHERE title ~ %s
         ORDER BY id""",
        (_NON_LATIN_REGEX_PG,),
    )
    rows = cur.fetchall()
    print(f"Found {len(rows)} films with non-Latin title.")

    planned: list[tuple[int, str, str, str, str]] = []   # (id, old_slug, new_title, new_slug, source)
    residue: list[tuple[int, str, str | None, int | None]] = []  # (id, title, original_title, tmdb_id)
    tmdb_cache: dict[int, str] = {}

    for row_id, old_slug, title, original_title, tmdb_id, year in rows:
        new_title = _pick_new_title(cur, row_id, title, original_title, tmdb_id, tmdb_cache)
        if not new_title:
            residue.append((row_id, title, original_title, tmdb_id))
            continue
        source = "original_title" if new_title == original_title else "tmdb_en"
        base = _slugify(new_title)
        new_slug = _unique_slug(cur, base, year, row_id)
        planned.append((row_id, old_slug, new_title, new_slug, source))

    print(f"\nPlanned renames: {len(planned)}   Residue (skipped): {len(residue)}")
    print("\n  id    old_slug                      → new_slug                      [source] new_title")
    for row_id, old_slug, new_title, new_slug, source in planned:
        print(f"  {row_id:<5} {old_slug[:30]:<30} → {new_slug[:30]:<30} [{source}] {new_title}")

    if residue:
        print(f"\nResidue ({len(residue)} rows — no Latin title available):")
        for row_id, title, original_title, tmdb_id in residue:
            print(f"  id={row_id} tmdb_id={tmdb_id} title={title!r} original={original_title!r}")

    if args.dry_run:
        print("\n[dry-run] no DB writes. Use --apply to execute.")
        conn.rollback()
        return 0

    for row_id, old_slug, new_title, new_slug, _source in planned:
        cur.execute(
            "UPDATE films SET title = %s, slug = %s WHERE id = %s",
            (new_title, new_slug, row_id),
        )
    conn.commit()
    print(f"\nCommitted {len(planned)} updates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
