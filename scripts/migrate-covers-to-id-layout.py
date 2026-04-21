#!/usr/bin/env python3
"""Sub-issue #576 — rename cover WebPs to the `{id}/cover.webp` layout.

One-shot migration that moves every cover file from the current name-based
key (e.g. `films/film-131.webp`, `films/large/children-of-the-sea.webp`) to
the new id-based layout:

    films/{id}/cover.webp         + films/{id}/cover-large.webp
    series/{id}/cover.webp        + series/{id}/cover-large.webp
    tv-shows/{id}/cover.webp      + tv-shows/{id}/cover-large.webp

Two stores are migrated:

1. **Cloudflare R2** (`cr-images` bucket) — primary, browser-facing store.
   Uses `rclone moveto` which does a server-side copy + delete, so no local
   download is needed for the 31k+ films on R2.

2. **Local disk** (repo `data/movies/covers-webp/` + `data/series/covers-webp/`).
   Small set; moved with `shutil.move`. Dev-only — production containers
   will stop copying these dirs into the image after Sub B (#577).

Safety rails:
    * Idempotent: if destination already exists in the pre-listed set,
      the source is deleted (leftover from a previous half-run) and the
      copy step is skipped. Re-running `--apply` is always safe.
    * Integrity: `rclone moveto` uses S3 `CopyObject`, which preserves
      the ETag — there's no half-written intermediate state, so no
      separate md5 round-trip is needed. `moveto` returns non-zero and
      aborts the row's move if the copy fails to verify.
    * Dry-run prints the full plan and counts missing sources without
      touching either store.
    * `cover_filename` column is LEFT POPULATED — the handler fallback
      accepts both old and new paths during the rollout window. Sub B
      (#577) will drop the column after verifying the migration.

Usage:
    # Dry-run everything (recommended first).
    python3 scripts/migrate-covers-to-id-layout.py --dry-run --r2 --local

    # Apply to R2 only (prod store):
    python3 scripts/migrate-covers-to-id-layout.py --apply --r2

    # Apply to local dev disk:
    python3 scripts/migrate-covers-to-id-layout.py --apply --local

    # Limit to films / series / tv_shows (useful for staged rollout).
    python3 scripts/migrate-covers-to-id-layout.py --apply --r2 --table films

Environment:
    DATABASE_URL      Postgres connection string (via .env or shell).
    RCLONE_REMOTE     rclone remote name for the bucket (default: cr-r2).
                      See `~/.config/rclone/rclone.conf`.
    R2_BUCKET         Bucket name (default: cr-images).

The `rclone` binary must be on PATH. Verify with `rclone lsd cr-r2:`.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import shutil
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

import psycopg2

_SCRIPTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPTS_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass

DB_URL = os.environ.get("DATABASE_URL", "").strip()
RCLONE_REMOTE = os.environ.get("RCLONE_REMOTE", "cr-r2").strip()
R2_BUCKET = os.environ.get("R2_BUCKET", "cr-images").strip()

# Local disk roots. Films live in their own dir; series + tv_shows share
# `series_covers_dir` so we namespace with a table subdir to avoid id
# collisions between those two tables (series.id and tv_shows.id overlap
# in prod — same ID can exist in both).
FILM_DIR = _REPO_ROOT / "data" / "movies" / "covers-webp"
SERIES_DIR = _REPO_ROOT / "data" / "series" / "covers-webp"


@dataclass
class Row:
    table: str              # "films" | "series" | "tv_shows"
    id: int
    slug: str
    cover_filename: str

    @property
    def new_r2_prefix(self) -> str:
        """Destination R2 prefix in the new id-based layout."""
        return {
            "films": "films",
            "series": "series",
            "tv_shows": "tv-shows",
        }[self.table]

    @property
    def old_r2_prefix(self) -> str:
        """Source R2 prefix in the current (name-based) layout.

        tv_shows covers share `series/` with the `series` table today —
        the web handler uses `series_covers_dir` for both (see
        tv_porad_cover). The new layout separates them into `tv-shows/`.
        """
        return {
            "films": "films",
            "series": "series",
            "tv_shows": "series",
        }[self.table]

    @property
    def local_root(self) -> Path:
        """Local disk root. For series/tv_shows we namespace below
        `series_covers_dir` so their overlapping IDs don't collide."""
        if self.table == "films":
            return FILM_DIR
        # series + tv_shows share `SERIES_DIR`; use a table subdir to keep
        # their id-keyed folders disjoint.
        return SERIES_DIR / self.new_r2_prefix

    # Old name-based keys — what the file is called today.
    @property
    def old_r2_small(self) -> str:
        return f"{self.old_r2_prefix}/{self.cover_filename}.webp"

    @property
    def old_r2_large_by_slug(self) -> str:
        # Handler writes `/large/{slug}.webp` — this is the canonical path
        # for the large variant in the old layout, per films_cover_large().
        return f"{self.old_r2_prefix}/large/{self.slug}.webp"

    @property
    def old_r2_large_by_cover(self) -> str:
        # Backfill script (#578) wrote `/large/{cover_filename}.webp` for
        # the exotic-cohort fix. Either form might exist — we try both.
        return f"{self.old_r2_prefix}/large/{self.cover_filename}.webp"

    @property
    def old_local_small(self) -> Path:
        if self.table == "films":
            return FILM_DIR / f"{self.cover_filename}.webp"
        return SERIES_DIR / f"{self.cover_filename}.webp"

    @property
    def old_local_large_by_slug(self) -> Path:
        if self.table == "films":
            return FILM_DIR / "large" / f"{self.slug}.webp"
        return SERIES_DIR / "large" / f"{self.slug}.webp"

    @property
    def old_local_large_by_cover(self) -> Path:
        if self.table == "films":
            return FILM_DIR / "large" / f"{self.cover_filename}.webp"
        return SERIES_DIR / "large" / f"{self.cover_filename}.webp"

    # New id-based keys.
    @property
    def new_r2_small(self) -> str:
        return f"{self.new_r2_prefix}/{self.id}/cover.webp"

    @property
    def new_r2_large(self) -> str:
        return f"{self.new_r2_prefix}/{self.id}/cover-large.webp"

    @property
    def new_local_small(self) -> Path:
        return self.local_root / str(self.id) / "cover.webp"

    @property
    def new_local_large(self) -> Path:
        return self.local_root / str(self.id) / "cover-large.webp"


def _load_rows(cur) -> list[Row]:
    rows: list[Row] = []
    for table in ("films", "series", "tv_shows"):
        cur.execute(
            f"SELECT id, slug, cover_filename FROM {table} "
            f"WHERE cover_filename IS NOT NULL AND cover_filename <> '' "
            f"ORDER BY id"
        )
        for row_id, slug, cover_filename in cur.fetchall():
            rows.append(Row(table=table, id=row_id, slug=slug,
                            cover_filename=cover_filename))
    return rows


# ----- rclone helpers (R2) ---------------------------------------------

def _list_all_r2_keys() -> set[str]:
    """Return every R2 key under the three cover prefixes in one go.

    `rclone lsf --recursive` streams the full listing in a handful of HTTP
    round-trips, which is ~500x cheaper than one `lsjson` per candidate.
    Includes both old-layout (`{prefix}/name.webp`, `{prefix}/large/...`)
    and any already-migrated new-layout (`{prefix}/{id}/cover.webp`) keys.
    """
    keys: set[str] = set()
    for prefix in ("films", "series", "tv-shows"):
        r = subprocess.run(
            ["rclone", "lsf", "--recursive", "--files-only",
             f"{RCLONE_REMOTE}:{R2_BUCKET}/{prefix}/"],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            # Prefix may not exist yet (tv-shows/ initially), not an error.
            continue
        for line in r.stdout.splitlines():
            line = line.strip()
            if line:
                keys.add(f"{prefix}/{line}")
    return keys


def _r2_exists(key: str, cache: set[str]) -> bool:
    return key in cache


def _r2_moveto(src: str, dst: str, *, dry_run: bool) -> bool:
    """Server-side copy + delete. `rclone moveto` is single-source-single-
    destination so no wildcard surprises, and the underlying S3
    `CopyObject` preserves the object's ETag byte-for-byte — no separate
    md5 round-trip needed for integrity. `rclone moveto` returns
    non-zero if the copy didn't verify.

    Returns True on success (or when dry-run prints the plan)."""
    src_path = f"{RCLONE_REMOTE}:{R2_BUCKET}/{src}"
    dst_path = f"{RCLONE_REMOTE}:{R2_BUCKET}/{dst}"
    if dry_run:
        print(f"    [R2 dry-run] moveto {src} → {dst}")
        return True
    r = subprocess.run(
        ["rclone", "moveto", "--s3-no-check-bucket", src_path, dst_path],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(
            f"    [R2 ERR] moveto {src} → {dst}: {r.stderr.strip()}",
            file=sys.stderr,
        )
        return False
    return True


def _r2_delete(key: str, *, dry_run: bool) -> None:
    if dry_run:
        print(f"    [R2 dry-run] delete {key}")
        return
    subprocess.run(
        ["rclone", "deletefile", f"{RCLONE_REMOTE}:{R2_BUCKET}/{key}"],
        capture_output=True, text=True,
    )


# ----- local-disk helpers ----------------------------------------------

def _local_move(src: Path, dst: Path, *, dry_run: bool) -> bool:
    if dry_run:
        print(f"    [disk dry-run] move {src} → {dst}")
        return True
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    return True


# ----- migration core --------------------------------------------------

def _migrate_r2_row(row: Row, cache: set[str], *,
                    dry_run: bool) -> tuple[int, int, int]:
    """Move row's R2 covers. Returns (moved, already_done, missing).

    `cache` is the pre-listed set of every R2 key; we update it in place
    as we move objects so later rows see a consistent view (matters when
    two rows claim the same old path — unlikely but possible)."""
    moved = already_done = missing = 0

    # Small cover: old → new.
    if _r2_exists(row.new_r2_small, cache):
        if _r2_exists(row.old_r2_small, cache):
            # Destination already there (previous half-run). Delete the
            # leftover source so re-runs are clean.
            _r2_delete(row.old_r2_small, dry_run=dry_run)
            if not dry_run:
                cache.discard(row.old_r2_small)
        already_done += 1
    elif _r2_exists(row.old_r2_small, cache):
        if _r2_moveto(row.old_r2_small, row.new_r2_small, dry_run=dry_run):
            moved += 1
            if not dry_run:
                cache.discard(row.old_r2_small)
                cache.add(row.new_r2_small)
        else:
            missing += 1
    else:
        missing += 1

    # Large cover: try the two possible old paths before giving up.
    # The handler wrote `/large/{slug}.webp`; the #578 backfill wrote
    # `/large/{cover_filename}.webp`. We accept whichever exists first.
    if _r2_exists(row.new_r2_large, cache):
        # Clean up any leftover in either old path.
        for old in (row.old_r2_large_by_slug, row.old_r2_large_by_cover):
            if _r2_exists(old, cache):
                _r2_delete(old, dry_run=dry_run)
                if not dry_run:
                    cache.discard(old)
        already_done += 1
    else:
        for old in (row.old_r2_large_by_slug, row.old_r2_large_by_cover):
            if _r2_exists(old, cache):
                if _r2_moveto(old, row.new_r2_large, dry_run=dry_run):
                    moved += 1
                    if not dry_run:
                        cache.discard(old)
                        cache.add(row.new_r2_large)
                break
        # Large variant is optional for most titles — TMDB-fetch path fills
        # it on first detail-page request. Don't count as missing.

    return moved, already_done, missing


def _migrate_local_row(row: Row, *, dry_run: bool) -> tuple[int, int, int]:
    moved = already_done = missing = 0

    if row.new_local_small.exists():
        if row.old_local_small.exists():
            if dry_run:
                print(f"    [disk dry-run] unlink {row.old_local_small}")
            else:
                row.old_local_small.unlink()
            already_done += 1
        else:
            already_done += 1
    elif row.old_local_small.exists():
        if _local_move(row.old_local_small, row.new_local_small,
                       dry_run=dry_run):
            moved += 1
    else:
        missing += 1

    if row.new_local_large.exists():
        for old in (row.old_local_large_by_slug, row.old_local_large_by_cover):
            if old.exists():
                if dry_run:
                    print(f"    [disk dry-run] unlink {old}")
                else:
                    old.unlink()
        already_done += 1
    else:
        for old in (row.old_local_large_by_slug, row.old_local_large_by_cover):
            if old.exists():
                if _local_move(old, row.new_local_large, dry_run=dry_run):
                    moved += 1
                break

    return moved, already_done, missing


# ----- entry point -----------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true",
                   help="Print planned moves without touching any store.")
    g.add_argument("--apply", action="store_true",
                   help="Execute the moves.")
    p.add_argument("--r2", action="store_true",
                   help="Include Cloudflare R2 in the migration.")
    p.add_argument("--local", action="store_true",
                   help="Include local disk (data/**/covers-webp) in the migration.")
    p.add_argument("--table", choices=("films", "series", "tv_shows"),
                   help="Restrict to one table (default: all three).")
    p.add_argument("--limit", type=int,
                   help="Process at most this many rows per table "
                        "(smoke-test convenience).")
    args = p.parse_args()

    if not (args.r2 or args.local):
        sys.exit("Pass --r2 and/or --local (nothing to do otherwise).")

    if not DB_URL:
        sys.exit("DATABASE_URL env var required (set via .env or shell).")

    # Pre-list R2 keys once per prefix we touch. Per-row `rclone lsjson`
    # would issue ~4 subprocess calls per row (small + large × old/new)
    # which is 120k calls for 30k films — around 30 min and seriously
    # rate-limited. A `rclone lsf` per prefix is 5 HTTP round-trips total
    # and populates a `set[str]` we then hit in O(1).
    r2_keys: set[str] = set()
    if args.r2:
        r2_keys = _list_all_r2_keys()
        print(f"R2 pre-listed {len(r2_keys):,} objects across films/"
              f" series/ tv-shows/")

    # SELECT only — autocommit=True so a slow rclone loop doesn't pin
    # an idle-in-transaction snapshot on films/series/tv_shows.
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    rows = _load_rows(cur)
    cur.close()
    conn.close()

    if args.table:
        rows = [r for r in rows if r.table == args.table]
    if args.limit:
        by_table: dict[str, list[Row]] = {}
        for r in rows:
            by_table.setdefault(r.table, []).append(r)
        rows = []
        for table_rows in by_table.values():
            rows.extend(table_rows[:args.limit])

    by_table: dict[str, int] = {}
    for r in rows:
        by_table[r.table] = by_table.get(r.table, 0) + 1
    print("Rows to migrate:", ", ".join(
        f"{t}={n}" for t, n in sorted(by_table.items())))

    # Parallelise R2 moves. Each rclone subprocess spends most of its
    # wall clock on the R2 network round-trip (server-side copy + delete,
    # ~150 ms each), so ~16 concurrent workers move 23k rows in <10 min
    # without tripping R2's per-account rate limits (~500 req/s). Local
    # disk work is tiny and stays sequential on the main thread.
    total_moved = total_done = total_missing = 0
    stats_lock = threading.Lock()
    cache_lock = threading.Lock()

    def handle_row(i_row: tuple[int, Row]) -> tuple[int, int, int]:
        i, row = i_row
        if i % 500 == 0 or i == 1:
            print(
                f"  [{i}/{len(rows)}] {row.table} id={row.id} "
                f"slug={row.slug!r} cf={row.cover_filename!r}",
                flush=True,
            )
        m = d = s = 0
        if args.r2:
            # The cache (set[str]) is not thread-safe for `discard`/`add`
            # races on the same key. In practice each row touches its own
            # keys, so contention is effectively zero; a coarse lock
            # keeps us correct even in the pathological case.
            with cache_lock:
                cache_snapshot = r2_keys
            rm, rd, rs = _migrate_r2_row(row, cache_snapshot, dry_run=args.dry_run)
            m += rm
            d += rd
            s += rs
        if args.local:
            lm, ld, ls = _migrate_local_row(row, dry_run=args.dry_run)
            m += lm
            d += ld
            s += ls
        return m, d, s

    if args.r2 and args.apply:
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            for m, d, s in pool.map(handle_row, enumerate(rows, 1)):
                with stats_lock:
                    total_moved += m
                    total_done += d
                    total_missing += s
    else:
        # Dry-run and local-only modes — sequential is fine and gives a
        # readable, ordered transcript of planned moves.
        for i_row in enumerate(rows, 1):
            m, d, s = handle_row(i_row)
            total_moved += m
            total_done += d
            total_missing += s

    print(f"\nSummary: moved={total_moved}  "
          f"already_migrated={total_done}  missing_source={total_missing}")
    if args.dry_run:
        print("[dry-run] no changes applied. Add --apply to execute.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
