#!/usr/bin/env python3
"""Download prehraj.to XML sitemap into a local cache directory (#646).

One-shot script invoked by `cr-prehrajto-sync.service` immediately before
`import-prehrajto-uploads.py`. Replaces the earlier bash wrapper —
keeping everything in Python lets the importer share the same
/var/cache layout and the systemd unit stays simple.

Behavior:
  - Fetches `https://prehraj.to/sitemap/index.xml`.
  - Lists all sub-sitemap URLs (host-agnostic match on
    `/sitemap/video-sitemap-N.xml`).
  - For each sub-sitemap, HEAD to read ETag.
  - Compares against last run's ETags persisted in
    `<sitemap-dir>/etags.json`.
  - In `--full` mode: downloads every sub-sitemap (a partial snapshot
    plus mark-dead would mis-flag rows). On any download failure, exits
    non-zero before the importer runs.
  - In `--incremental` mode: only fetches sub-sitemaps whose ETag has
    changed since last run. Best-effort — failures don't abort.

Both modes refresh `etags.json` so subsequent runs have the right
baseline.

Required env: nothing.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

INDEX_URL = "https://prehraj.to/sitemap/index.xml"
SUB_RE = re.compile(r"https?://[^<\s]*/sitemap/video-sitemap-\d+\.xml")
USER_AGENT = "ceskarepublika.wiki sitemap sync (https://github.com/Olbrasoft/cr)"


def http_get(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_head_etag(url: str, timeout: int = 30) -> str | None:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.headers.get("ETag")
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sitemap-dir", required=True,
                    help="Directory to download into (e.g. /var/cache/cr/prehrajto-sitemap)")
    ap.add_argument("--mode", choices=["full", "incremental"], required=True)
    ap.add_argument("--workers", type=int, default=8,
                    help="Parallel downloads (default 8)")
    ap.add_argument("--keep-days", type=int, default=2,
                    help="After a successful FULL run, delete sub-sitemap "
                         "files whose mtime is older than this many days. "
                         "Each sub-sitemap is ~32 MB and there are ~487 of "
                         "them (~15 GB), so we don't accumulate them on the "
                         "VPS. Set 0 to disable pruning.")
    args = ap.parse_args()

    sitemap_dir = Path(args.sitemap_dir)
    sitemap_dir.mkdir(parents=True, exist_ok=True)
    etag_file = sitemap_dir / "etags.json"

    # --- 1. Index ---
    print(f"[{args.mode}] fetching index.xml...", flush=True)
    try:
        index_bytes = http_get(INDEX_URL)
    except Exception as e:
        print(f"ERROR: failed to fetch {INDEX_URL}: {e}", file=sys.stderr)
        return 1
    # Natural-sort by the integer in the filename so video-sitemap-99 comes
    # before video-sitemap-100; lexicographic sort would put 99 last.
    _NUM_RE = re.compile(r"video-sitemap-(\d+)\.xml")

    def _key(u: str) -> int:
        m = _NUM_RE.search(u)
        return int(m.group(1)) if m else 0

    sub_urls = sorted(set(SUB_RE.findall(index_bytes.decode("utf-8", "replace"))), key=_key)
    if not sub_urls:
        print(f"ERROR: index.xml contained zero sub-sitemap URLs", file=sys.stderr)
        return 1
    print(f"  {len(sub_urls)} sub-sitemaps in index", flush=True)

    # --- 2. ETag baseline ---
    old_etags: dict[str, str] = {}
    if etag_file.exists():
        try:
            old_etags = json.loads(etag_file.read_text())
        except Exception:
            pass

    # HEAD all to capture current ETags (decides changed-set in incremental
    # mode, and refreshes baseline in full mode).
    print(f"[{args.mode}] HEAD {len(sub_urls)} sub-sitemaps...", flush=True)
    new_etags: dict[str, str] = {}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(http_head_etag, u): u for u in sub_urls}
        for f in as_completed(futs):
            etag = f.result()
            if etag:
                new_etags[futs[f]] = etag
    print(f"  {len(new_etags)} ETags collected in {time.time()-t0:.1f}s", flush=True)

    # --- 3. Decide which to download ---
    if args.mode == "full":
        to_download = sub_urls
        print(f"  full: downloading all {len(to_download)} sub-sitemaps", flush=True)
    else:
        to_download = []
        for u in sub_urls:
            new = new_etags.get(u)
            old = old_etags.get(u)
            if new is None or new != old:
                to_download.append(u)
        print(f"  incremental: {len(to_download)} of {len(sub_urls)} changed", flush=True)

    # --- 4. Parallel downloads ---
    failed: list[str] = []
    succeeded: set[str] = set()
    t1 = time.time()

    def fetch_one(url: str) -> tuple[str, bool]:
        target = sitemap_dir / Path(url).name
        tmp = target.with_suffix(target.suffix + ".tmp")
        try:
            data = http_get(url, timeout=300)
            tmp.write_bytes(data)
            tmp.replace(target)
            return url, True
        except Exception as e:
            print(f"WARN: {url} failed: {e}", file=sys.stderr)
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass
            return url, False

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for url, ok in ex.map(fetch_one, to_download):
            if ok:
                succeeded.add(url)
            else:
                failed.append(url)
    print(f"  downloaded {len(to_download) - len(failed)}/{len(to_download)} "
          f"in {time.time()-t1:.1f}s ({len(failed)} failures)", flush=True)

    # --- 5. Persist ETags ---
    # Only persist the new ETag for shards we successfully downloaded.
    # Shards we did NOT attempt to download (incremental: ETag unchanged)
    # also keep their refreshed ETag — they match the on-disk file by
    # definition. Shards whose download FAILED keep the OLD ETag (or no
    # entry) so the next run re-attempts them. Without this, a failed
    # download would persist its new ETag and be silently skipped forever.
    merged: dict[str, str] = dict(old_etags)
    untouched_set = set(sub_urls) - set(to_download)
    for url, etag in new_etags.items():
        if url in succeeded or url in untouched_set:
            merged[url] = etag
        # else: failure during this run — leave old_etags entry as-is
    etag_file.write_text(json.dumps(merged, indent=2, sort_keys=True))

    # --- 6. Fail-fast in full mode if any download failed ---
    if failed and args.mode == "full":
        print(f"ERROR: full mode requires complete snapshot; "
              f"{len(failed)} sub-sitemaps failed", file=sys.stderr)
        return 1

    # --- 7. Prune old sitemap files (full mode only) ---
    # Each sub-sitemap is ~32 MB; 487 of them is ~15 GB. Without pruning we'd
    # quickly eat the VPS disk. Full runs are atomic (we just downloaded the
    # complete current set), so anything older than --keep-days from previous
    # runs is safely removable. Keep the index.xml + etags.json (small,
    # needed for incremental).
    if args.mode == "full" and args.keep_days > 0:
        cutoff = time.time() - args.keep_days * 86400
        removed = 0
        freed_bytes = 0
        for path in sitemap_dir.glob("video-sitemap-*.xml"):
            try:
                if path.stat().st_mtime < cutoff:
                    freed_bytes += path.stat().st_size
                    path.unlink()
                    removed += 1
            except FileNotFoundError:
                pass
        if removed:
            print(f"  pruned {removed} stale sub-sitemaps "
                  f"(freed {freed_bytes / 1024 / 1024:.0f} MB)", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
