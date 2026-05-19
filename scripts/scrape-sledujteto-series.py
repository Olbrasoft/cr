#!/usr/bin/env python3
"""Fresh scrape of sledujteto.cz series episodes (#744).

Sledujteto.cz exposes no sitemap of uploads — only the search API at
`/api/web/videos?query=...&page=N&limit=30`. The API returns 30 files
per page, with `data.pages` showing how many pages a query has. To
cover the entire series-episode universe we enumerate broad
SxxE-prefix queries that catch every season's episodes, then dedupe by
`files.id` (the global numeric upload id).

Discovered query depths (sampled 2026-05-18):
  S01E → 521 pages (~15 600 files)   S04E → ?
  S02E → ?                           S05E →   3 pages
  S03E → ?                           S10E → 1 page

Queries we emit:
  - "S01E" .. "S15E"   — covers all SxxExx-tagged uploads
  - "1x"   .. "5x"     — covers the NxM short-form (less common)

For every returned file we keep the full JSON row verbatim so the
downstream importer (sub-issue #745 + #746) has all metadata. Output
schema matches the old `sledujteto-raw-2026-04-21.json` (dict keyed
by `slug_id` extracted from the URL `/file/<slug>/...`).

Run locally — sledujteto's API blocks Hetzner datacenter ASNs silently
(returns empty `files: []`). The prod web container falls back to the
aspone proxy for live search; the offline scraper has no such fallback
and must run from a residential IP.

Usage:
    python3 scripts/scrape-sledujteto-series.py [--out PATH] [--limit-pages N]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. pip install requests", file=sys.stderr)
    sys.exit(2)


API = "https://www.sledujteto.cz/api/web/videos"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

# `s\d+e\d+` (case insensitive) – the user's filter: URL must look like
# a series episode (sXXeXX where X is 0-9). Most sledujteto URLs are
# lowercase but search results sometimes use uppercase in the `name`
# field; we match both via re.IGNORECASE.
_EPISODE_RE = re.compile(r"s\d+e\d+", re.IGNORECASE)
# Optional: also accept "/file/<slug>/show-1x01-..." NxM style.
_NXM_RE = re.compile(r"\b\d{1,2}x\d{1,2}\b", re.IGNORECASE)

_SLUG_RE = re.compile(r"/file/([^/]+)/")


def fetch_page(sess: requests.Session, query: str, page: int) -> tuple[list[dict], int]:
    """Return (files, total_pages) for one query+page. Empty list + 0 on error."""
    url = f"{API}?query={quote(query)}&page={page}&limit=30&collection=suggestions&sort=newest&me=0"
    try:
        r = sess.get(url, timeout=20)
        r.raise_for_status()
        d = r.json().get("data", {})
        return d.get("files", []) or [], int(d.get("pages", 0))
    except Exception as e:  # noqa: BLE001
        print(f"  ! fetch error q={query!r} p={page}: {e}", file=sys.stderr)
        return [], 0


def crawl_query(sess: requests.Session, query: str, max_pages: int | None,
                seen: dict[str, dict], stats: dict[str, int], delay: float) -> None:
    page = 1
    files, total_pages = fetch_page(sess, query, page)
    if not files:
        print(f"  q={query!r}: empty result", file=sys.stderr)
        return
    if max_pages:
        total_pages = min(total_pages, max_pages)
    print(f"  q={query!r}: {total_pages} pages", file=sys.stderr)

    while True:
        new_in_page = 0
        episode_in_page = 0
        for f in files:
            stats["fetched"] += 1
            full_url = f.get("full_url") or f.get("link") or ""
            name = f.get("name") or f.get("filename") or ""
            # Filter: URL or name must contain sXXeXX (or NxM as fallback)
            haystack = f"{full_url} {name}"
            if not (_EPISODE_RE.search(haystack) or _NXM_RE.search(haystack)):
                stats["skipped_not_episode"] += 1
                continue
            episode_in_page += 1
            m = _SLUG_RE.search(full_url)
            if not m:
                stats["skipped_no_slug"] += 1
                continue
            slug = m.group(1)
            if slug in seen:
                stats["dedup"] += 1
                continue
            seen[slug] = f
            new_in_page += 1

        if page == 1 or page % 25 == 0 or page == total_pages:
            print(f"    page {page}/{total_pages}: +{new_in_page} new, "
                  f"{episode_in_page} eps in page, total unique={len(seen):,}",
                  file=sys.stderr)

        if page >= total_pages:
            break
        page += 1
        time.sleep(delay)
        files, _ = fetch_page(sess, query, page)
        if not files:
            print(f"    early stop at page {page} (empty)", file=sys.stderr)
            break


def main() -> int:
    ap = argparse.ArgumentParser()
    default_out = Path(__file__).resolve().parent.parent / "data" / "sledujteto" / \
        f"sledujteto-series-raw-{date.today().isoformat()}.json"
    ap.add_argument("--out", type=Path, default=default_out,
                    help=f"output JSON path (default: {default_out})")
    ap.add_argument("--limit-pages", type=int, default=None,
                    help="cap pages per query (default: no cap)")
    ap.add_argument("--delay", type=float, default=0.20,
                    help="seconds between requests (default 0.20 = 5 req/s)")
    ap.add_argument("--max-season", type=int, default=15,
                    help="enumerate S01E..S{max}E (default 15)")
    ap.add_argument("--include-nxm", action="store_true",
                    help="also crawl 1x..NxM short-form queries (default: off)")
    ap.add_argument("--nxm-max", type=int, default=5)
    ns = ap.parse_args()

    ns.out.parent.mkdir(parents=True, exist_ok=True)

    queries: list[str] = [f"S{n:02d}E" for n in range(1, ns.max_season + 1)]
    if ns.include_nxm:
        queries += [f"{n}x" for n in range(1, ns.nxm_max + 1)]

    sess = requests.Session()
    sess.headers["User-Agent"] = UA
    sess.headers["Accept"] = "application/json"

    seen: dict[str, dict] = {}
    stats: dict[str, int] = {"fetched": 0, "skipped_not_episode": 0,
                              "skipped_no_slug": 0, "dedup": 0}
    started = time.time()

    for q in queries:
        crawl_query(sess, q, ns.limit_pages, seen, stats, ns.delay)
        time.sleep(ns.delay)

    elapsed = time.time() - started
    print("\n=== summary ===", file=sys.stderr)
    print(f"  queries:               {len(queries)}", file=sys.stderr)
    print(f"  files fetched:         {stats['fetched']:,}", file=sys.stderr)
    print(f"  filtered out (non-ep): {stats['skipped_not_episode']:,}", file=sys.stderr)
    print(f"  filtered out (no slug):{stats['skipped_no_slug']:,}", file=sys.stderr)
    print(f"  dedup hits:            {stats['dedup']:,}", file=sys.stderr)
    print(f"  UNIQUE episodes saved: {len(seen):,}", file=sys.stderr)
    print(f"  elapsed:               {elapsed:.1f}s "
          f"({stats['fetched']/max(elapsed,1):.1f} req/s)", file=sys.stderr)

    ns.out.write_text(json.dumps(seen, ensure_ascii=False, indent=2))
    print(f"\nwrote → {ns.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
