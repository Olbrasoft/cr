#!/usr/bin/env python3
"""Scrape SK Torrent /videos/tv-porady section into a staging table.

Fetches all listing pages, then detail pages for each video, and stores
everything in `sktorrent_tv_porady`. Resumable — skips already-scraped IDs.

Usage:
    # Set env vars (or run from VPS where .env has them)
    export DATABASE_URL=postgres://cr:xxx@localhost:5432/cr
    export CZ_PROXY_URL=http://chobotnice.aspfree.cz/Proxy.ashx
    export CZ_PROXY_KEY=cr-proxy-2026-chobotnice

    python3 scripts/scrape-tv-porady.py [--max-pages 47] [--verbose]
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass

import psycopg2
import psycopg2.extras
import requests

# Reuse CzProxy helper from auto_import
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scripts.auto_import.cz_proxy import proxy_get
from scripts.auto_import.sktorrent_detail import (
    fetch_detail,
    VideoDetail,
    DetailFetchError,
)

LISTING_URL = "https://online.sktorrent.eu/videos/tv-porady"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
PAGE_SLEEP_S = 1.5          # throttle between listing pages
DETAIL_SLEEP_S = 1.0        # throttle between detail fetches
MAX_PAGES_DEFAULT = 50      # should be enough for 47 pages

log = logging.getLogger(__name__)


# --- Listing parser (same DOM structure as /videos) ---

_WELL_SPLIT_RE = re.compile(r'<div\s+class="well\s+well-sm">', re.DOTALL)
_LINK_RE = re.compile(r'<a\s+href="(/video/(\d+)/[^"]+)"', re.DOTALL)
_IMG_TITLE_RE = re.compile(r'<img\s+[^>]*?title="([^"]+)"', re.DOTALL)
_IMG_SRC_RE = re.compile(r'<img\s+[^>]*?src="([^"]+)"', re.DOTALL)
_HD_RE = re.compile(r'<div\s+class="hd-text-icon">HD</div>')
_DURATION_RE = re.compile(r'<div\s+class="duration">\s*([^<]+?)\s*</div>', re.DOTALL)
_ADDED_RE = re.compile(r'<div\s+class="video-added">\s*([^<]+?)\s*</div>', re.DOTALL)


@dataclass
class ListingItem:
    video_id: int
    title: str
    url: str
    thumbnail_url: str
    duration_str: str | None = None
    added_text: str | None = None
    is_hd: bool = False


def _parse_listing_html(html: str) -> list[ListingItem]:
    out: list[ListingItem] = []
    seen: set[int] = set()
    chunks = _WELL_SPLIT_RE.split(html)[1:]
    for chunk in chunks:
        link_m = _LINK_RE.search(chunk)
        if not link_m:
            continue
        try:
            vid = int(link_m.group(2))
        except (TypeError, ValueError):
            continue
        if vid in seen:
            continue
        seen.add(vid)

        title_m = _IMG_TITLE_RE.search(chunk)
        thumb_m = _IMG_SRC_RE.search(chunk)
        dur_m = _DURATION_RE.search(chunk)
        added_m = _ADDED_RE.search(chunk)

        out.append(
            ListingItem(
                video_id=vid,
                title=(title_m.group(1) if title_m else "").strip(),
                url="https://online.sktorrent.eu" + link_m.group(1),
                thumbnail_url=(thumb_m.group(1) if thumb_m else "").strip(),
                duration_str=(dur_m.group(1).strip() if dur_m else None) or None,
                added_text=(added_m.group(1).strip() if added_m else None) or None,
                is_hd=bool(_HD_RE.search(chunk)),
            )
        )
    return out


def _fetch_listing_page(session: requests.Session, page: int) -> str:
    target = LISTING_URL if page <= 1 else f"{LISTING_URL}?page={page}"
    r = proxy_get(target, session, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"listing page {page} returned HTTP {r.status_code}")
    return r.text


# --- DB helpers ---

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sktorrent_tv_porady (
    sktorrent_video_id  INTEGER PRIMARY KEY,
    title               TEXT NOT NULL,
    url                 TEXT NOT NULL,
    thumbnail_url       TEXT,
    duration_str        TEXT,
    is_hd               BOOLEAN NOT NULL DEFAULT FALSE,
    cdn                 INTEGER,
    qualities           TEXT[],
    description         TEXT,
    full_title          TEXT,
    scraped_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    log.info("staging table sktorrent_tv_porady ensured")


def _get_existing_ids(conn) -> set[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT sktorrent_video_id FROM sktorrent_tv_porady")
        return {row[0] for row in cur.fetchall()}


def _insert_row(conn, item: ListingItem, detail: VideoDetail | None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sktorrent_tv_porady
                (sktorrent_video_id, title, url, thumbnail_url, duration_str,
                 is_hd, cdn, qualities, description, full_title)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sktorrent_video_id) DO NOTHING
            """,
            (
                item.video_id,
                item.title,
                item.url,
                item.thumbnail_url or None,
                item.duration_str,
                item.is_hd,
                detail.cdn if detail else None,
                detail.qualities if detail else None,
                detail.description if detail else None,
                detail.full_title if detail else None,
            ),
        )
    conn.commit()


# --- Main ---

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--max-pages", type=int, default=MAX_PAGES_DEFAULT,
                    help=f"Max listing pages to fetch (default {MAX_PAGES_DEFAULT})")
    ap.add_argument("--skip-details", action="store_true",
                    help="Only scrape listings, skip detail page fetches")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        log.error("DATABASE_URL not set")
        sys.exit(1)
    # Rewrite @db: to @127.0.0.1: for host-side scripts
    db_url = db_url.replace("@db:", "@127.0.0.1:")

    conn = psycopg2.connect(db_url)
    _ensure_table(conn)
    existing_ids = _get_existing_ids(conn)
    log.info("already scraped: %d videos", len(existing_ids))

    session = requests.Session()
    session.headers.update({
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept-Encoding": "identity",
    })

    # Phase 1: Scan all listing pages
    all_items: list[ListingItem] = []
    for page in range(1, args.max_pages + 1):
        log.info("fetching listing page %d...", page)
        try:
            html = _fetch_listing_page(session, page)
        except Exception as e:
            log.error("page %d failed: %s", page, e)
            break

        items = _parse_listing_html(html)
        if not items:
            log.info("page %d returned 0 items — end of listings", page)
            break

        new_on_page = [it for it in items if it.video_id not in existing_ids]
        all_items.extend(new_on_page)
        log.info("page %d: %d items (%d new)", page, len(items), len(new_on_page))

        if page < args.max_pages:
            time.sleep(PAGE_SLEEP_S)

    log.info("total new items to process: %d", len(all_items))

    if not all_items:
        log.info("nothing new to scrape")
        conn.close()
        session.close()
        return

    # Phase 2: Fetch details + insert
    inserted = 0
    failed = 0
    for i, item in enumerate(all_items, 1):
        log.info("[%d/%d] detail for #%d: %s", i, len(all_items), item.video_id,
                 item.title[:60])

        detail: VideoDetail | None = None
        if not args.skip_details:
            try:
                detail = fetch_detail(item.video_id, item.url, session=session)
            except DetailFetchError as e:
                log.warning("detail fetch failed for %d: %s", item.video_id, e)
                failed += 1
            if i < len(all_items):
                time.sleep(DETAIL_SLEEP_S)

        _insert_row(conn, item, detail)
        inserted += 1

    conn.close()
    session.close()

    log.info("done: %d inserted, %d detail-fetch failures", inserted, failed)
    print(f"\nScraping complete: {inserted} inserted, {failed} failures, "
          f"{len(existing_ids)} already existed")


if __name__ == "__main__":
    main()
