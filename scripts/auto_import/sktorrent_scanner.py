"""SK Torrent listing scanner.

Fetches https://online.sktorrent.eu/videos page-by-page in newest-first order,
parses each item, and stops once it reaches a video_id ≤ checkpoint. Returns
the list of new items in ASCENDING order so downstream processing handles the
oldest-new video first (safer for batch grouping of series episodes).

Pure HTTP scraping — no DB writes, no TMDB calls, no Gemma. The output is a
plain list of `ScannedVideo` dataclasses ready for the enricher pipeline.

Usage as a module:
    from scripts.auto_import.sktorrent_scanner import scan_new_videos
    new = scan_new_videos(checkpoint=59313, max_new=5)

CLI for ad-hoc use:
    python3 -m scripts.auto_import.sktorrent_scanner --checkpoint 59313 --max-new 5
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass, asdict

import requests

LISTING_URL = "https://online.sktorrent.eu/videos"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
PAGE_SLEEP_S = 1.0          # throttle between consecutive page fetches
MAX_PAGES_HARD_CAP = 50     # safety net — never crawl past this many pages

log = logging.getLogger(__name__)


class ScannerError(RuntimeError):
    """Raised when the scanner cannot make progress (e.g. SK Torrent is down)."""


@dataclass
class ScannedVideo:
    """One row from the SK Torrent listing — minimal data only.

    Detail page fetching and TMDB resolution happen in later pipeline steps.
    """

    video_id: int
    title: str                          # e.g. "Pomocnice / The Housemaid (2025)(CZ)"
    url: str                            # full URL of detail page
    thumbnail_url: str                  # small thumb on listing
    duration_str: str | None = None     # "02:06:10"
    added_text: str | None = None       # raw "6 hodinami před" / "1 dnem před"
    is_hd: bool = False                 # listing badge

    def to_dict(self) -> dict:
        return asdict(self)


# Listing items are wrapped in `<div class="well well-sm">`. We split the HTML
# on those wrappers, then extract each field with a small focused regex so the
# parser stays robust against unrelated layout drift.
_WELL_SPLIT_RE = re.compile(r'<div\s+class="well\s+well-sm">', re.DOTALL)
_LINK_RE = re.compile(r'<a\s+href="(/video/(\d+)/[^"]+)"', re.DOTALL)
_IMG_TITLE_RE = re.compile(r'<img\s+[^>]*?title="([^"]+)"', re.DOTALL)
_IMG_SRC_RE = re.compile(r'<img\s+[^>]*?src="([^"]+)"', re.DOTALL)
_HD_RE = re.compile(r'<div\s+class="hd-text-icon">HD</div>')
_DURATION_RE = re.compile(r'<div\s+class="duration">\s*([^<]+?)\s*</div>', re.DOTALL)
_ADDED_RE = re.compile(r'<div\s+class="video-added">\s*([^<]+?)\s*</div>', re.DOTALL)


def _parse_listing_html(html: str) -> list[ScannedVideo]:
    """Extract all ScannedVideo entries from a listing page.

    Order in the returned list matches DOM order (newest first on page 1)."""
    out: list[ScannedVideo] = []
    seen: set[int] = set()
    # First chunk before the first `well well-sm` is the page chrome — discard.
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
            continue  # defensive — listings don't repeat IDs
        seen.add(vid)

        title_m = _IMG_TITLE_RE.search(chunk)
        thumb_m = _IMG_SRC_RE.search(chunk)
        dur_m = _DURATION_RE.search(chunk)
        added_m = _ADDED_RE.search(chunk)

        out.append(
            ScannedVideo(
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


def _fetch_page(session: requests.Session, page: int, timeout: int = 20) -> str:
    """GET listing page N. Returns raw HTML; raises ScannerError on failure."""
    params = {"page": page} if page > 1 else None
    try:
        r = session.get(LISTING_URL, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise ScannerError(f"page {page} request failed: {e}") from e
    if r.status_code != 200:
        raise ScannerError(f"page {page} returned HTTP {r.status_code}")
    return r.text


def scan_new_videos(
    checkpoint: int,
    max_new: int = 5,
    max_pages: int = MAX_PAGES_HARD_CAP,
    session: requests.Session | None = None,
    sleep_s: float = PAGE_SLEEP_S,
) -> list[ScannedVideo]:
    """Crawl pages from newest until checkpoint, return new videos ASC.

    Args:
        checkpoint: highest already-known sktorrent_video_id; videos ≤ this
            are considered "already imported" and end the scan.
        max_new: hard cap on returned new videos. Defaults to 5 so ad-hoc
            invocations don't accidentally crawl a huge batch. Pass 0 for
            unlimited (used by the daily cron job once stable).
        max_pages: defensive crawl ceiling. Clamped to MAX_PAGES_HARD_CAP
            (50) so a buggy caller can't accidentally hammer SK Torrent.
        session: optional reusable requests.Session. Required headers
            (User-Agent, Accept-Encoding: identity) are set if missing,
            so even caller-provided sessions get the malformed-gzip fix.
        sleep_s: throttle between page fetches.

    Returns:
        New videos sorted by id ASC (oldest-new first).

    Raises:
        ScannerError: SK Torrent unreachable or returns non-200.
    """
    if max_pages > MAX_PAGES_HARD_CAP:
        log.warning("max_pages=%d clamped to hard cap %d", max_pages, MAX_PAGES_HARD_CAP)
        max_pages = MAX_PAGES_HARD_CAP

    own_session = session is None
    if session is None:
        session = requests.Session()
    # `Accept-Encoding: identity` is mandatory for SK Torrent — the server
    # occasionally returns malformed gzip otherwise. Force-set even on
    # caller-provided sessions because `requests.Session()` defaults to
    # `gzip, deflate` which triggers the bug. User-Agent only when missing
    # so callers can override.
    session.headers["Accept-Encoding"] = "identity"
    session.headers.setdefault("User-Agent", DEFAULT_USER_AGENT)

    new_videos: list[ScannedVideo] = []
    pages_scanned = 0
    reached_checkpoint = False

    try:
        for page in range(1, max_pages + 1):
            if page > 1:
                time.sleep(sleep_s)
            html = _fetch_page(session, page)
            pages_scanned += 1
            items = _parse_listing_html(html)
            if not items:
                log.warning("page %d returned 0 items — stopping", page)
                break

            page_has_known = False
            for item in items:
                if item.video_id <= checkpoint:
                    page_has_known = True
                    continue
                new_videos.append(item)

            if page_has_known:
                reached_checkpoint = True
                break  # everything older than this page is also ≤ checkpoint
        else:
            log.warning(
                "scanned %d pages without hitting checkpoint=%d — stopping",
                max_pages, checkpoint,
            )
    finally:
        if own_session:
            session.close()

    # Dedupe (defensive — a video shouldn't appear on two consecutive pages)
    # and sort by id ASC so the enricher processes oldest-new first.
    seen_ids: set[int] = set()
    deduped: list[ScannedVideo] = []
    for v in new_videos:
        if v.video_id in seen_ids:
            continue
        seen_ids.add(v.video_id)
        deduped.append(v)
    deduped.sort(key=lambda v: v.video_id)

    if max_new and len(deduped) > max_new:
        deduped = deduped[:max_new]

    log.info(
        "scanned %d pages, found %d new videos (checkpoint=%d, reached=%s, capped=%s)",
        pages_scanned, len(deduped), checkpoint, reached_checkpoint,
        bool(max_new),
    )
    return deduped


def _cli() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--checkpoint", type=int, required=True,
                    help="Highest already-known sktorrent_video_id")
    ap.add_argument("--max-new", type=int, default=5,
                    help="Hard cap on returned new videos (default 5; 0 = unlimited)")
    ap.add_argument("--max-pages", type=int, default=MAX_PAGES_HARD_CAP,
                    help=f"Defensive crawl ceiling (default {MAX_PAGES_HARD_CAP})")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    new = scan_new_videos(
        checkpoint=args.checkpoint,
        max_new=args.max_new,
        max_pages=args.max_pages,
    )
    print(f"Found {len(new)} new videos:")
    for v in new:
        print(f"  {v.video_id:6d}  {v.title[:70]:70}  ({v.duration_str or '?'}, {v.added_text or '?'})")


if __name__ == "__main__":
    _cli()
