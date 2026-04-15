"""Fetch one SK Torrent video detail page and extract playback info.

The detail page (e.g. https://online.sktorrent.eu/video/59452/...) lists
every available quality as a `<source src="https://online{N}.sktorrent.eu/
media/videos//h264/{ID}_{QUALITY}.mp4" res="...">` tag. We pull:
  - cdn        : N from the first source URL (3 for online3, 5 for online5...)
  - qualities  : sorted list of quality labels ("720p", "480p", "1080p")
  - description: the og:description meta (fallback when SK Torrent's own block
                 is empty)

This module makes ONE HTTP request per call. Caller is responsible for
throttling between calls (1-2 s) to be polite to SK Torrent.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, asdict

import requests

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

log = logging.getLogger(__name__)


class DetailFetchError(RuntimeError):
    """Raised on network / non-200 HTTP errors."""


@dataclass
class VideoDetail:
    video_id: int
    cdn: int | None                  # online{N} server number (e.g. 3, 5)
    qualities: list[str]             # ["1080p","720p","480p"] sorted high→low
    description: str | None          # og:description text
    full_title: str | None           # og:title text

    def to_dict(self) -> dict:
        return asdict(self)


_SOURCE_RE = re.compile(
    r'<source\s+src="https://online(\d+)\.sktorrent\.eu/media/videos//h264/'
    r'(\d+)_(\d+p)\.mp4"',
    re.IGNORECASE,
)
_OG_DESC_RE = re.compile(
    r'<meta\s+property="og:description"\s+content="([^"]*)"',
    re.IGNORECASE,
)
_OG_TITLE_RE = re.compile(
    r'<meta\s+property="og:title"\s+content="([^"]*)"',
    re.IGNORECASE,
)


def _quality_sort_key(label: str) -> int:
    """Sort key: higher resolution number first ('1080p' > '720p' > '480p')."""
    m = re.match(r"(\d+)p", label.lower())
    return int(m.group(1)) if m else 0


def parse_detail_html(video_id: int, html: str) -> VideoDetail:
    """Extract VideoDetail from a SK Torrent detail page HTML."""
    cdn: int | None = None
    qualities: list[str] = []
    seen_quality: set[str] = set()

    for m in _SOURCE_RE.finditer(html):
        src_cdn = int(m.group(1))
        src_video_id = int(m.group(2))
        src_quality = m.group(3).lower()
        if src_video_id != video_id:
            # Ignore source tags pointing to other videos (related/embedded)
            continue
        if cdn is None:
            cdn = src_cdn
        if src_quality not in seen_quality:
            seen_quality.add(src_quality)
            qualities.append(src_quality)

    qualities.sort(key=_quality_sort_key, reverse=True)

    desc_m = _OG_DESC_RE.search(html)
    description = desc_m.group(1).strip() if desc_m else None
    if description == "":
        description = None

    title_m = _OG_TITLE_RE.search(html)
    full_title = title_m.group(1).strip() if title_m else None
    if full_title == "":
        full_title = None

    return VideoDetail(
        video_id=video_id,
        cdn=cdn,
        qualities=qualities,
        description=description,
        full_title=full_title,
    )


def fetch_detail(
    video_id: int,
    detail_url: str | None = None,
    session: requests.Session | None = None,
    timeout: int = 20,
) -> VideoDetail | None:
    """Fetch a single video detail page and parse it.

    Args:
        video_id: SK Torrent video id (must match the URL we fetch)
        detail_url: full URL; if None, builds canonical "/video/{id}/x"
        session: optional reusable requests.Session. Required headers
            (User-Agent, Accept-Encoding: identity) are set if missing.
        timeout: HTTP timeout seconds

    Returns:
        VideoDetail on success, None if the video was deleted (HTTP 404).

    Raises:
        DetailFetchError on network or non-200/404 response.
    """
    own_session = session is None
    if session is None:
        session = requests.Session()
    # Apply required headers to caller-provided sessions too — Accept-Encoding
    # MUST be identity to avoid SK Torrent's malformed-gzip bug.
    session.headers["Accept-Encoding"] = "identity"
    session.headers.setdefault("User-Agent", DEFAULT_USER_AGENT)
    if detail_url is None:
        detail_url = f"https://online.sktorrent.eu/video/{video_id}/x"

    try:
        try:
            r = session.get(detail_url, timeout=timeout)
        except requests.RequestException as e:
            raise DetailFetchError(f"detail {video_id} request failed: {e}") from e

        if r.status_code == 404:
            log.warning("detail %d returned HTTP 404 — video deleted", video_id)
            return None
        if r.status_code != 200:
            raise DetailFetchError(f"detail {video_id} returned HTTP {r.status_code}")

        detail = parse_detail_html(video_id, r.text)
        if not detail.qualities:
            log.warning("detail %d returned no playable sources", video_id)
        return detail
    finally:
        if own_session:
            session.close()


def _cli() -> None:
    import argparse, json
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("video_ids", nargs="+", type=int, help="SK Torrent video IDs to fetch")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    session = requests.Session()
    session.headers.update({
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept-Encoding": "identity",
    })
    for vid in args.video_ids:
        try:
            d = fetch_detail(vid, session=session)
            print(json.dumps(d.to_dict(), ensure_ascii=False, indent=2))
        except DetailFetchError as e:
            print(f"ERROR {vid}: {e}")


if __name__ == "__main__":
    _cli()
