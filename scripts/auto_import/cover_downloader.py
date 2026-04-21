"""Download a TMDB poster, convert to WebP at our two display sizes.

Output: `data/movies/covers-webp/{slug}.webp` (200×300) and
`{slug}-large.webp` (780×1170 portrait, used by film_detail page).

Pure HTTP + Pillow. No DB. Idempotent — skips if both files already exist.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

import requests

try:
    from PIL import Image
except ImportError:  # pragma: no cover
    Image = None

TMDB_IMG_BASE = "https://image.tmdb.org/t/p"
DEFAULT_TIMEOUT = 30

# Integrity bounds for decoded posters. TMDB w780 is 2:3 portrait: a valid
# poster is ~780×1170, well above this floor. Anything smaller/weirder is
# almost certainly a placeholder, a content-encoding tripped halfway, or
# bytes from a different response — refuse to save it rather than corrupt
# the cover. Aspect 0.4–1.0 accepts slightly off-square crops without
# admitting square ad creatives.
_MIN_POSTER_SIDE = 100
_MIN_POSTER_ASPECT = 0.4
_MAX_POSTER_ASPECT = 1.0

log = logging.getLogger(__name__)


def download_sktorrent_thumb(
    sktorrent_video_id: int,
    slug: str,
    out_dir: Path,
    *,
    overwrite: bool = False,
) -> tuple[Path, Path] | None:
    """Fallback cover source — SK Torrent's listing thumbnail.

    Used when TMDB has no poster (obscure CZ titles frequently lack one).
    The thumbnail lives at a predictable URL: `/media/videos/tmb1/{id}/1.jpg`.
    It's small (≈200×300) so we skip the `-large` variant — the detail
    page would just upscale it.
    """
    if Image is None:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    # The Rust handler at GET /filmy-online/{slug}-large.webp looks in
    # `{covers_dir}/large/{slug}.webp`, not `{slug}-large.webp` in the base
    # dir. See films.rs films_cover_large().
    large_dir = out_dir / "large"
    large_dir.mkdir(parents=True, exist_ok=True)
    small_path = out_dir / f"{slug}.webp"
    large_path = large_dir / f"{slug}.webp"
    if not overwrite and small_path.exists() and large_path.exists():
        return small_path, large_path

    url = f"https://online.sktorrent.eu/media/videos/tmb1/{sktorrent_video_id}/1.jpg"
    try:
        r = requests.get(url, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as e:
        log.warning("sktorrent thumb fetch failed for %s: %s", slug, type(e).__name__)
        return None
    if r.status_code != 200 or len(r.content) < 500:
        log.warning("sktorrent thumb missing/empty for %s (HTTP %d)", slug, r.status_code)
        return None

    try:
        img = Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception as e:
        log.warning("sktorrent thumb decode failed for %s: %s", slug, e)
        return None

    # SKT thumbnail is already small (≈200×300 native), so for `-large` we
    # just save the native bitmap without forcing it through `.thumbnail()`
    # (which would have no effect). Detail page will upscale in CSS, but at
    # least the file exists — better than a white/404 placeholder.
    img.save(large_path, "WEBP", quality=85, method=6)

    small = img.copy()
    small.thumbnail((200, 300), Image.LANCZOS)
    small.save(small_path, "WEBP", quality=85, method=6)

    log.info("cover saved %s from SK Torrent thumbnail (TMDB had none)", slug)
    return small_path, large_path


def download_cover(
    poster_path: str,
    slug: str,
    out_dir: Path,
    *,
    overwrite: bool = False,
) -> tuple[Path, Path] | None:
    """Download TMDB poster, save as `{slug}.webp` (200×300) and `{slug}-large.webp` (780×1170).

    Args:
        poster_path: TMDB path like "/abc.jpg"
        slug: target filename stem (without extension)
        out_dir: directory to write into (created if missing)
        overwrite: re-download even if files already exist

    Returns:
        (small_path, large_path) on success, None on failure.
    """
    if Image is None:
        log.error("Pillow not installed — cannot convert covers")
        return None
    if not poster_path:
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    small_path = out_dir / f"{slug}.webp"
    large_path = out_dir / f"{slug}-large.webp"
    if not overwrite and small_path.exists() and large_path.exists():
        log.debug("cover %s already exists — skip", slug)
        return small_path, large_path

    # Fetch w780 from TMDB (best quality available without going to original).
    # Buffer the full response body into memory BEFORE handing to Pillow.
    # Previously we used `stream=True` + `Image.open(r.raw)`; under thread-
    # pool parallelism that has been observed to splice bytes across
    # responses — the exotic-cohort cover corruption in issue #574 matches
    # the symptom. A 780×1170 JPEG is ≤200 kB, so `.content` is cheap and
    # gives Pillow a fully-owned buffer that cannot be touched by any
    # other thread's response handle.
    url = f"{TMDB_IMG_BASE}/w780{poster_path}"
    try:
        r = requests.get(url, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as e:
        # Don't interpolate `e`: the URL contains no api_key here, but the
        # habit avoids a future slip where it might.
        log.warning("cover fetch failed for %s: %s", slug, type(e).__name__)
        return None
    if r.status_code != 200:
        log.warning("cover fetch HTTP %d for %s", r.status_code, slug)
        return None

    try:
        img = Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception as e:
        log.warning("cover decode failed for %s: %s", slug, e)
        return None

    # Sanity: reject absurdly tiny or wrong-aspect frames. The TMDB CDN
    # occasionally returns a 1×1 tracking GIF on internal 5xx; a successful
    # HTTP 200 isn't enough by itself.
    w, h = img.size
    if w < _MIN_POSTER_SIDE or h < _MIN_POSTER_SIDE:
        log.warning("cover too small for %s: %dx%d (min %d)", slug, w, h, _MIN_POSTER_SIDE)
        return None
    aspect = w / h
    if not (_MIN_POSTER_ASPECT <= aspect <= _MAX_POSTER_ASPECT):
        log.warning("cover aspect out of range for %s: %dx%d (aspect %.2f)",
                    slug, w, h, aspect)
        return None

    # Large: 780×1170 (preserve aspect — TMDB poster is 2:3)
    large = img.copy()
    large.thumbnail((780, 1170), Image.LANCZOS)
    large.save(large_path, "WEBP", quality=85, method=6)

    # Small: 200×300
    small = img.copy()
    small.thumbnail((200, 300), Image.LANCZOS)
    small.save(small_path, "WEBP", quality=85, method=6)

    log.info("cover saved %s + -large", slug)
    return small_path, large_path
