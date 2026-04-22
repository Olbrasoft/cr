"""Download a TMDB poster, convert to WebP at our two display sizes.

Output layout (id-keyed, matches R2 prefix written after sub-issue #576):
    `{out_dir}/{id}/cover.webp`        (200×300)
    `{out_dir}/{id}/cover-large.webp`  (780×1170)

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
# the cover. Aspect 0.4–1.0 accepts portrait, near-square, and square
# images while still rejecting wider landscape-like creatives.
_MIN_POSTER_SIDE = 100
_MIN_POSTER_ASPECT = 0.4
_MAX_POSTER_ASPECT = 1.0

log = logging.getLogger(__name__)


def _cover_paths(out_dir: Path, entity_id: int) -> tuple[Path, Path]:
    entity_dir = out_dir / str(entity_id)
    entity_dir.mkdir(parents=True, exist_ok=True)
    return entity_dir / "cover.webp", entity_dir / "cover-large.webp"


def download_sktorrent_thumb(
    sktorrent_video_id: int,
    entity_id: int,
    out_dir: Path,
    *,
    overwrite: bool = False,
) -> bool:
    """Fallback cover source — SK Torrent's listing thumbnail.

    Used when TMDB has no poster (obscure CZ titles frequently lack one).
    The thumbnail lives at a predictable URL: `/media/videos/tmb1/{id}/1.jpg`.
    It's small (≈200×300) so `cover-large.webp` is saved at native size —
    the detail page would just upscale it.
    """
    if Image is None:
        return False
    small_path, large_path = _cover_paths(out_dir, entity_id)
    if not overwrite and small_path.exists() and large_path.exists():
        return True

    url = f"https://online.sktorrent.eu/media/videos/tmb1/{sktorrent_video_id}/1.jpg"
    try:
        r = requests.get(url, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as e:
        log.warning("sktorrent thumb fetch failed for id=%d: %s",
                    entity_id, type(e).__name__)
        return False
    if r.status_code != 200 or len(r.content) < 500:
        log.warning("sktorrent thumb missing/empty for id=%d (HTTP %d)",
                    entity_id, r.status_code)
        return False

    try:
        img = Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception as e:
        log.warning("sktorrent thumb decode failed for id=%d: %s", entity_id, e)
        return False

    img.save(large_path, "WEBP", quality=85, method=6)

    small = img.copy()
    small.thumbnail((200, 300), Image.LANCZOS)
    small.save(small_path, "WEBP", quality=85, method=6)

    log.info("cover saved id=%d from SK Torrent thumbnail (TMDB had none)",
             entity_id)
    return True


def download_cover(
    poster_path: str,
    entity_id: int,
    out_dir: Path,
    *,
    overwrite: bool = False,
) -> bool:
    """Download TMDB poster, save as `{id}/cover.webp` (200×300) and
    `{id}/cover-large.webp` (780×1170).

    Args:
        poster_path: TMDB path like "/abc.jpg"
        entity_id:   target entity id (films.id / series.id / tv_shows.id)
        out_dir:     directory containing `{id}/` subdirs (created if missing)
        overwrite:   re-download even if files already exist

    Returns:
        True on success, False on failure.
    """
    if Image is None:
        log.error("Pillow not installed — cannot convert covers")
        return False
    if not poster_path:
        return False

    small_path, large_path = _cover_paths(out_dir, entity_id)
    if not overwrite and small_path.exists() and large_path.exists():
        log.debug("cover id=%d already exists — skip", entity_id)
        return True

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
        log.warning("cover fetch failed for id=%d: %s",
                    entity_id, type(e).__name__)
        return False
    if r.status_code != 200:
        log.warning("cover fetch HTTP %d for id=%d", r.status_code, entity_id)
        return False

    try:
        img = Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception as e:
        log.warning("cover decode failed for id=%d: %s", entity_id, e)
        return False

    # Sanity: reject absurdly tiny or wrong-aspect frames. The TMDB CDN
    # occasionally returns a 1×1 tracking GIF on internal 5xx; a successful
    # HTTP 200 isn't enough by itself.
    w, h = img.size
    if w < _MIN_POSTER_SIDE or h < _MIN_POSTER_SIDE:
        log.warning("cover too small for id=%d: %dx%d (min %d)",
                    entity_id, w, h, _MIN_POSTER_SIDE)
        return False
    aspect = w / h
    if not (_MIN_POSTER_ASPECT <= aspect <= _MAX_POSTER_ASPECT):
        log.warning("cover aspect out of range for id=%d: %dx%d (aspect %.2f)",
                    entity_id, w, h, aspect)
        return False

    # Large: 780×1170 (preserve aspect — TMDB poster is 2:3)
    large = img.copy()
    large.thumbnail((780, 1170), Image.LANCZOS)
    large.save(large_path, "WEBP", quality=85, method=6)

    # Small: 200×300
    small = img.copy()
    small.thumbnail((200, 300), Image.LANCZOS)
    small.save(small_path, "WEBP", quality=85, method=6)

    log.info("cover saved id=%d", entity_id)
    return True


def download_cover_from_url(
    image_url: str,
    entity_id: int,
    out_dir: Path,
    *,
    overwrite: bool = False,
) -> bool:
    """Download an arbitrary image URL (e.g. ČSFD og:image) and save as
    `{id}/cover.webp` + `{id}/cover-large.webp`. Used when the enricher
    has no TMDB poster_path but does have a direct image URL.
    """
    if Image is None:
        log.error("Pillow not installed — cannot convert covers")
        return False
    if not image_url:
        return False

    small_path, large_path = _cover_paths(out_dir, entity_id)
    if not overwrite and small_path.exists() and large_path.exists():
        return True

    try:
        r = requests.get(image_url, timeout=DEFAULT_TIMEOUT)
    except requests.RequestException as e:
        log.warning("cover fetch failed for id=%d: %s",
                    entity_id, type(e).__name__)
        return False
    if r.status_code != 200:
        log.warning("cover fetch HTTP %d for id=%d", r.status_code, entity_id)
        return False

    try:
        img = Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception as e:
        log.warning("cover decode failed for id=%d: %s", entity_id, e)
        return False

    large = img.copy()
    large.thumbnail((780, 1170), Image.LANCZOS)
    large.save(large_path, "WEBP", quality=85, method=6)

    small = img.copy()
    small.thumbnail((200, 300), Image.LANCZOS)
    small.save(small_path, "WEBP", quality=85, method=6)

    log.info("cover saved id=%d from URL", entity_id)
    return True
