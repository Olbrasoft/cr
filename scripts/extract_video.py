#!/usr/bin/env python3
"""Extract video metadata and download URLs from Czech news sites.

Uses Playwright headless browser to capture video stream URLs from
Novinky.cz, Seznam Zprávy, and Stream.cz.

Usage:
    python3 extract_video.py <url>
    python3 extract_video.py --download <url> [--quality 480p] [--output /path/to/file.mp4]

Output (JSON):
    {
        "title": "Video title",
        "thumbnail": "https://...",
        "duration": 69.0,
        "uploader": "novinky.cz",
        "formats": [
            {"format_id": "480p", "resolution": "480p", "ext": "mp4", "url": "https://..."}
        ]
    }
"""
import json
import re
import sys
import os
import argparse
from urllib.parse import urlparse

CONSENT_COOKIE = {
    "name": "euconsent-v2",
    "value": "CPzqWAAPzqWAAAGABCCSC5CgAP_gAEPgACiQKZNB9G7WTXFneXp2YPskOYUX0VBJ4CUAAwgBwAIAIBoBKBECAAAAAKAAEIIAAAABBAAICIAAgBIBAAMBAgMNAEAMgAYCASgBIAKIEACEAAOECAAAJAgCBDAQIJCgBMATEACAAJAQEBBQBUCgAAAACAAAAAmAUYmAgAILAAiKAGAAQAAoACAAAABIAAAAAIgAAAAYAAAAYiAAAAAAAAAAAAAABAAAAAAAAAAAAgAAAAAQAAAIAAAAAAAIAAAAAAAAAAAAAAAAIAGAgAAAAABDQAEBAAIABgIAAAAAAAAAAAAAAAAAAAAAABAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAIAIAAAAAIAAAAYgAAAAAAAAAAAAAAEAAAAKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ",
    "path": "/",
}

SUPPORTED_DOMAINS = ["novinky.cz", "seznamzpravy.cz", "stream.cz"]
QUALITIES = ["144p", "240p", "360p", "480p", "720p", "1080p"]


def get_domain(url):
    host = urlparse(url).hostname or ""
    host = host.replace("www.", "")
    return host


def is_supported(url):
    domain = get_domain(url)
    return any(d in domain for d in SUPPORTED_DOMAINS)


def extract_video_info(url):
    """Extract video metadata and download URLs using Playwright."""
    from playwright.sync_api import sync_playwright

    domain = get_domain(url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        # Set consent cookies for the target domain
        context.add_cookies([
            {**CONSENT_COOKIE, "domain": f".{domain}"},
            {"name": "szncmpone", "value": "1", "domain": f".{domain}", "path": "/"},
        ])

        page = context.new_page()

        vmd_urls = []
        sec1_urls = []

        def handle_request(request):
            req_url = request.url
            if "vmd_ko_" in req_url or "vmd_ng_" in req_url:
                vmd_urls.append(req_url)
            elif "vmd/" in req_url and "SEC1" in req_url:
                sec1_urls.append(req_url)

        page.on("request", handle_request)

        page.goto(url, wait_until="networkidle", timeout=30000)

        # Accept consent if CMP frame appears
        for frame in page.frames:
            try:
                btn = frame.query_selector(
                    'button:has-text("Přijmout"), button:has-text("souhlasím")'
                )
                if btn:
                    btn.click()
                    page.wait_for_timeout(5000)
                    break
            except Exception:
                pass

        # Wait for video to auto-load
        page.wait_for_timeout(3000)

        # Scroll to trigger lazy-loaded content
        page.evaluate("window.scrollTo(0, 500)")
        page.wait_for_timeout(2000)

        # Extract title from page
        title = page.title() or ""
        # Clean up title (remove " | Novinky.cz" etc.)
        title = re.sub(r"\s*[|–—-]\s*(Novinky|Seznam Zprávy|Stream).*$", "", title)

        # Try to get thumbnail from JSON-LD
        thumbnail = None
        duration = None
        try:
            ld_json = page.evaluate("""() => {
                const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                for (const s of scripts) {
                    try {
                        const data = JSON.parse(s.textContent);
                        if (data.video) return data.video;
                        if (data['@type'] === 'VideoObject') return data;
                    } catch(e) {}
                }
                return null;
            }""")
            if ld_json:
                thumbnail = ld_json.get("thumbnailUrl") or ld_json.get("thumbnail", {}).get("url")
                if thumbnail and thumbnail.startswith("//"):
                    thumbnail = "https:" + thumbnail
                dur_str = ld_json.get("duration", "")
                if dur_str:
                    m = re.match(r"T?(\d+)S", dur_str)
                    if m:
                        duration = float(m.group(1))
                video_title = ld_json.get("name")
                if video_title:
                    title = video_title
        except Exception:
            pass

        browser.close()

    # Build download URLs from captured vmd URLs
    formats = []
    seen_ids = set()

    for vmd_url in vmd_urls:
        m = re.search(
            r"(https?://[^/]+)/([^/]+)/vmd_(ko|ng)_(\d+)/(\d+)\?fl=mdk,([a-f0-9]+)",
            vmd_url,
        )
        if not m:
            continue

        host = m.group(1)
        path_prefix = m.group(2)
        prefix = m.group(3)
        video_id = m.group(4)
        timestamp = m.group(5)
        hash_val = m.group(6)

        key = f"{prefix}_{video_id}_{timestamp}"
        if key in seen_ids:
            continue
        seen_ids.add(key)

        for quality in QUALITIES:
            mp4_url = (
                f"{host}/{path_prefix}/vd_{prefix}_{video_id}_{timestamp}"
                f"/h264_aac_{quality}_mp4/{hash_val}.mp4"
            )
            formats.append({
                "format_id": quality,
                "resolution": quality,
                "ext": "mp4",
                "url": mp4_url,
                "filesize_approx": None,
            })

        # Only use the first matching video
        break

    # If no vmd URLs found but we have SEC1 URLs (Stream.cz pattern)
    if not formats and sec1_urls:
        for sec1_url in sec1_urls:
            m = re.search(
                r"(https?://[^/]+/~SEC1~[^/]+)/([^/]+)/vmd/(\d+)\?fl=mdk,([a-f0-9]+)",
                sec1_url,
            )
            if m:
                sec1_base = m.group(1)
                path_prefix = m.group(2)
                # SEC1 URLs need special handling — skip for now
                break

    return {
        "title": title,
        "thumbnail": thumbnail,
        "duration": duration,
        "uploader": domain,
        "formats": formats,
    }


def download_video(url, quality="480p", output=None):
    """Extract and download a video."""
    info = extract_video_info(url)

    if not info["formats"]:
        print(json.dumps({"error": "No downloadable formats found"}), file=sys.stderr)
        sys.exit(1)

    # Find requested quality
    fmt = None
    for f in info["formats"]:
        if f["format_id"] == quality:
            fmt = f
            break

    if not fmt:
        # Fall back to best available
        fmt = info["formats"][-1]

    if not output:
        safe_title = re.sub(r'[^\w\s-]', '', info["title"])[:80].strip()
        output = f"{safe_title}.{fmt['ext']}"

    import requests

    resp = requests.get(fmt["url"], stream=True, timeout=120)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))

    with open(output, "wb") as f:
        downloaded = 0
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)

    info["downloaded_file"] = os.path.abspath(output)
    info["downloaded_size"] = downloaded
    info["selected_quality"] = fmt["format_id"]

    return info


def main():
    parser = argparse.ArgumentParser(description="Extract video from Czech news sites")
    parser.add_argument("url", help="URL of the article/video page")
    parser.add_argument("--download", action="store_true", help="Download the video")
    parser.add_argument("--quality", default="480p", help="Video quality (default: 480p)")
    parser.add_argument("--output", help="Output file path")
    args = parser.parse_args()

    if not is_supported(args.url):
        print(json.dumps({"error": f"Unsupported URL: {args.url}"}))
        sys.exit(1)

    if args.download:
        result = download_video(args.url, args.quality, args.output)
    else:
        result = extract_video_info(args.url)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
