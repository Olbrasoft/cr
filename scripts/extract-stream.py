#!/usr/bin/env python3
"""Extract direct stream URL from video hosting providers.

Usage:
    python3 extract-stream.py filemoon <code>
    python3 extract-stream.py streamtape <code>
    python3 extract-stream.py mixdrop <code>
    python3 extract-stream.py vidlink <tmdb_id>

Output: JSON { "provider": "...", "code": "...", "stream_url": "...", "format": "hls"|"mp4", "expires_in": N }
        or { "provider": "...", "code": "...", "error": "..." }
"""

import json
import sys
import time


def extract_filemoon(code: str, timeout_s: int = 30) -> dict:
    """Navigate bombuj.si filemoon wrapper → intercept master.m3u8."""
    from playwright.sync_api import sync_playwright

    url = f"https://www.bombuj.si/prehravace_final/filemoon.sx7.php?code={code}&version=12&v=&id=0&us=&tit="
    stream_url = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36")
        page = ctx.new_page()
        page.on("response", lambda r: _capture(r, "master.m3u8", result))
        result = {}

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
            deadline = time.time() + timeout_s
            while "url" not in result and time.time() < deadline:
                page.wait_for_timeout(500)
        except Exception as e:
            if "url" not in result:
                browser.close()
                return {"provider": "filemoon", "code": code, "error": str(e)}

        browser.close()

    if "url" in result:
        return {"provider": "filemoon", "code": code, "stream_url": result["url"], "format": "hls", "expires_in": 10800}
    return {"provider": "filemoon", "code": code, "error": "m3u8 not found within timeout"}


def extract_streamtape(code: str, timeout_s: int = 20) -> dict:
    """Navigate to streamtape embed → capture tapecontent.net CDN URL from network."""
    from playwright.sync_api import sync_playwright

    url = f"https://streamtape.com/e/{code}"
    result = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36")
        page = ctx.new_page()

        def on_response(response):
            # Capture the final CDN URL (tapecontent.net/radosgw/) after redirect — skip thumbs
            if "tapecontent.net/radosgw/" in response.url and response.status in (200, 206):
                result["cdn_url"] = response.url

        page.on("response", on_response)

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
            # Wait for video to start loading (triggers get_video → tapecontent redirect)
            deadline = time.time() + timeout_s
            while "cdn_url" not in result and time.time() < deadline:
                page.wait_for_timeout(500)

            # Check for "not found"
            if "cdn_url" not in result:
                not_found = page.evaluate("() => { const h1 = document.querySelector('h1'); return h1 && h1.textContent.includes('not found'); }")
                if not_found:
                    browser.close()
                    return {"provider": "streamtape", "code": code, "error": "Video not found on Streamtape"}

        except Exception as e:
            if "cdn_url" not in result:
                browser.close()
                return {"provider": "streamtape", "code": code, "error": str(e)}

        browser.close()

    if "cdn_url" in result:
        return {"provider": "streamtape", "code": code, "stream_url": result["cdn_url"], "format": "mp4", "expires_in": 3600}
    return {"provider": "streamtape", "code": code, "error": "tapecontent CDN URL not captured"}


def extract_mixdrop(code: str, timeout_s: int = 20) -> dict:
    """Navigate to mixdrop embed → read MDCore.wurl + cookies for CDN access."""
    from playwright.sync_api import sync_playwright

    url = f"https://mixdrop.ag/e/{code}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36")
        page = ctx.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
            page.wait_for_timeout(5000)  # MDCore needs time to initialize

            data = page.evaluate("""() => {
                const w = window.MDCore || {};
                if (w.wurl) return { mp4_url: 'https:' + w.wurl };
                if (document.body.textContent.includes("can't find")) return { error: 'Video not found on Mixdrop' };
                return { error: 'MDCore.wurl not set', keys: Object.keys(w).join(',') };
            }""")

            # Capture cookies for CDN access
            cookies = ctx.cookies()
            cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)

        except Exception as e:
            browser.close()
            return {"provider": "mixdrop", "code": code, "error": str(e)}

        browser.close()

    if "mp4_url" in data:
        return {"provider": "mixdrop", "code": code, "stream_url": data["mp4_url"],
                "format": "mp4", "expires_in": 14400, "cookies": cookie_header}
    return {"provider": "mixdrop", "code": code, "error": data.get("error", "unknown")}


def extract_vidlink(tmdb_id: str, timeout_s: int = 20) -> dict:
    """Navigate to vidlink.pro → intercept HLS m3u8 from network."""
    from playwright.sync_api import sync_playwright

    url = f"https://vidlink.pro/movie/{tmdb_id}"
    result = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36")
        page = ctx.new_page()
        page.on("response", lambda r: _capture(r, ".m3u8", result))

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
            deadline = time.time() + timeout_s
            while "url" not in result and time.time() < deadline:
                page.wait_for_timeout(500)
        except Exception as e:
            if "url" not in result:
                browser.close()
                return {"provider": "vidlink", "code": tmdb_id, "error": str(e)}

        browser.close()

    if "url" in result:
        return {"provider": "vidlink", "code": tmdb_id, "stream_url": result["url"], "format": "hls", "expires_in": 7200}
    return {"provider": "vidlink", "code": tmdb_id, "error": "m3u8 not found within timeout"}


def _capture(response, pattern: str, result: dict):
    """Capture first network response matching pattern."""
    if "url" not in result and pattern in response.url and response.status == 200:
        result["url"] = response.url


PROVIDERS = {
    "filemoon": extract_filemoon,
    "streamtape": extract_streamtape,
    "mixdrop": extract_mixdrop,
    "vidlink": extract_vidlink,
}

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(json.dumps({"error": f"Usage: extract-stream.py <{'|'.join(PROVIDERS)}> <code>"}))
        sys.exit(1)

    provider = sys.argv[1].strip().lower()
    code = sys.argv[2].strip()

    if provider not in PROVIDERS:
        print(json.dumps({"error": f"Unknown provider: {provider}. Use: {', '.join(PROVIDERS)}"}))
        sys.exit(1)

    if not code or len(code) < 4:
        print(json.dumps({"error": f"Invalid code: {code}"}))
        sys.exit(1)

    result = PROVIDERS[provider](code)
    print(json.dumps(result))
    sys.exit(0 if "stream_url" in result else 1)
