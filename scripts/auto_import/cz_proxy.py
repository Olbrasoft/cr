"""Shared CZ-proxy helper for SK Torrent requests.

SK Torrent blocks datacenter ASNs (incl. Hetzner where the VPS lives) — it
returns HTTP 200 with an empty body. To route around that we reuse the
CZ-hosted ASP.NET `Proxy.ashx` the main app already uses for prehraj.to.

When `CZ_PROXY_URL` + `CZ_PROXY_KEY` env vars are set, the scanner and
detail fetcher call `proxy_get()` instead of hitting SK Torrent directly.
The proxy forwards the request from a Czech residential IP and returns the
raw HTML body. Both modules still pass a `requests.Session` so connection
reuse keeps working (same session object hits the proxy base URL each
time).

See also `Proxy.ashx` on chobotnice.aspfree.cz — default action is
`HandleProxy`, which just forwards to the target URL and streams HTML back.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import urlencode

import requests

log = logging.getLogger(__name__)


def proxy_config() -> tuple[str, str] | None:
    """Return (base_url, key) if both env vars are set, else None."""
    url = os.environ.get("CZ_PROXY_URL", "").strip()
    key = os.environ.get("CZ_PROXY_KEY", "").strip()
    if not url or not key:
        return None
    return url, key


def proxy_get(
    target_url: str,
    session: requests.Session,
    timeout: int = 30,
) -> requests.Response:
    """GET `target_url` — direct when CZ proxy is not configured, otherwise via proxy.

    The proxy strips the original Set-Cookie / caching headers and just returns
    the response body as `text/html; charset=utf-8`, which is what the SK
    Torrent listing / detail parsers already expect.

    Caller still handles retries + HTTP status — this function returns the raw
    Response so existing `r.status_code` / `r.text` code keeps working.
    """
    cfg = proxy_config()
    if cfg is None:
        return session.get(target_url, timeout=timeout)

    base, key = cfg
    params = urlencode({"action": "proxy", "url": target_url, "key": key})
    proxy_url = f"{base}?{params}"
    return session.get(proxy_url, timeout=timeout)
