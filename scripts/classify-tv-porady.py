#!/usr/bin/env python3
"""Classify scraped TV pořady — extract show names, group episodes, match ČSFD.

Phase 1 (offline): Parse titles → extract show_name + season/episode
Phase 2 (ČSFD):    Search each unique show on ČSFD via Playwright
Phase 3 (persist):  Update staging table with classification

Usage:
    # Phase 1 only (no network, just parse + group):
    python3 scripts/classify-tv-porady.py --phase1-only

    # Full run (Phase 1 + ČSFD matching):
    python3 scripts/classify-tv-porady.py

    # Retry ČSFD for unmatched shows:
    python3 scripts/classify-tv-porady.py --retry-unmatched
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
import unicodedata
from collections import defaultdict

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


# --- Title parsing ---

def extract_show_info(title: str) -> tuple[str, int | None, int | None]:
    """Extract (show_name, season, episode) from SK Torrent title.

    Returns the show name with language/year tags removed, plus optional
    season and episode numbers.
    """
    t = title.strip()

    # Remove language tags: (CZ), (SK), (EN)
    t = re.sub(r'\s*\((?:CZ|SK|EN)\)\s*', ' ', t, flags=re.I)
    # Remove year tags: (2020), (2025)
    t = re.sub(r'\s*\(\d{4}\)\s*', ' ', t)
    t = t.strip()

    season = None
    episode = None
    show_name = t

    # Pattern 1: SxxExx — most common
    m = re.search(r'S(\d+)E(\d+)', t, re.I)
    if m:
        season = int(m.group(1))
        episode = int(m.group(2))
        # Everything before SxxExx is show name
        show_name = t[:m.start()].strip().rstrip('-').strip()
        if not show_name:
            show_name = t[m.end():].strip()
        return show_name, season, episode

    # Pattern 2: "N.epizoda SHOW_NAME" or "N. Epizoda SHOW_NAME"
    m = re.match(r'^(\d+)\.?\s*[Ee]piz[oó]d[ae]\s+(.+)', t)
    if m:
        episode = int(m.group(1))
        show_name = m.group(2).strip()
        return show_name, season, episode

    # Pattern 3: "SHOW_NAME N. Epizóda"
    m = re.match(r'^(.+?)\s+(\d+)\.?\s*[Ee]piz[oó]d[ae]?\s*$', t)
    if m:
        show_name = m.group(1).strip()
        episode = int(m.group(2))
        return show_name, season, episode

    # Pattern 4: "SHOW_NAME Epizode N"
    m = re.match(r'^(.+?)\s+[Ee]piz[oó]d[ae]\s+(\d+)', t)
    if m:
        show_name = m.group(1).strip()
        episode = int(m.group(2))
        return show_name, season, episode

    # Pattern 5: "SHOW_NAME (E01)" or "SHOW_NAME (E12)"
    m = re.match(r'^(.+?)\s*\(E(\d+)\)\s*$', t)
    if m:
        show_name = m.group(1).strip()
        episode = int(m.group(2))
        return show_name, season, episode

    # Pattern 6: "SHOW_NAME E01" at end (no parens)
    m = re.match(r'^(.+?)\s+E(\d+)\s*$', t)
    if m:
        show_name = m.group(1).strip()
        episode = int(m.group(2))
        return show_name, season, episode

    # Pattern 7: "SHOW_NAME N. díl" or "SHOW_NAME N díl"
    m = re.match(r'^(.+?)\s+(\d+)\.?\s*díl\s*$', t, re.I)
    if m:
        show_name = m.group(1).strip()
        episode = int(m.group(2))
        return show_name, season, episode

    # Pattern 8: "SHOW_NAME - Episode N"
    m = re.match(r'^(.+?)\s*-\s*[Ee]pisode\s+(\d+)', t)
    if m:
        show_name = m.group(1).strip()
        episode = int(m.group(2))
        return show_name, season, episode

    # Pattern 9: "SHOW_NAME N" where N is a trailing number (e.g. "Smějeme se s Petrem Rychlým 14")
    m = re.match(r'^(.+?)\s+(\d{1,3})\s*$', t)
    if m:
        candidate_name = m.group(1).strip()
        candidate_ep = int(m.group(2))
        # Only treat as episode if the name part has >5 chars (avoid false positives like years)
        if len(candidate_name) > 5 and candidate_ep < 200:
            show_name = candidate_name
            episode = candidate_ep
            return show_name, season, episode

    # Pattern 10: "SHOW_NAME - 2025xNN" (year x episode format)
    m = re.match(r'^(.+?)\s*-\s*\d{4}x(\d+)', t)
    if m:
        show_name = m.group(1).strip()
        episode = int(m.group(2))
        return show_name, season, episode

    # No episode pattern — standalone
    return show_name, None, None


def normalize_show_name(name: str) -> str:
    """Normalize show name for grouping: lowercase, strip accents, collapse whitespace."""
    # NFD decompose → strip combining marks → NFC
    s = unicodedata.normalize('NFD', name.lower())
    s = ''.join(c for c in s if not unicodedata.combining(c))
    # Remove punctuation except /
    s = re.sub(r'[^\w\s/]', '', s)
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# --- ČSFD search via Playwright ---

def search_csfd_playwright(show_name: str) -> dict | None:
    """Search ČSFD for a show name using Playwright MCP (Edge browser).

    Returns dict with csfd_id, csfd_url, canonical_name or None if not found.
    This function uses subprocess to call a helper that interacts with
    the browser's console.
    """
    # We'll use a simpler approach: fetch via the browser's fetch() API
    # since Edge is already logged in and passes bot protection
    import subprocess
    import json

    # URL-encode the search query
    from urllib.parse import quote
    search_url = f"https://www.csfd.cz/hledat/?q={quote(show_name)}"

    # Use Edge CDP to fetch the page (bypasses Anubis since Edge has cookies)
    # We'll extract search results from the page
    script = f"""
    const response = await fetch("{search_url}");
    const html = await response.text();

    // Extract film/series links from search results
    const matches = [...html.matchAll(/href="\\/film\\/(\\d+)-([^"]+)"/g)];
    const results = matches.slice(0, 5).map(m => ({{
        csfd_id: parseInt(m[1]),
        slug: m[2],
        url: '/film/' + m[1] + '-' + m[2]
    }}));

    // Try to get titles from nearby text
    for (const r of results) {{
        const idx = html.indexOf(r.url);
        if (idx > -1) {{
            const snippet = html.substring(idx, idx + 500);
            const titleMatch = snippet.match(/>([^<]+)<\\/a>/);
            if (titleMatch) r.title = titleMatch[1].trim();
        }}
    }}

    JSON.stringify(results);
    """

    return None  # Placeholder — will use direct approach below


def search_csfd_via_edge(show_name: str, port: int = 9226) -> list[dict]:
    """Use Edge CDP to search ČSFD (bypasses Anubis bot protection).

    Navigates a tab to the ČSFD search page and extracts results from the DOM.
    Edge is already running with user profile on the detected CDP port.
    """
    import json
    from urllib.parse import quote
    import websocket

    search_url = f"https://www.csfd.cz/hledat/?q={quote(show_name)}"

    try:
        import requests as req
        tabs = req.get(f"http://127.0.0.1:{port}/json", timeout=5).json()
        page_tabs = [t for t in tabs if t.get("type") == "page" and t.get("webSocketDebuggerUrl")]
        if not page_tabs:
            log.warning("No page tabs available on port %d", port)
            return []

        # Use the last page tab (least disruptive)
        ws_url = page_tabs[-1]["webSocketDebuggerUrl"]
        ws = websocket.create_connection(ws_url, timeout=30)

        # Navigate to ČSFD search
        ws.send(json.dumps({
            "id": 1,
            "method": "Page.navigate",
            "params": {"url": search_url}
        }))
        json.loads(ws.recv())  # consume navigate response

        # Wait for page load
        import time as _time
        _time.sleep(4)

        # Extract search results from DOM
        ws.send(json.dumps({
            "id": 2,
            "method": "Runtime.evaluate",
            "params": {
                "expression": """
                    (() => {
                        const links = document.querySelectorAll('a[href*="/film/"]');
                        const seen = new Set();
                        const results = [];
                        for (const a of links) {
                            const m = a.href.match(/\\/film\\/(\\d+)-/);
                            if (m && !seen.has(m[1])) {
                                seen.add(m[1]);
                                const title = a.textContent.trim();
                                if (title && title.length > 0) {
                                    results.push({
                                        csfd_id: parseInt(m[1]),
                                        title: title,
                                        url: a.getAttribute('href')
                                    });
                                }
                            }
                        }
                        return JSON.stringify(results.slice(0, 10));
                    })()
                """,
                "returnByValue": True,
            }
        }))
        result = json.loads(ws.recv())
        ws.close()

        val = result.get("result", {}).get("result", {})
        if val.get("type") == "string":
            data = json.loads(val["value"])
            return [r for r in data if r.get("title")]
        return []
    except Exception as e:
        log.warning("CDP search failed for '%s': %s", show_name, e)
        return []


# --- Main classification logic ---

def phase1_parse_and_group(conn) -> dict[str, list[dict]]:
    """Phase 1: Parse all titles, extract show names, group by normalized name.

    Returns: {normalized_name: [list of rows with parsed info]}
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT sktorrent_video_id, title, description
            FROM sktorrent_tv_porady
            ORDER BY title
        """)
        rows = cur.fetchall()

    groups: dict[str, list[dict]] = defaultdict(list)
    name_variants: dict[str, str] = {}  # normalized → best original name

    for row in rows:
        show_name, season, episode = extract_show_info(row["title"])
        normalized = normalize_show_name(show_name)

        # Track the "best" (most common, with diacritics) variant of the name
        if normalized not in name_variants or len(show_name) > len(name_variants[normalized]):
            name_variants[normalized] = show_name

        groups[normalized].append({
            "video_id": row["sktorrent_video_id"],
            "title": row["title"],
            "show_name": show_name,
            "season": season,
            "episode": episode,
            "description": row["description"],
        })

    # Update show_name to best variant for each group
    for norm_name, items in groups.items():
        best_name = name_variants[norm_name]
        for item in items:
            item["canonical_name"] = best_name

    return dict(groups)


def phase1_persist(conn, groups: dict[str, list[dict]]):
    """Save Phase 1 results (show_name, season, episode) to DB."""
    with conn.cursor() as cur:
        for norm_name, items in groups.items():
            for item in items:
                cur.execute("""
                    UPDATE sktorrent_tv_porady
                    SET show_name = %s, season_number = %s, episode_number = %s,
                        classified_at = now()
                    WHERE sktorrent_video_id = %s
                """, (
                    item["canonical_name"],
                    item["season"],
                    item["episode"],
                    item["video_id"],
                ))
    conn.commit()
    log.info("Phase 1: updated %d videos across %d shows",
             sum(len(v) for v in groups.values()), len(groups))


def phase2_csfd_match(conn, groups: dict[str, list[dict]], port: int = 9223):
    """Phase 2: Search ČSFD for each unique show name."""
    # Get shows that don't have csfd_id yet
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT show_name FROM sktorrent_tv_porady
            WHERE show_name IS NOT NULL AND csfd_id IS NULL
            ORDER BY show_name
        """)
        unmatched_shows = [row[0] for row in cur.fetchall()]

    log.info("Phase 2: %d unique shows to search on ČSFD", len(unmatched_shows))

    matched = 0
    not_found = 0

    for i, show_name in enumerate(unmatched_shows, 1):
        log.info("[%d/%d] Searching ČSFD for: %s", i, len(unmatched_shows), show_name)

        results = search_csfd_via_edge(show_name, port=port)
        time.sleep(1.5)  # Be polite to ČSFD

        if results:
            # Pick best match — prefer exact title match
            best = None
            show_lower = show_name.lower()
            for r in results:
                if r.get("title", "").lower() == show_lower:
                    best = r
                    break
            if not best:
                best = results[0]  # First result as fallback

            csfd_id = best["csfd_id"]
            csfd_url = f"https://www.csfd.cz{best['url']}"
            csfd_title = best.get("title", "")

            log.info("  ✓ Found: csfd_id=%d, title='%s', url=%s",
                     csfd_id, csfd_title, csfd_url)

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE sktorrent_tv_porady
                    SET csfd_id = %s, csfd_url = %s
                    WHERE show_name = %s
                """, (csfd_id, csfd_url, show_name))
            conn.commit()
            matched += 1
        else:
            log.info("  ✗ Not found on ČSFD")
            not_found += 1

    log.info("Phase 2 complete: %d matched, %d not found", matched, not_found)


def print_report(conn):
    """Print classification report."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(show_name) as classified,
                COUNT(CASE WHEN episode_number IS NOT NULL THEN 1 END) as with_episode,
                COUNT(CASE WHEN season_number IS NOT NULL THEN 1 END) as with_season,
                COUNT(CASE WHEN episode_number IS NULL AND season_number IS NULL THEN 1 END) as standalone,
                COUNT(csfd_id) as with_csfd,
                COUNT(DISTINCT show_name) as unique_shows,
                COUNT(DISTINCT csfd_id) FILTER (WHERE csfd_id IS NOT NULL) as unique_csfd
            FROM sktorrent_tv_porady
        """)
        stats = cur.fetchone()

    print("\n" + "=" * 60)
    print("TV POŘADY CLASSIFICATION REPORT")
    print("=" * 60)
    print(f"Total videos:          {stats['total']}")
    print(f"Classified (has name): {stats['classified']}")
    print(f"  With episode number: {stats['with_episode']}")
    print(f"  With season number:  {stats['with_season']}")
    print(f"  Standalone:          {stats['standalone']}")
    print(f"Unique shows:          {stats['unique_shows']}")
    print(f"ČSFD matched:          {stats['with_csfd']} videos ({stats['unique_csfd']} shows)")
    print(f"ČSFD unmatched:        {stats['total'] - stats['with_csfd']} videos")
    print("=" * 60)

    # Top shows by episode count
    with conn.cursor() as cur:
        cur.execute("""
            SELECT show_name, csfd_id, COUNT(*) as cnt
            FROM sktorrent_tv_porady
            WHERE show_name IS NOT NULL
            GROUP BY show_name, csfd_id
            ORDER BY cnt DESC
            LIMIT 20
        """)
        shows = cur.fetchall()

    print("\nTop 20 shows:")
    for name, csfd_id, cnt in shows:
        csfd_str = f"csfd={csfd_id}" if csfd_id else "no-csfd"
        print(f"  {cnt:4d} ep  {name[:45]:45s}  {csfd_str}")

    # Unmatched shows (no ČSFD)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT show_name, COUNT(*) as cnt
            FROM sktorrent_tv_porady
            WHERE show_name IS NOT NULL AND csfd_id IS NULL
            GROUP BY show_name
            ORDER BY cnt DESC
        """)
        unmatched = cur.fetchall()

    if unmatched:
        print(f"\nUnmatched shows ({len(unmatched)}):")
        for name, cnt in unmatched:
            print(f"  {cnt:4d} ep  {name}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phase1-only", action="store_true",
                    help="Only parse titles, skip ČSFD matching")
    ap.add_argument("--retry-unmatched", action="store_true",
                    help="Only retry ČSFD for shows without csfd_id")
    ap.add_argument("--cdp-port", type=int, default=9223,
                    help="Edge CDP port (default 9223)")
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
    db_url = db_url.replace("@db:", "@127.0.0.1:")

    conn = psycopg2.connect(db_url)

    # Phase 1: Parse and group
    if not args.retry_unmatched:
        log.info("=== Phase 1: Parse titles and group episodes ===")
        groups = phase1_parse_and_group(conn)
        phase1_persist(conn, groups)

        # Quick stats
        total_eps = sum(len(v) for v in groups.values())
        with_ep = sum(1 for v in groups.values() for i in v if i["episode"] is not None)
        log.info("Parsed %d videos → %d shows (%d with episode numbers, %d standalone)",
                 total_eps, len(groups), with_ep, total_eps - with_ep)

    if args.phase1_only:
        print_report(conn)
        conn.close()
        return

    # Phase 2: ČSFD matching
    log.info("=== Phase 2: ČSFD matching via Edge CDP ===")
    groups = phase1_parse_and_group(conn) if args.retry_unmatched else groups
    phase2_csfd_match(conn, groups, port=args.cdp_port)

    print_report(conn)
    conn.close()


if __name__ == "__main__":
    main()
