#!/usr/bin/env python3
"""Download full Wikipedia article texts for all Czech municipalities.

Stores raw text in wikipedia_raw table for later LLM processing.
Uses MediaWiki API with explaintext=1 (plain text, no HTML).

Rate limit: ~1 request/second (Wikipedia API etiquette).
Supports resume — skips municipalities already in wikipedia_raw.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
import psycopg2

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://jirka@localhost/cr_dev"
)

USER_AGENT = "CeskaRepublikaWiki/1.0 (info@ceskarepublika.wiki)"
API_URL = "https://cs.wikipedia.org/w/api.php"


def get_wikipedia_extract(title: str) -> str | None:
    """Fetch full article text from Czech Wikipedia."""
    params = {
        "action": "query",
        "titles": title,
        "prop": "extracts",
        "explaintext": "1",
        "format": "json",
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            pages = data.get("query", {}).get("pages", {})
            for pid, page in pages.items():
                if pid == "-1":
                    return None
                return page.get("extract", "")
    except Exception as e:
        print(f"  ERROR fetching {title}: {e}", file=sys.stderr)
        return None


def title_from_url(wiki_url: str) -> str:
    """Extract Wikipedia article title from URL."""
    # https://cs.wikipedia.org/wiki/Bene%C5%A1ov -> Benešov
    prefix = "https://cs.wikipedia.org/wiki/"
    if wiki_url.startswith(prefix):
        return urllib.parse.unquote(wiki_url[len(prefix):])
    return ""


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Get all municipalities with Wikipedia URLs
    cur.execute("""
        SELECT m.municipality_code, m.wikipedia_url
        FROM municipalities m
        WHERE m.wikipedia_url IS NOT NULL
        AND m.municipality_code NOT IN (SELECT municipality_code FROM wikipedia_raw)
        ORDER BY m.municipality_code
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Municipalities to fetch: {total}")

    fetched = 0
    errors = 0

    for i, (code, wiki_url) in enumerate(rows):
        title = title_from_url(wiki_url)
        if not title:
            errors += 1
            continue

        extract = get_wikipedia_extract(title)
        if extract is None:
            errors += 1
            if (i + 1) % 50 == 0:
                print(f"  Progress: {i+1}/{total} (fetched: {fetched}, errors: {errors})")
            time.sleep(0.5)
            continue

        cur.execute(
            "INSERT INTO wikipedia_raw (municipality_code, title, extract) VALUES (%s, %s, %s)",
            (code, title, extract),
        )
        conn.commit()
        fetched += 1

        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{total} (fetched: {fetched}, errors: {errors})")

        # Rate limit: ~1 req/sec
        time.sleep(1.0)

    conn.close()
    print(f"\nDone! Fetched: {fetched}, Errors: {errors}, Total: {total}")


if __name__ == "__main__":
    main()
