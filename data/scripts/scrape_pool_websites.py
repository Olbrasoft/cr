#!/usr/bin/env python3
"""Scrape introductory texts from pool official websites.

For each pool with a website, fetches the landing page and extracts
meaningful text content. Saves to cr_staging.pool_texts.
"""

import re
import time
import requests
import psycopg2
from bs4 import BeautifulSoup

STAGING_URL = "postgresql:///cr_staging"
UA = "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"
HEADERS = {"User-Agent": UA}


def extract_text(url):
    """Fetch URL and extract meaningful text content."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code}"

        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove script, style, nav, footer, header elements
        for tag in soup.select("script, style, nav, footer, header, noscript, iframe, form"):
            tag.decompose()

        # Try to find main content areas
        main = (
            soup.select_one("main")
            or soup.select_one("article")
            or soup.select_one(".content")
            or soup.select_one("#content")
            or soup.select_one(".main")
            or soup.select_one("#main")
            or soup.body
        )

        if not main:
            return None, "No content found"

        # Get text paragraphs
        paragraphs = []
        for p in main.find_all(["p", "div", "section"]):
            text = p.get_text(separator=" ", strip=True)
            # Filter out very short or navigation-like text
            if len(text) > 40 and not text.startswith("©") and "cookie" not in text.lower():
                paragraphs.append(text)

        if not paragraphs:
            # Fallback: get all text from main
            text = main.get_text(separator="\n", strip=True)
            lines = [l.strip() for l in text.split("\n") if len(l.strip()) > 40]
            paragraphs = lines[:10]

        if not paragraphs:
            return None, "No text paragraphs found"

        # Take first ~2000 chars of meaningful text
        result = "\n\n".join(paragraphs)
        if len(result) > 3000:
            result = result[:3000]

        return result, None

    except requests.exceptions.SSLError:
        # Try HTTP fallback
        if url.startswith("https://"):
            return extract_text(url.replace("https://", "http://"))
        return None, "SSL error"
    except requests.exceptions.ConnectionError as e:
        return None, f"Connection error: {str(e)[:100]}"
    except Exception as e:
        return None, str(e)[:200]


def main():
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    # Create table for scraped texts
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pool_texts (
            id SERIAL PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            source_url TEXT NOT NULL,
            raw_text TEXT,
            error TEXT,
            fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)
    conn.commit()

    # Get pools with websites, skip already scraped
    cur.execute("""
        SELECT p.slug, p.name, p.website
        FROM pools p
        WHERE p.website IS NOT NULL
          AND p.slug NOT IN (SELECT slug FROM pool_texts)
        ORDER BY p.slug
    """)
    pools = cur.fetchall()

    total = len(pools)
    print(f"Scraping {total} pool websites...", flush=True)

    scraped = 0
    failed = 0

    for i, (slug, name, website) in enumerate(pools):
        text, err = extract_text(website)

        cur.execute("""
            INSERT INTO pool_texts (slug, source_url, raw_text, error)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (slug) DO NOTHING
        """, (slug, website, text, err))

        if err:
            failed += 1
            if (i + 1) % 20 == 0 or "error" in (err or "").lower():
                print(f"  FAIL [{slug}]: {err}", flush=True)
        else:
            scraped += 1

        if (i + 1) % 20 == 0:
            conn.commit()
            print(f"  Progress: {i+1}/{total} (ok: {scraped}, fail: {failed})", flush=True)

        time.sleep(2)

    conn.commit()

    # Stats
    cur.execute("SELECT COUNT(*) FROM pool_texts WHERE raw_text IS NOT NULL AND LENGTH(raw_text) > 100")
    good = cur.fetchone()[0]
    print(f"\nDone! Scraped: {scraped}, Failed: {failed}, With good text: {good}", flush=True)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
