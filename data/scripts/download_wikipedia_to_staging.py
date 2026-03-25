#!/usr/bin/env python3
"""Download Wikipedia article texts into cr_staging database.

Staging DB (cr_staging) is a permanent store for source data.
It is NEVER dropped or reset during development.

Supports resume — skips already-fetched entities.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
import psycopg2

STAGING_URL = os.environ.get("STAGING_DATABASE_URL", "postgres://jirka@localhost/cr_staging")
DEV_URL = os.environ.get("DATABASE_URL", "postgres://jirka@localhost/cr_dev")
USER_AGENT = "CeskaRepublikaWiki/1.0 (info@ceskarepublika.wiki)"
API_URL = "https://cs.wikipedia.org/w/api.php"


def get_wikipedia_extract(title: str) -> str | None:
    params = {
        "action": "query", "titles": title,
        "prop": "extracts", "explaintext": "1", "format": "json",
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
        print(f"  ERROR: {e}", file=sys.stderr)
        return None


def main():
    dev = psycopg2.connect(DEV_URL)
    staging = psycopg2.connect(STAGING_URL)
    dev_cur = dev.cursor()
    stg_cur = staging.cursor()

    # Get municipalities that need Wikipedia text
    dev_cur.execute("""
        SELECT municipality_code, wikipedia_url
        FROM municipalities
        WHERE wikipedia_url IS NOT NULL
        ORDER BY municipality_code
    """)
    all_munis = dev_cur.fetchall()

    # Get already-fetched codes from staging
    stg_cur.execute("SELECT entity_code FROM wikipedia_raw WHERE entity_type = 'municipality'")
    done_codes = {row[0] for row in stg_cur.fetchall()}

    remaining = [(c, u) for c, u in all_munis if c not in done_codes]
    print(f"Total municipalities: {len(all_munis)}")
    print(f"Already fetched: {len(done_codes)}")
    print(f"Remaining: {len(remaining)}")

    fetched = 0
    errors = 0

    for i, (code, wiki_url) in enumerate(remaining):
        prefix = "https://cs.wikipedia.org/wiki/"
        title = urllib.parse.unquote(wiki_url[len(prefix):]) if wiki_url.startswith(prefix) else ""
        if not title:
            errors += 1
            continue

        extract = get_wikipedia_extract(title)
        if extract is None:
            errors += 1
            time.sleep(0.5)
            continue

        stg_cur.execute(
            "INSERT INTO wikipedia_raw (entity_type, entity_code, title, extract) VALUES ('municipality', %s, %s, %s) ON CONFLICT DO NOTHING",
            (code, title, extract)
        )
        staging.commit()
        fetched += 1

        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(remaining)} (fetched: {fetched}, errors: {errors})")

        time.sleep(1.0)

    dev.close()
    staging.close()
    print(f"\nDone! Fetched: {fetched}, Errors: {errors}")


if __name__ == "__main__":
    main()
