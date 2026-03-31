#!/usr/bin/env python3
"""Download Czech cultural heritage items from Wikidata via SPARQL.

Fetches all items with P762 (Czech cultural heritage ID), including
cs.wikipedia URL, coordinates, and municipality label.
Stores results in cr_staging.wikidata_landmarks table.
"""

import os
import requests
import psycopg2

STAGING_URL = os.environ.get("STAGING_DATABASE_URL", "postgresql:///cr_staging")
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

SPARQL_QUERY = """
SELECT ?item ?itemLabel ?catalogId ?article ?lat ?lon ?municipalityLabel WHERE {
  ?item wdt:P762 ?catalogId .
  OPTIONAL {
    ?article schema:about ?item ;
             schema:isPartOf <https://cs.wikipedia.org/> .
  }
  OPTIONAL {
    ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat)
    BIND(geof:longitude(?coord) AS ?lon)
  }
  OPTIONAL {
    ?item wdt:P131 ?municipality .
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "cs,en" }
}
"""


def clean_uri(uri):
    """Remove < > brackets from URI."""
    if uri:
        return uri.strip("<>")
    return uri


def extract_wikidata_id(uri):
    """Extract Q-number from Wikidata URI."""
    uri = clean_uri(uri)
    if uri and "/entity/" in uri:
        return uri.split("/entity/")[-1]
    return None


def clean_label(label):
    """Remove @lang suffix from label."""
    if label and "@" in label:
        return label.rsplit("@", 1)[0]
    return label


def extract_wikipedia_url(url):
    """Return full cs.wikipedia URL from article URI."""
    url = clean_uri(url)
    if url and "cs.wikipedia.org" in url:
        return url
    return None


def main():
    print("Querying Wikidata SPARQL for items with P762...", flush=True)

    headers = {
        "Accept": "text/tab-separated-values",
        "User-Agent": "CeskaRepublikaWiki/1.0 (info@ceskarepublika.wiki)",
    }

    resp = requests.get(
        WIKIDATA_SPARQL,
        params={"query": SPARQL_QUERY},
        headers=headers,
        timeout=300,
    )
    resp.raise_for_status()

    import csv
    import io

    reader = csv.DictReader(io.StringIO(resp.text), delimiter="\t")
    rows = list(reader)
    print(f"Received {len(rows)} results from Wikidata", flush=True)

    # Deduplicate by wikidata_id (SPARQL can return duplicates due to multiple municipalities)
    items = {}
    for r in rows:
        wikidata_id = extract_wikidata_id(r.get("?item", ""))
        if not wikidata_id:
            continue

        if wikidata_id not in items:
            items[wikidata_id] = {
                "wikidata_id": wikidata_id,
                "label": clean_label(r.get("?itemLabel")),
                "catalog_id": r.get("?catalogId"),
                "wikipedia_url": extract_wikipedia_url(r.get("?article")),
                "latitude": None,
                "longitude": None,
                "municipality": clean_label(r.get("?municipalityLabel")),
            }

        # Fill in coordinates if available
        item = items[wikidata_id]
        if item["latitude"] is None:
            lat = r.get("?lat")
            lon = r.get("?lon")
            if lat and lon:
                try:
                    item["latitude"] = float(lat)
                    item["longitude"] = float(lon)
                except (ValueError, TypeError):
                    pass

    print(f"Deduplicated to {len(items)} unique items", flush=True)

    has_wiki = sum(1 for i in items.values() if i["wikipedia_url"])
    has_coords = sum(1 for i in items.values() if i["latitude"] is not None)
    print(f"  With cs.wikipedia article: {has_wiki}", flush=True)
    print(f"  With coordinates: {has_coords}", flush=True)

    # Store in database
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    cur.execute("TRUNCATE wikidata_landmarks")

    from psycopg2.extras import execute_batch

    execute_batch(
        cur,
        """INSERT INTO wikidata_landmarks
           (wikidata_id, label, catalog_id, wikipedia_url, latitude, longitude, municipality)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (wikidata_id) DO NOTHING""",
        [
            (
                i["wikidata_id"],
                i["label"],
                i["catalog_id"],
                i["wikipedia_url"],
                i["latitude"],
                i["longitude"],
                i["municipality"],
            )
            for i in items.values()
        ],
        page_size=1000,
    )

    conn.commit()
    print(f"\nStored {len(items)} items in wikidata_landmarks table", flush=True)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
