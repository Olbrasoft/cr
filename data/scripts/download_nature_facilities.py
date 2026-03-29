#!/usr/bin/env python3
"""Download zoos, botanical gardens, aquariums from Wikidata + Wikipedia texts.

Saves to cr_staging.nature_facilities table.
"""

import time
import requests
import psycopg2

STAGING_URL = "postgresql:///cr_staging"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API = "https://cs.wikipedia.org/w/api.php"

UA = "CeskaRepublikaWiki/1.0 (contact@ceskarepublika.wiki)"
HEADERS = {"User-Agent": UA}

# SPARQL query for zoos + botanical gardens + aquariums in Czech Republic
SPARQL_QUERY = """
SELECT DISTINCT ?item ?itemLabel ?typeLabel ?coords ?website ?image ?inception ?area ?visitors ?municipalityLabel ?wikipedia
WHERE {
  VALUES ?type { wd:Q43501 wd:Q167346 wd:Q862067 wd:Q2281788 }
  ?item wdt:P31/wdt:P279* ?type .
  ?item wdt:P17 wd:Q213 .
  OPTIONAL { ?item wdt:P625 ?coords }
  OPTIONAL { ?item wdt:P856 ?website }
  OPTIONAL { ?item wdt:P18 ?image }
  OPTIONAL { ?item wdt:P571 ?inception }
  OPTIONAL { ?item wdt:P2046 ?area }
  OPTIONAL { ?item wdt:P1174 ?visitors }
  OPTIONAL { ?item wdt:P131 ?municipality }
  OPTIONAL {
    ?wikipedia schema:about ?item .
    ?wikipedia schema:isPartOf <https://cs.wikipedia.org/> .
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "cs,en" }
}
ORDER BY ?itemLabel
"""

TYPE_MAP = {
    "zoologická zahrada": "zoo",
    "zoo": "zoo",
    "botanická zahrada": "botanical_garden",
    "arboretum": "botanical_garden",
    "veřejné akvárium": "aquarium",
    "safari park": "safari_park",
}


def parse_coords(coord_str):
    """Parse 'Point(lon lat)' to (lat, lon)."""
    if not coord_str or "Point" not in coord_str:
        return None, None
    parts = coord_str.replace("Point(", "").replace(")", "").split()
    if len(parts) == 2:
        return float(parts[1]), float(parts[0])
    return None, None


def get_wikipedia_text(title):
    """Get Wikipedia extract for a given title."""
    params = {
        "action": "query",
        "titles": title,
        "prop": "extracts",
        "explaintext": True,
        "format": "json",
    }
    try:
        resp = requests.get(WIKIPEDIA_API, params=params, headers=HEADERS, timeout=15)
        data = resp.json()
        pages = data.get("query", {}).get("pages", {})
        for page in pages.values():
            return page.get("extract", "")
    except Exception:
        return None
    return None


def classify_type(type_label):
    """Map Wikidata type label to our facility_type."""
    tl = type_label.lower().strip()
    for key, val in TYPE_MAP.items():
        if key in tl:
            return val
    if "zoo" in tl or "zviř" in tl or "fauna" in tl:
        return "zoo"
    if "botan" in tl or "arbor" in tl:
        return "botanical_garden"
    if "akvár" in tl or "aquar" in tl:
        return "aquarium"
    return "zoo"  # default


def main():
    print("Fetching from Wikidata SPARQL...", flush=True)
    resp = requests.get(
        WIKIDATA_SPARQL,
        params={"query": SPARQL_QUERY, "format": "json"},
        headers=HEADERS,
        timeout=60,
    )
    data = resp.json()
    results = data["results"]["bindings"]
    print(f"  Got {len(results)} rows from Wikidata", flush=True)

    # Deduplicate by QID (multiple rows per item due to OPTIONAL joins)
    items = {}
    for r in results:
        qid = r["item"]["value"].split("/")[-1]
        if qid not in items:
            lat, lon = parse_coords(r.get("coords", {}).get("value"))
            wiki_url = r.get("wikipedia", {}).get("value")
            wiki_title = wiki_url.split("/wiki/")[-1] if wiki_url else None

            items[qid] = {
                "wikidata_id": qid,
                "name": r.get("itemLabel", {}).get("value", ""),
                "facility_type": classify_type(r.get("typeLabel", {}).get("value", "")),
                "latitude": lat,
                "longitude": lon,
                "website": r.get("website", {}).get("value"),
                "wikipedia_url": wiki_url,
                "wikipedia_title": wiki_title,
                "image_url": r.get("image", {}).get("value"),
                "inception_year": None,
                "area_ha": None,
                "visitors": None,
                "municipality_name": r.get("municipalityLabel", {}).get("value"),
            }

            inception = r.get("inception", {}).get("value")
            if inception:
                try:
                    items[qid]["inception_year"] = int(inception[:4])
                except (ValueError, IndexError):
                    pass

            area = r.get("area", {}).get("value")
            if area:
                try:
                    items[qid]["area_ha"] = float(area)
                except ValueError:
                    pass

            visitors = r.get("visitors", {}).get("value")
            if visitors:
                try:
                    items[qid]["visitors"] = int(float(visitors))
                except ValueError:
                    pass

    print(f"  Deduplicated to {len(items)} unique facilities", flush=True)

    # Fetch Wikipedia texts
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    saved = 0
    for i, (qid, item) in enumerate(items.items()):
        # Get Wikipedia text if available
        wiki_text = None
        if item["wikipedia_title"]:
            wiki_text = get_wikipedia_text(item["wikipedia_title"])
            time.sleep(0.5)

        cur.execute("""
            INSERT INTO nature_facilities (
                wikidata_id, name, facility_type, latitude, longitude,
                website, wikipedia_url, wikipedia_title, wikipedia_text,
                image_url, inception_year, area_ha, visitors, municipality_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (wikidata_id) DO NOTHING
        """, (
            item["wikidata_id"], item["name"], item["facility_type"],
            item["latitude"], item["longitude"],
            item["website"], item["wikipedia_url"], item["wikipedia_title"],
            wiki_text, item["image_url"],
            item["inception_year"], item["area_ha"], item["visitors"],
            item["municipality_name"],
        ))
        saved += 1

        if (i + 1) % 20 == 0:
            conn.commit()
            print(f"  Progress: {i+1}/{len(items)} saved", flush=True)

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone! Saved {saved} facilities to cr_staging.nature_facilities", flush=True)

    # Summary
    conn2 = psycopg2.connect(STAGING_URL)
    cur2 = conn2.cursor()
    cur2.execute("SELECT facility_type, COUNT(*) FROM nature_facilities GROUP BY facility_type ORDER BY facility_type")
    for row in cur2.fetchall():
        print(f"  {row[0]}: {row[1]}")
    cur2.execute("SELECT COUNT(*) FROM nature_facilities WHERE wikipedia_text IS NOT NULL AND LENGTH(wikipedia_text) > 100")
    print(f"  With Wikipedia text: {cur2.fetchone()[0]}")
    cur2.close()
    conn2.close()


if __name__ == "__main__":
    main()
