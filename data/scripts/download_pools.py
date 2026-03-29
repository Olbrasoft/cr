#!/usr/bin/env python3
"""Scrape swimming pools, aquaparks from jduplavat.cz into cr_staging.

Step 1: Get all pool slugs from homepage map data (all pages)
Step 2: Scrape each pool detail page for full data
Saves to cr_staging.pools table.
"""

import json
import re
import time
import requests
import psycopg2
from bs4 import BeautifulSoup

STAGING_URL = "postgresql:///cr_staging"
BASE_URL = "https://jduplavat.cz"
UA = "Mozilla/5.0 (X11; Linux x86_64) CeskaRepublikaWiki/1.0"
HEADERS = {"User-Agent": UA}


def get_all_slugs():
    """Get all pool slugs from paginated listing."""
    slugs = []
    page = 1
    while True:
        url = f"{BASE_URL}/?page={page}"
        print(f"  Fetching page {page}...", flush=True)
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract pool links from cards
        links = soup.select('a[href^="/place/"]')
        page_slugs = set()
        for link in links:
            href = link.get("href", "")
            if href.startswith("/place/"):
                slug = href.replace("/place/", "").rstrip("/")
                if slug:
                    page_slugs.add(slug)

        if not page_slugs:
            break

        slugs.extend(page_slugs)
        print(f"    Found {len(page_slugs)} pools on page {page} (total: {len(slugs)})", flush=True)

        # Check if there's a next page
        next_link = soup.select_one('a[rel="next"]')
        if not next_link:
            # Also check for pagination buttons
            pag = soup.select('.pagination a')
            has_next = any('Další' in (a.text or '') for a in pag)
            if not has_next:
                break

        page += 1
        time.sleep(1)

    return list(set(slugs))


def scrape_pool_detail(slug):
    """Scrape a single pool detail page."""
    url = f"{BASE_URL}/place/{slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return None
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    data = {"slug": slug}

    # Name
    h1 = soup.select_one("h1")
    data["name"] = h1.text.strip() if h1 else slug

    # Description
    desc = soup.select_one('meta[name="description"]')
    data["description"] = desc.get("content", "").strip() if desc else None

    # GPS from page content - coordinates appear as bare numbers (49.xxxx, 13.xxxx)
    text = resp.text

    lat_match = re.search(r'(4[89]\.\d{5,})', text)
    lng_match = re.search(r'(1[2-8]\.\d{5,})', text)
    if lat_match and lng_match:
        data["latitude"] = float(lat_match.group(1))
        data["longitude"] = float(lng_match.group(1))
    else:
        data["latitude"] = None
        data["longitude"] = None

    # Website - find external links, exclude known non-official sites
    data["website"] = None
    skip_domains = ["facebook.com", "mapy.com", "jduplavat", "google.com",
                    "gstatic.com", "openstreetmap", "unpkg.com", "leaflet",
                    "frame.mapy", "cdn.", "fonts.", "jquery", "bootstrap"]
    for link in soup.select('a[href^="http"]'):
        href = link.get("href", "")
        if any(skip in href for skip in skip_domains):
            continue
        if href.startswith("http"):
            data["website"] = href.rstrip(")")
            break

    # Facebook
    fb_link = soup.select_one('a[href*="facebook.com"]')
    data["facebook"] = fb_link.get("href") if fb_link else None

    # Email
    email_match = re.search(r'[\w.-]+@[\w.-]+\.\w+', text)
    data["email"] = email_match.group(0) if email_match else None

    # Phone
    phone_link = soup.select_one('a[href^="tel:"]')
    data["phone"] = phone_link.get("href", "").replace("tel:", "") if phone_link else None

    # Address
    addr_el = soup.select_one('.address, [itemprop="address"]')
    if addr_el:
        data["address"] = addr_el.text.strip()
    else:
        # Try to find address pattern in text
        addr_match = re.search(r'([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+\s+\d+[/\d]*,\s*[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž\s]+)', text)
        data["address"] = addr_match.group(1) if addr_match else None

    # Lane schedule link
    lane_link = soup.select_one('a[href*="rozpis"], a[href*="rozvrh"]')
    data["lane_schedule_url"] = lane_link.get("href") if lane_link else None

    # Facilities/equipment - look for common keywords
    facilities = []
    facility_keywords = {
        "tobogán": "tobogan",
        "sauna": "sauna",
        "whirlpool": "whirlpool",
        "jacuzzi": "jacuzzi",
        "vířivka": "whirlpool",
        "dětský bazén": "kids_pool",
        "parní": "steam_sauna",
        "wellness": "wellness",
        "fitness": "fitness",
    }
    text_lower = text.lower()
    for keyword, code in facility_keywords.items():
        if keyword in text_lower:
            facilities.append(code)
    data["facilities"] = ",".join(facilities) if facilities else None

    # Pool length
    length_match = re.search(r'(\d+)\s*m\s*(?:bazén|pool|drah)', text_lower)
    data["pool_length_m"] = int(length_match.group(1)) if length_match else None

    # Maps link
    maps_link = soup.select_one('a[href*="mapy.com"], a[href*="google.com/maps"]')
    data["maps_url"] = maps_link.get("href") if maps_link else None

    return data


def main():
    # Create table
    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pools (
            id SERIAL PRIMARY KEY,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            address TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            website TEXT,
            email TEXT,
            phone TEXT,
            facebook TEXT,
            lane_schedule_url TEXT,
            maps_url TEXT,
            facilities TEXT,
            pool_length_m INT,
            fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)
    conn.commit()

    # Check already scraped
    cur.execute("SELECT slug FROM pools")
    done = {r[0] for r in cur.fetchall()}

    print("Step 1: Getting all pool slugs...", flush=True)
    slugs = get_all_slugs()
    slugs = [s for s in slugs if s not in done]
    print(f"  Total new slugs: {len(slugs)}", flush=True)

    print(f"\nStep 2: Scraping {len(slugs)} pool detail pages...", flush=True)
    scraped = 0
    failed = 0

    for i, slug in enumerate(slugs):
        data = scrape_pool_detail(slug)

        if data is None:
            failed += 1
        else:
            cur.execute("""
                INSERT INTO pools (slug, name, description, address, latitude, longitude,
                    website, email, phone, facebook, lane_schedule_url, maps_url,
                    facilities, pool_length_m)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (slug) DO NOTHING
            """, (
                data["slug"], data["name"], data.get("description"),
                data.get("address"), data.get("latitude"), data.get("longitude"),
                data.get("website"), data.get("email"), data.get("phone"),
                data.get("facebook"), data.get("lane_schedule_url"),
                data.get("maps_url"), data.get("facilities"),
                data.get("pool_length_m"),
            ))
            scraped += 1

        if (i + 1) % 10 == 0:
            conn.commit()
            print(f"  Progress: {i+1}/{len(slugs)} (scraped: {scraped}, failed: {failed})", flush=True)

        time.sleep(2)  # Polite: 2 seconds between requests

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone! Scraped: {scraped}, Failed: {failed}, Total: {len(slugs)}", flush=True)


if __name__ == "__main__":
    main()
