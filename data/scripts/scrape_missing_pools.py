#!/usr/bin/env python3
"""Scrape missing pools from jduplavat.cz and add to staging DB."""

import re
import time
import os
import requests
import psycopg2
from bs4 import BeautifulSoup

STAGING_URL = "postgresql:///cr_staging"
BASE_URL = "https://jduplavat.cz"
UA = "Mozilla/5.0 (X11; Linux x86_64) CeskaRepublikaWiki/1.0"
HEADERS = {"User-Agent": UA}
PHOTO_DIR = "/home/jirka/Olbrasoft/cr/data/images/pools"

# Read map data for types and GPS
MAP_DATA = {}
with open("/tmp/jduplavat.html") as f:
    html = f.read()
for m in re.finditer(r'"slug"\s*:\s*"([^"]+)"', html):
    slug = m.group(1)
    start = max(0, m.start() - 500)
    end = min(len(html), m.end() + 500)
    ctx = html[start:end]
    MAP_DATA[slug] = {
        "indoor": bool(re.search(r'"indoor_pool"\s*:\s*true', ctx)),
        "outdoor": bool(re.search(r'"outdoor_pool"\s*:\s*true', ctx)),
        "natural": bool(re.search(r'"natural_pool"\s*:\s*true', ctx)),
        "lat": float(m2.group(1)) if (m2 := re.search(r'"lat"\s*:\s*([\d.]+)', ctx)) else None,
        "lng": float(m2.group(1)) if (m2 := re.search(r'"lng"\s*:\s*([\d.]+)', ctx)) else None,
    }


def scrape_detail(slug):
    url = f"{BASE_URL}/place/{slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return None
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    text = resp.text
    data = {"slug": slug}

    h1 = soup.select_one("h1")
    data["name"] = h1.text.strip() if h1 else slug

    desc = soup.select_one('meta[name="description"]')
    data["description"] = desc.get("content", "").strip() if desc else None

    # GPS from map data
    md = MAP_DATA.get(slug, {})
    data["latitude"] = md.get("lat")
    data["longitude"] = md.get("lng")
    data["is_indoor"] = md.get("indoor", False)
    data["is_outdoor"] = md.get("outdoor", False)
    data["is_natural"] = md.get("natural", False)
    data["is_aquapark"] = "aqua" in slug or "aqualand" in slug

    # Website
    data["website"] = None
    skip = ["facebook.com", "mapy.com", "jduplavat", "google.com", "gstatic", "openstreetmap", "unpkg", "leaflet", "frame.mapy", "cdn.", "fonts.", "jquery", "bootstrap"]
    for link in soup.select('a[href^="http"]'):
        href = link.get("href", "")
        if any(s in href for s in skip):
            continue
        if href.startswith("http"):
            data["website"] = href.rstrip(")")
            break

    fb = soup.select_one('a[href*="facebook.com"]')
    data["facebook"] = fb.get("href") if fb else None

    email_m = re.search(r'[\w.-]+@[\w.-]+\.\w+', text)
    data["email"] = email_m.group(0) if email_m else None

    phone = soup.select_one('a[href^="tel:"]')
    data["phone"] = phone.get("href", "").replace("tel:", "") if phone else None

    addr_m = re.search(r'([A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž]+\s+\d+[/\d]*,\s*[A-ZÁČĎÉĚÍŇÓŘŠŤÚŮÝŽ][a-záčďéěíňóřšťúůýž\s]+)', text)
    data["address"] = addr_m.group(1) if addr_m else None

    facilities = []
    fkw = {"tobogán": "tobogan", "sauna": "sauna", "whirlpool": "whirlpool", "vířivka": "whirlpool",
           "dětský bazén": "kids_pool", "parní": "steam_sauna", "wellness": "wellness", "fitness": "fitness"}
    tl = text.lower()
    for kw, code in fkw.items():
        if kw in tl:
            facilities.append(code)
    data["facilities"] = ",".join(facilities) if facilities else None

    len_m = re.search(r'(\d+)\s*m\s*(?:bazén|pool|drah)', tl)
    data["pool_length_m"] = int(len_m.group(1)) if len_m else None

    # Photos
    photos = []
    for img in soup.select("img"):
        src = img.get("src", "") or img.get("data-src", "")
        if "/uploads/pools/" in src and "image" in src:
            if src.startswith("/"):
                src = BASE_URL + src
            photos.append(src)
    photos = list(dict.fromkeys(photos))

    photo_count = 0
    os.makedirs(PHOTO_DIR, exist_ok=True)
    for j, img_url in enumerate(photos):
        out = os.path.join(PHOTO_DIR, f"{slug}-{j+1}.webp")
        if os.path.exists(out) and os.path.getsize(out) > 500:
            photo_count += 1
            continue
        try:
            ir = requests.get(img_url, headers=HEADERS, timeout=30)
            if ir.status_code == 200 and len(ir.content) > 500:
                ext = img_url.rsplit(".", 1)[-1].lower()
                if ext == "webp":
                    with open(out, "wb") as f:
                        f.write(ir.content)
                else:
                    tmp = out.replace(".webp", f".{ext}")
                    with open(tmp, "wb") as f:
                        f.write(ir.content)
                    os.system(f'cwebp -q 85 -quiet "{tmp}" -o "{out}" 2>/dev/null')
                    if os.path.exists(out):
                        os.remove(tmp)
                photo_count += 1
        except Exception:
            pass
        time.sleep(0.5)

    data["photo_count"] = photo_count
    return data


def main():
    with open("/tmp/missing_slugs.txt") as f:
        slugs = [l.strip() for l in f if l.strip()]

    print(f"Scraping {len(slugs)} missing pools...", flush=True)

    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    done = 0
    for i, slug in enumerate(slugs):
        data = scrape_detail(slug)
        if data is None:
            print(f"  FAIL: {slug}", flush=True)
            continue

        cur.execute("""
            INSERT INTO pools (name, slug, description, address, latitude, longitude, website, email, phone,
                              facebook, facilities, pool_length_m, is_aquapark, is_indoor, is_outdoor, is_natural, photo_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (slug) DO NOTHING
        """, (data["name"], data["slug"], data.get("description"), data.get("address"),
              data.get("latitude"), data.get("longitude"), data.get("website"), data.get("email"),
              data.get("phone"), data.get("facebook"), data.get("facilities"), data.get("pool_length_m"),
              data.get("is_aquapark", False), data.get("is_indoor", False),
              data.get("is_outdoor", False), data.get("is_natural", False), data.get("photo_count", 0)))
        done += 1

        if (i + 1) % 10 == 0:
            conn.commit()
            print(f"  Progress: {i+1}/{len(slugs)} (done: {done})", flush=True)

        time.sleep(2)

    conn.commit()
    cur.close()
    conn.close()
    print(f"\nDone! Added {done} pools", flush=True)


if __name__ == "__main__":
    main()
