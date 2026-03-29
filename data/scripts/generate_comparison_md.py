#!/usr/bin/env python3
"""Generate comparison markdown files: originals + Gemma 3 27B rewrites."""

import time
import os
import requests
import psycopg2

GEMINI_KEY = "AIzaSyBR4Um_j1hi5ZJvuIWuziHX6HQ2eW83piQ"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemma-3-27b-it:generateContent?key={GEMINI_KEY}"
STAGING_URL = "postgresql:///cr_staging"
OUT_DIR = "/home/jirka/Olbrasoft/cr/data/porovnani"

SYSTEM_PROMPT = """Jsi odborný copywriter specializující se na české kulturní dědictví. Přepiš poskytnutý text o památce podle těchto pravidel:

POVINNÉ:
- Zachovej VŠECHNA fakta, data, rozměry, jména osob a historické údaje beze změny
- Zachovej STEJNOU délku a podrobnost textu — nesmíš nic vynechat ani zkrátit
- Každý odstavec originálu přepiš jako samostatný odstavec

KLÍČOVÉ — JAK PŘEPISOVAT:
- KAŽDOU větu musíš přeformulovat jinak. NIKDY nekopíruj celá souvětí z originálu doslovně
- Změň slovosled vět — co bylo na začátku věty, dej doprostřed nebo na konec
- Použij jiné spojky, jiná příslovce, jiné větné konstrukce
- Rozděl dlouhá souvětí na kratší, nebo naopak spoj krátké věty do delších
- Odborné termíny (suprafenestra, rizalit, bosáž, klenba) zachovej, ale větu kolem nich postav jinak
- Místo "nachází se" použij "stojí", "je umístěn", "rozkládá se" — obměňuj slovesa
- Místo "je zaklenutý" použij "klenba pokrývá", "zastřešuje" apod.
- Historické pasáže přeformuluj — např. "v roce 1654 postavil palác" → "palác vznikl roku 1654"

CÍL: Text musí být natolik odlišný od originálu, aby ho vyhledávače NEPOVAŽOVALY za duplicitní obsah. Pokud bys porovnal originál a tvůj text větu po větě, žádné dvě věty by neměly být stejné.

Napiš POUZE přepsaný text, bez komentářů nebo vysvětlení."""


def rewrite(text):
    payload = {
        "contents": [
            {"role": "user", "parts": [{"text": SYSTEM_PROMPT + "\n\n" + text}]}
        ],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 4000,
        },
    }
    resp = requests.post(GEMINI_URL, json=payload, timeout=120)
    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]


def slugify(name):
    import unicodedata
    import re
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^\w\s-]", "", name.lower())
    return re.sub(r"[-\s]+", "-", name).strip("-")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    conn = psycopg2.connect(STAGING_URL)
    cur = conn.cursor()

    # 3 landmarks: zámek Ploskovice, Moravská banka, Kalvárie
    cur.execute("""
        SELECT catalog_id, name, annotation, description, historical_development
        FROM npu_details
        WHERE catalog_id IN ('1000137888', '1000149111', '1000154835')
        ORDER BY catalog_id
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    for catalog_id, name, annotation, description, history in rows:
        slug = slugify(name)

        # Build source text with sections
        source_parts = []
        if annotation and annotation.strip():
            source_parts.append(f"## Anotace\n\n{annotation.strip()}")
        if description and description.strip():
            source_parts.append(f"## Popis\n\n{description.strip()}")
        if history and history.strip():
            source_parts.append(f"## Historický vývoj\n\n{history.strip()}")

        source_md = "\n\n".join(source_parts)
        source_plain = "\n\n".join([
            p.strip() for p in [annotation, description, history] if p and p.strip()
        ])

        # Write original
        orig_path = os.path.join(OUT_DIR, f"original-{slug}.md")
        with open(orig_path, "w") as f:
            f.write(f"# {name}\n\n")
            f.write(f"*Zdroj: NPÚ Památkový katalog (catalog: {catalog_id})*\n\n")
            f.write(source_md)
        print(f"Uložen originál: {orig_path}", flush=True)

        # Rewrite with Gemma
        print(f"Přepisuji: {name}...", flush=True)
        rewritten = rewrite(source_plain)

        # Write rewritten
        rew_path = os.path.join(OUT_DIR, f"prepis-gemma3-{slug}.md")
        with open(rew_path, "w") as f:
            f.write(f"# {name} (přepis Gemma 3 27B)\n\n")
            f.write(f"*Přepsáno modelem Gemma 3 27B z originálu NPÚ (catalog: {catalog_id})*\n\n")
            f.write(rewritten)
        print(f"Uložen přepis: {rew_path}", flush=True)

        time.sleep(5)

    print(f"\nHotovo! Soubory v: {OUT_DIR}", flush=True)


if __name__ == "__main__":
    main()
