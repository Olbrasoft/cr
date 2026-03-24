#!/usr/bin/env python3
"""Download coat of arms and flags for all Czech municipalities from Wikimedia Commons."""

import json
import os
import subprocess
import time
import sys
from urllib.parse import urlparse, unquote

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'images', 'municipalities')

def download_file(url, outfile):
    """Download a file using curl. Returns True on success."""
    result = subprocess.run([
        'curl', '-s', '-L', '-o', outfile,
        '-H', 'User-Agent: CeskaRepublikaWiki/1.0 (info@ceskarepublika.wiki)',
        url
    ], capture_output=True, timeout=60)
    return os.path.exists(outfile) and os.path.getsize(outfile) > 100

def get_extension(url):
    path = unquote(urlparse(url).path)
    ext = os.path.splitext(path)[1].lower()
    return ext if ext else '.svg'

def main():
    data_dir = os.path.dirname(os.path.dirname(__file__))

    # Load coat of arms data
    coats = {}
    with open(os.path.join(data_dir, 'wikidata_municipalities.json')) as f:
        data = json.load(f)
    for r in data['results']['bindings']:
        code = r['municipalityCode']['value']
        if 'coatOfArms' in r and code not in coats:
            coats[code] = r['coatOfArms']['value']

    # Load flags data
    flags = {}
    with open(os.path.join(data_dir, 'wikidata', 'municipality_flags.json')) as f:
        data = json.load(f)
    for r in data['results']['bindings']:
        code = r['municipalityCode']['value']
        if code not in flags:
            flags[code] = r['flag']['value']

    all_codes = sorted(set(list(coats.keys()) + list(flags.keys())))
    total = sum(1 for c in all_codes if c in coats) + sum(1 for c in all_codes if c in flags)

    print(f"Total images to download: {total}")
    print(f"  Coat of arms: {len(coats)}")
    print(f"  Flags: {len(flags)}")
    print(f"  Municipalities: {len(all_codes)}")

    downloaded = 0
    skipped = 0
    errors = 0

    for i, code in enumerate(all_codes):
        muni_dir = os.path.join(BASE_DIR, code)
        os.makedirs(muni_dir, exist_ok=True)

        for field_name, url_map, prefix in [
            ('coat', coats, 'coat-of-arms'),
            ('flag', flags, 'flag'),
        ]:
            if code not in url_map:
                continue

            url = url_map[code]
            ext = get_extension(url)
            outfile = os.path.join(muni_dir, f'{prefix}{ext}')

            if os.path.exists(outfile) and os.path.getsize(outfile) > 100:
                skipped += 1
                continue

            if download_file(url, outfile):
                downloaded += 1
            else:
                errors += 1
                # Clean up empty/broken file
                if os.path.exists(outfile):
                    os.remove(outfile)

            # Rate limit: ~3 requests/second
            time.sleep(0.3)

        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(all_codes)} municipalities, {downloaded} downloaded, {skipped} skipped, {errors} errors")

    print(f"\nDone! Downloaded: {downloaded}, Skipped: {skipped}, Errors: {errors}")

if __name__ == '__main__':
    main()
