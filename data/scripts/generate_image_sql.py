#!/usr/bin/env python3
"""Generate SQL UPDATE statements for coat of arms and flag paths based on actual downloaded files.

Scans data/images/municipalities/ and data/images/regions/ directories and generates
SQL that sets coat_of_arms_url and flag_url with correct extensions.

Convention:
  - coat-of-arms.{svg|webp} -> coat_of_arms_url
  - flag.{svg|webp} -> flag_url
  - Path stored as: /img/municipalities/{code}/coat-of-arms.webp
"""

import os
import sys

def scan_directory(base_dir, entity_type, code_column, table_name):
    """Scan image directory and generate SQL UPDATE statements."""
    lines = []
    if not os.path.isdir(base_dir):
        return lines

    for code in sorted(os.listdir(base_dir)):
        entity_dir = os.path.join(base_dir, code)
        if not os.path.isdir(entity_dir):
            continue

        sets = []
        for filename in os.listdir(entity_dir):
            filepath = os.path.join(entity_dir, filename)
            if not os.path.isfile(filepath) or os.path.getsize(filepath) < 100:
                continue

            name, ext = os.path.splitext(filename)
            r2_path = f"/img/{entity_type}/{code}/{filename}"

            if name == "coat-of-arms":
                sets.append(f"coat_of_arms_url = '{r2_path}'")
            elif name == "flag":
                sets.append(f"flag_url = '{r2_path}'")

        if sets:
            lines.append(
                f"UPDATE {table_name} SET {', '.join(sets)} WHERE {code_column} = '{code}';"
            )

    return lines


def main():
    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)))

    # Regions
    region_lines = scan_directory(
        os.path.join(data_dir, "images", "regions"),
        "regions", "region_code", "regions"
    )

    # Municipalities
    muni_lines = scan_directory(
        os.path.join(data_dir, "images", "municipalities"),
        "municipalities", "municipality_code", "municipalities"
    )

    all_lines = []
    all_lines.append("-- Auto-generated image paths from downloaded files")
    all_lines.append("-- Run after uploading images to Cloudflare R2")
    all_lines.append(f"-- Regions: {len(region_lines)}, Municipalities: {len(muni_lines)}")
    all_lines.append("")
    all_lines.append("-- Regions")
    all_lines.extend(region_lines)
    all_lines.append("")
    all_lines.append("-- Municipalities")
    all_lines.extend(muni_lines)

    output_path = os.path.join(data_dir, "wikidata", "import_image_paths.sql")
    with open(output_path, "w") as f:
        f.write("\n".join(all_lines) + "\n")

    print(f"Generated: {len(region_lines)} region + {len(muni_lines)} municipality statements")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
