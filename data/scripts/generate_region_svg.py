#!/usr/bin/env python3
"""Generate SVG map includes for each Czech region from ORP GeoJSON data.

Reads orp_simple.geojson, groups by NUTS3 region code, converts polygon
coordinates to SVG path elements, and writes Askama template includes.

Output: cr-web/templates/includes/map_{region_slug}.html
"""

import json
import os
import psycopg2

GEOJSON_PATH = "data/geojson/orp_simple.geojson"
OUTPUT_DIR = "cr-web/templates/includes"
DB_URL = "postgresql:///cr_dev"

# NUTS3 → region slug mapping
NUTS3_TO_SLUG = {
    "CZ010": "hlavni-mesto-praha",
    "CZ020": "stredocesky-kraj",
    "CZ031": "jihocesky-kraj",
    "CZ032": "plzensky-kraj",
    "CZ041": "karlovarsky-kraj",
    "CZ042": "ustecky-kraj",
    "CZ051": "liberecky-kraj",
    "CZ052": "kralovehradecky-kraj",
    "CZ053": "pardubicky-kraj",
    "CZ063": "kraj-vysocina",
    "CZ064": "jihomoravsky-kraj",
    "CZ071": "olomoucky-kraj",
    "CZ072": "zlinsky-kraj",
    "CZ080": "moravskoslezsky-kraj",
}


def get_orp_data():
    """Get ORP code → (slug, lat, lon) mapping from database."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT orp_code, slug, name, latitude, longitude FROM orp")
    result = {}
    for row in cur.fetchall():
        result[row[0]] = {"slug": row[1], "name": row[2], "lat": row[3], "lon": row[4]}
    conn.close()
    return result


def polygon_to_svg_path(coords, min_x, min_y, scale):
    """Convert GeoJSON polygon coordinates to SVG path d attribute."""
    parts = []
    for ring in coords:
        points = []
        for i, (lon, lat) in enumerate(ring):
            x = (lon - min_x) * scale
            y = (min_y - lat) * scale  # flip Y axis
            if i == 0:
                points.append(f"M {x:.1f} {y:.1f}")
            else:
                points.append(f"L {x:.1f} {y:.1f}")
        points.append("Z")
        parts.append(" ".join(points))
    return " ".join(parts)


def multipolygon_to_svg_path(geometry, min_x, min_y, scale):
    """Convert GeoJSON geometry (Polygon or MultiPolygon) to SVG path."""
    if geometry["type"] == "Polygon":
        return polygon_to_svg_path(geometry["coordinates"], min_x, min_y, scale)
    elif geometry["type"] == "MultiPolygon":
        parts = []
        for polygon in geometry["coordinates"]:
            parts.append(polygon_to_svg_path(polygon, min_x, min_y, scale))
        return " ".join(parts)
    return ""


def generate_region_svg(features, region_slug, orp_data, svg_width=480):
    """Generate SVG content for a region."""
    # Calculate bounding box
    all_coords = []
    for feat in features:
        geom = feat["geometry"]
        if geom["type"] == "Polygon":
            for ring in geom["coordinates"]:
                all_coords.extend(ring)
        elif geom["type"] == "MultiPolygon":
            for polygon in geom["coordinates"]:
                for ring in polygon:
                    all_coords.extend(ring)

    lons = [c[0] for c in all_coords]
    lats = [c[1] for c in all_coords]
    min_lon, max_lon = min(lons), max(lons)
    min_lat, max_lat = min(lats), max(lats)

    # Add padding
    pad = 0.02
    min_lon -= pad
    max_lon += pad
    min_lat -= pad
    max_lat += pad

    lon_range = max_lon - min_lon
    lat_range = max_lat - min_lat
    scale = svg_width / lon_range
    svg_height = lat_range * scale

    # Generate SVG paths, dots and labels
    path_elements = []
    label_elements = []
    for feat in features:
        props = feat["properties"]
        orp_code = props["kod_orp_p"]
        orp_name = props["naz_orp_p"]
        info = orp_data.get(orp_code, {})
        orp_slug = info.get("slug", orp_name.lower().replace(" ", "-"))
        lat = info.get("lat")
        lon = info.get("lon")

        d = multipolygon_to_svg_path(feat["geometry"], min_lon, max_lat, scale)
        path_elements.append(
            f'    <path d="{d}" class="orp" '
            f'data-name="{orp_name}" data-slug="{orp_slug}" />'
        )

        # Add dot and label if coordinates available
        if lat and lon:
            cx = (lon - min_lon) * scale
            cy = (max_lat - lat) * scale
            label_elements.append(
                f'    <circle cx="{cx:.1f}" cy="{cy:.1f}" r="2.5" class="orp-dot" />'
            )
            label_elements.append(
                f'    <text x="{cx:.1f}" y="{cy - 5:.1f}" class="orp-label">{orp_name}</text>'
            )

    svg = f'<svg id="map-region" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {svg_width:.0f} {svg_height:.0f}">\n'
    svg += "\n".join(path_elements)
    svg += "\n"
    svg += "\n".join(label_elements)
    svg += "\n</svg>"
    return svg


def main():
    with open(GEOJSON_PATH) as f:
        data = json.load(f)

    orp_data = get_orp_data()
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Group features by NUTS3 region
    by_region = {}
    for feat in data["features"]:
        nuts3 = feat["properties"]["nuts3_kraj"]
        by_region.setdefault(nuts3, []).append(feat)

    for nuts3, features in sorted(by_region.items()):
        region_slug = NUTS3_TO_SLUG.get(nuts3, nuts3)
        region_name = features[0]["properties"]["naz_kraj"]

        svg = generate_region_svg(features, region_slug, orp_data)

        output_path = os.path.join(OUTPUT_DIR, f"map_{region_slug.replace('-', '_')}.html")
        with open(output_path, "w") as f:
            f.write(svg)

        print(f"{region_name} ({nuts3}): {len(features)} ORP → {output_path}")

    print(f"\nDone! Generated {len(by_region)} region SVG maps.")


if __name__ == "__main__":
    main()
