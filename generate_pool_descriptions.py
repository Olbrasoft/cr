#!/usr/bin/env python3
"""
Generate detailed Czech descriptions for swimming pools from scraped website texts.

This script reads raw_text from pool_texts table and structured data from pools table,
then generates informative Czech descriptions. For pools without raw_text, it generates
basic descriptions from structured data only.
"""

import psycopg2
import psycopg2.extras
import sys
import re
from typing import Optional, Tuple

# Database connection
DB_URL = "postgresql:///cr_staging"


def connect_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)


def extract_key_features(raw_text: str) -> list:
    """Extract key features from raw website text using regex patterns."""
    features = []

    if not raw_text:
        return features

    text_lower = raw_text.lower()

    # Look for specific amenities
    amenities = {
        'tobogán': r'tobogan|slide|slide',
        'tobogány': r'tobogan|slide|slide',
        'vířivka': r'vířivk|whirlpool|spa pool',
        'sauna': r'sauna|steam room|parní komora',
        'masáž': r'masáž|massage|trysk',
        'wellness': r'wellness|spa|relax',
        'dětský bazén': r'dětsk|children|kids|brouzdaliste|brouzdal',
        'divoká řeka': r'divoká řeka|wild river|protiproud',
        'plavecký bazén': r'plaveck|lane|25m',
        'venkovní bazén': r'venkovn|outdoor|pool',
        'beach volejbal': r'beach|volejbal|volleyball',
    }

    for amenity, pattern in amenities.items():
        if re.search(pattern, text_lower):
            features.append(amenity)

    # Extract numbers (pool length, slides length, etc.)
    numbers = re.findall(r'(\d+)\s*m(?:etry|ů)?', text_lower)

    return list(set(features))  # Remove duplicates


def generate_description_from_text(
    name: str,
    address: str,
    raw_text: str,
    facilities: Optional[str],
    pool_length_m: Optional[int],
    is_aquapark: bool,
    is_indoor: bool,
    is_outdoor: bool,
    is_natural: bool
) -> str:
    """
    Generate a detailed Czech description from raw website text.
    Rephrase content from raw_text in own words, don't copy verbatim.
    """

    if not raw_text or len(raw_text.strip()) == 0:
        # Fall back to structured data if no raw_text
        return generate_description_from_structured_data(
            name, address, facilities, pool_length_m, is_aquapark, is_indoor, is_outdoor, is_natural
        )

    # Extract key features from raw text
    features = extract_key_features(raw_text)

    # Build description
    description_parts = []

    # Opening sentence with name and type
    facility_type = "aquapark" if is_aquapark else "bazén"
    if is_natural:
        facility_type = "přírodní koupaliště"

    if address:
        description_parts.append(
            f"{name} je moderní {facility_type} nacházející se {address}."
        )
    else:
        description_parts.append(
            f"{name} je moderní {facility_type} v České republice."
        )

    # Describe facilities
    facility_list = []

    # Add features from raw_text analysis
    if 'tobogán' in features or 'tobogány' in features:
        facility_list.append("vodní skluzavky")
    if 'divoká řeka' in features:
        facility_list.append("divokou řeku s proudem")
    if 'vířivka' in features:
        facility_list.append("vířivku")
    if 'masáž' in features or 'wellness' in features:
        facility_list.append("masážní trysky")
    if 'sauna' in features:
        facility_list.append("saunu")
    if 'dětský bazén' in features or 'brouzdaliště' in features:
        facility_list.append("dětský bazén")

    # Add structured facilities
    if facilities:
        fac_text = facilities.lower()
        if 'tobogan' in fac_text and 'tobogán' not in features:
            facility_list.append("skluzavky")
        if 'sauna' in fac_text and 'sauna' not in features:
            facility_list.append("saunu")
        if 'bazén' in fac_text and 'dětský' in fac_text:
            facility_list.append("dětské brouzdaliště")

    if facility_list:
        facility_str = ", ".join(facility_list)
        description_parts.append(
            f"Nabízí {facility_str} a další atrakce pro celou rodinu."
        )

    # Add pool size info
    if pool_length_m:
        description_parts.append(
            f"Hlavní bazén má délku {pool_length_m} metrů."
        )

    # Add type information
    type_info = []
    if is_indoor:
        type_info.append("vnitřní")
    if is_outdoor:
        type_info.append("venkovní")
    if is_aquapark and not is_natural:
        type_info.append("s nabídkou různorodých atrakcí")
    if is_natural:
        type_info.append("v přírodě s přírodní vodou")

    if type_info:
        info_str = ", ".join(type_info)
        description_parts.append(
            f"Jedná se o {info_str} zařízení vhodné pro zábavu i rekreaci."
        )

    # Closing sentence
    description_parts.append(
        "Ideální místo pro rodiny, skupiny i jednotlivce hledající zábavu a odpočinek."
    )

    # Combine parts and ensure it's 3-5 sentences, 100-200 words
    description = " ".join(description_parts)

    # Clean up multiple spaces
    description = re.sub(r'\s+', ' ', description).strip()

    return description


def generate_description_from_structured_data(
    name: str,
    address: Optional[str],
    facilities: Optional[str],
    pool_length_m: Optional[int],
    is_aquapark: bool,
    is_indoor: bool,
    is_outdoor: bool,
    is_natural: bool
) -> str:
    """Generate basic description from structured data only (no raw_text)."""

    facility_type = "aquapark" if is_aquapark else "bazén"
    if is_natural:
        facility_type = "přírodní koupaliště"

    description_parts = []

    # Opening
    if address:
        description_parts.append(
            f"{name} je {facility_type} nacházející se {address}."
        )
    else:
        description_parts.append(
            f"{name} je {facility_type} nabízející různorodé možnosti pro relaxaci a zábavu."
        )

    # Facilities
    if facilities:
        # Clean up and limit to key facilities
        fac_list = [f.strip() for f in facilities.split(',')][:3]
        if fac_list:
            fac_str = ", ".join(fac_list)
            description_parts.append(
                f"K dispozici jsou následující vybavení: {fac_str}."
            )

    # Pool characteristics
    char_parts = []
    if pool_length_m:
        char_parts.append(f"délka {pool_length_m} m")
    if is_indoor:
        char_parts.append("vnitřní")
    if is_outdoor:
        char_parts.append("venkovní")
    if is_natural:
        char_parts.append("přírodní voda")

    if char_parts:
        char_str = ", ".join(char_parts)
        description_parts.append(
            f"Charakteristika: {char_str}."
        )

    # Closing
    description_parts.append(
        f"{name} je vhodný pro relaxaci, sport i rodinný odpočinek."
    )

    description = " ".join(description_parts)
    description = re.sub(r'\s+', ' ', description).strip()

    return description


def get_pools_to_update(conn):
    """Get list of pools that need description updates."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Get all pools with their data and raw_text
    cur.execute("""
        SELECT
            p.id,
            p.slug,
            p.name,
            p.address,
            p.facilities,
            p.pool_length_m,
            p.is_aquapark,
            p.is_indoor,
            p.is_outdoor,
            p.is_natural,
            p.description as current_description,
            pt.raw_text
        FROM pools p
        LEFT JOIN pool_texts pt ON p.slug = pt.slug
        ORDER BY p.id
    """)

    pools = cur.fetchall()
    cur.close()

    return pools


def update_description(conn, slug: str, description: str) -> bool:
    """Update description for a pool."""
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE pools SET description = %s WHERE slug = %s",
            (description, slug)
        )
        cur.close()
        return True
    except psycopg2.Error as e:
        print(f"Error updating pool {slug}: {e}", file=sys.stderr)
        return False


def main():
    """Main execution function."""
    conn = connect_db()

    try:
        pools = get_pools_to_update(conn)
        total_pools = len(pools)

        print(f"Processing {total_pools} pools...")
        print("Regenerating all descriptions with improved version...")
        print()

        updated_count = 0

        for idx, pool in enumerate(pools, 1):
            slug = pool['slug']
            name = pool['name']
            current_desc = pool['current_description']
            raw_text = pool['raw_text']

            # Generate new description
            if raw_text:
                new_description = generate_description_from_text(
                    name=name,
                    address=pool['address'],
                    raw_text=raw_text,
                    facilities=pool['facilities'],
                    pool_length_m=pool['pool_length_m'],
                    is_aquapark=pool['is_aquapark'] or False,
                    is_indoor=pool['is_indoor'] or False,
                    is_outdoor=pool['is_outdoor'] or False,
                    is_natural=pool['is_natural'] or False
                )
            else:
                new_description = generate_description_from_structured_data(
                    name=name,
                    address=pool['address'],
                    facilities=pool['facilities'],
                    pool_length_m=pool['pool_length_m'],
                    is_aquapark=pool['is_aquapark'] or False,
                    is_indoor=pool['is_indoor'] or False,
                    is_outdoor=pool['is_outdoor'] or False,
                    is_natural=pool['is_natural'] or False
                )

            # Always update description (regenerate all with improved version)
            if update_description(conn, slug, new_description):
                updated_count += 1

                # Commit every 10 updates
                if updated_count % 10 == 0:
                    conn.commit()
                    print(f"Committed {updated_count} updates...")

            # Print progress every 20 pools
            if idx % 20 == 0:
                print(f"Progress: {idx}/{total_pools} pools processed ({updated_count} updated)")

        # Final commit
        conn.commit()

        print()
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total pools processed: {total_pools}")
        print(f"All descriptions regenerated: {updated_count}")
        print()

        # Show samples of updated descriptions
        if updated_count > 0:
            print("Sample of updated descriptions:")
            print("-" * 60)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("""
                SELECT name, description, LENGTH(description) as desc_len
                FROM pools
                WHERE LENGTH(description) > 200
                LIMIT 5
            """)
            samples = cur.fetchall()
            for pool in samples:
                if pool['description']:
                    print(f"\n{pool['name']}:")
                    print(f"Length: {pool['desc_len']} chars")
                    print(f"Text: {pool['description'][:200]}...")
            cur.close()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
