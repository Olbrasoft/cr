#!/bin/bash
# Convert all raster images (JPG, JPEG, GIF, PNG) to WebP.
# SVG files are left as-is (vector format, best quality for heraldic images).
# Originals are removed after successful conversion.
#
# Usage: bash data/scripts/convert_to_webp.sh [directory]
# Default directory: data/images/municipalities

set -e

DIR="${1:-data/images/municipalities}"
CONVERTED=0
SKIPPED=0
FAILED=0
SVG=0

echo "Converting raster images to WebP in: $DIR"

find "$DIR" -type f \( -name "*.jpg" -o -name "*.jpeg" -o -name "*.gif" -o -name "*.png" \) | while read -r file; do
    dir=$(dirname "$file")
    base=$(basename "$file")
    name="${base%.*}"
    webp_file="${dir}/${name}.webp"

    if [ -f "$webp_file" ]; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    if cwebp -q 90 -quiet "$file" -o "$webp_file" 2>/dev/null; then
        rm "$file"
        CONVERTED=$((CONVERTED + 1))
    else
        # cwebp failed — try imagemagick as fallback
        if convert "$file" "$webp_file" 2>/dev/null; then
            rm "$file"
            CONVERTED=$((CONVERTED + 1))
        else
            echo "  FAIL: $file"
            FAILED=$((FAILED + 1))
        fi
    fi
done

# Count SVGs
SVG=$(find "$DIR" -name "*.svg" -type f | wc -l)

echo ""
echo "Conversion complete."
echo "  Converted to WebP: see above"
echo "  SVG (kept as-is): $SVG"

# Final statistics
echo ""
echo "Final file types:"
find "$DIR" -type f | sed 's/.*\.//' | sort | uniq -c | sort -rn
