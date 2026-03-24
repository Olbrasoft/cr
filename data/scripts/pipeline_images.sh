#!/bin/bash
# Complete image pipeline: download → convert → generate SQL → upload to R2
#
# Usage: bash data/scripts/pipeline_images.sh [step]
#   Steps: download, convert, sql, upload, all (default: all)
#
# Prerequisites:
#   - python3, curl, cwebp (sudo apt install webp)
#   - wrangler authenticated (npx wrangler login)

set -e
cd "$(dirname "$0")/../.."  # cd to project root

STEP="${1:-all}"

download() {
    echo "=== STEP 1: Download images from Wikimedia Commons ==="
    python3 data/scripts/download_municipality_images.py
}

convert() {
    echo "=== STEP 2: Convert raster images to WebP ==="
    bash data/scripts/convert_to_webp.sh data/images/municipalities
    echo ""
    echo "Final format distribution:"
    find data/images -type f | sed 's/.*\.//' | sort | uniq -c | sort -rn
}

generate_sql() {
    echo "=== STEP 3: Generate SQL with correct image paths ==="
    python3 data/scripts/generate_image_sql.py
}

upload() {
    echo "=== STEP 4: Upload images to Cloudflare R2 ==="
    cd workers/img-proxy

    UPLOADED=0
    TOTAL=$(find ../../data/images -type f | wc -l)
    echo "Uploading $TOTAL images..."

    for entity_type in regions municipalities; do
        for dir in "../../data/images/${entity_type}"/*/; do
            [ -d "$dir" ] || continue
            code=$(basename "$dir")
            for file in "$dir"*; do
                [ -f "$file" ] || continue
                fname=$(basename "$file")
                r2_key="${entity_type}/${code}/${fname}"

                case "$fname" in
                    *.svg) ct="image/svg+xml" ;;
                    *.webp) ct="image/webp" ;;
                    *.png) ct="image/png" ;;
                    *.jpg|*.jpeg) ct="image/jpeg" ;;
                    *) ct="application/octet-stream" ;;
                esac

                if npx wrangler r2 object put "cr-images/${r2_key}" \
                    --file="$file" --content-type="$ct" --remote 2>/dev/null | grep -q "Upload complete"; then
                    UPLOADED=$((UPLOADED + 1))
                else
                    echo "  FAIL: $r2_key"
                fi

                if [ $((UPLOADED % 200)) -eq 0 ] && [ $UPLOADED -gt 0 ]; then
                    echo "  Uploaded: $UPLOADED / $TOTAL"
                fi
            done
        done
    done

    echo ""
    echo "Upload complete: $UPLOADED images"
    cd ../..
}

case "$STEP" in
    download) download ;;
    convert) convert ;;
    sql) generate_sql ;;
    upload) upload ;;
    all)
        download
        echo ""
        convert
        echo ""
        generate_sql
        echo ""
        upload
        ;;
    *) echo "Unknown step: $STEP. Use: download, convert, sql, upload, all" ;;
esac
