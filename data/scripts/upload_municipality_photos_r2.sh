#!/bin/bash
# Upload municipality photos to Cloudflare R2
# R2 key: municipalities/{code}/{slug}.webp (SEO-friendly)
# Local file: {code}-{slug}.webp

set -e

BUCKET="cr-images"
IMG_DIR="/home/jirka/Olbrasoft/cr/data/images/municipalities_wiki"
LOG_OK="/tmp/r2_muni_photos_ok.log"
LOG_ERR="/tmp/r2_muni_photos_err.log"

> "$LOG_OK"
> "$LOG_ERR"

export BUCKET IMG_DIR

upload_one() {
    local FILE="$1"
    local BASENAME=$(basename "$FILE" .webp)
    # Parse: {code}-{slug} → code is first part before first hyphen (6 digits)
    local CODE=$(echo "$BASENAME" | grep -oP '^\d+')
    local SLUG=$(echo "$BASENAME" | sed "s/^${CODE}-//")
    local KEY="municipalities/${CODE}/${SLUG}.webp"

    if npx wrangler r2 object put "${BUCKET}/${KEY}" \
        --file="$FILE" \
        --content-type="image/webp" \
        --remote 2>&1 | grep -q "Upload complete"; then
        echo "$KEY" >> /tmp/r2_muni_photos_ok.log
    else
        echo "$KEY" >> /tmp/r2_muni_photos_err.log
    fi
}

export -f upload_one

TOTAL=$(find "$IMG_DIR" -name "*.webp" | wc -l)
echo "Uploading $TOTAL municipality photos to R2 (4 parallel workers)..."
echo "Naming: municipalities/{code}/{slug}.webp"
echo "Start: $(date)"

find "$IMG_DIR" -name "*.webp" | parallel -j4 upload_one {}

OK=$(wc -l < "$LOG_OK" 2>/dev/null || echo 0)
ERR=$(wc -l < "$LOG_ERR" 2>/dev/null || echo 0)

echo ""
echo "Done! Uploaded: $OK, Failed: $ERR, Total: $TOTAL"
echo "End: $(date)"
