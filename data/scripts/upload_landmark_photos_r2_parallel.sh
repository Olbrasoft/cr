#!/bin/bash
# Parallel upload of landmark photos to Cloudflare R2
# Simple naming: landmarks/{catalog_id}.webp
# SEO-friendly URLs handled by Cloudflare Worker or Axum route

set -e

BUCKET="cr-images"
IMG_DIR="/home/jirka/Olbrasoft/cr/data/images/landmarks"
LOG_OK="/tmp/r2_landmarks_ok.log"
LOG_ERR="/tmp/r2_landmarks_err.log"

> "$LOG_OK"
> "$LOG_ERR"

export BUCKET IMG_DIR

upload_one() {
    local FILE="$1"
    local BASENAME=$(basename "$FILE")
    local KEY="landmarks/${BASENAME}"

    if npx wrangler r2 object put "${BUCKET}/${KEY}" \
        --file="$FILE" \
        --content-type="image/webp" \
        --remote 2>&1 | grep -q "Upload complete"; then
        echo "$KEY" >> /tmp/r2_landmarks_ok.log
    else
        echo "$KEY" >> /tmp/r2_landmarks_err.log
    fi
}

export -f upload_one

TOTAL=$(find "$IMG_DIR" -name "*.webp" | wc -l)
echo "Uploading $TOTAL landmark photos to R2 (4 parallel workers)..."
echo "Naming: landmarks/{catalog_id}.webp"
echo "Start: $(date)"

find "$IMG_DIR" -name "*.webp" | parallel -j4 upload_one {}

OK=$(wc -l < "$LOG_OK" 2>/dev/null || echo 0)
ERR=$(wc -l < "$LOG_ERR" 2>/dev/null || echo 0)

echo ""
echo "Done! Uploaded: $OK, Failed: $ERR, Total: $TOTAL"
echo "End: $(date)"
