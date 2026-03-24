#!/bin/bash
# Upload municipality coat of arms and flags to Cloudflare R2.
# Run from project root: bash data/scripts/upload_images_to_r2.sh
#
# Prerequisites: wrangler must be authenticated (npx wrangler login)

set -e

WORKER_DIR="workers/img-proxy"
IMAGE_DIR="data/images/municipalities"
UPLOADED=0
FAILED=0
SKIPPED=0
TOTAL=$(find "$IMAGE_DIR" -type f | wc -l)

echo "Uploading $TOTAL municipality images to R2..."

for dir in "$IMAGE_DIR"/*/; do
    code=$(basename "$dir")
    for file in "$dir"*; do
        [ -f "$file" ] || continue
        fname=$(basename "$file")
        r2_key="municipalities/${code}/${fname}"

        # Determine content type
        case "$fname" in
            *.svg) ct="image/svg+xml" ;;
            *.png) ct="image/png" ;;
            *.jpg|*.jpeg) ct="image/jpeg" ;;
            *.gif) ct="image/gif" ;;
            *) ct="application/octet-stream" ;;
        esac

        # Upload
        if npx wrangler r2 object put "cr-images/${r2_key}" \
            --file="$file" --content-type="$ct" --remote 2>/dev/null | grep -q "Upload complete"; then
            UPLOADED=$((UPLOADED + 1))
        else
            FAILED=$((FAILED + 1))
            echo "  FAIL: $r2_key"
        fi

        # Progress every 100 files
        DONE=$((UPLOADED + FAILED + SKIPPED))
        if [ $((DONE % 100)) -eq 0 ]; then
            echo "  Progress: $DONE/$TOTAL (uploaded: $UPLOADED, failed: $FAILED)"
        fi
    done
done

echo ""
echo "Done! Uploaded: $UPLOADED, Failed: $FAILED, Total: $TOTAL"
