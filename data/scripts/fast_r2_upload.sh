#!/bin/bash
# Fast parallel upload to Cloudflare R2 using wrangler with GNU parallel.
# Much faster than sequential wrangler calls.

set -e
cd "$(dirname "$0")/../.."

WORKER_DIR="workers/img-proxy"
UPLOADED=0
FAILED=0

upload_file() {
    local file="$1"
    local base_dir="$2"

    # Get relative path from base_dir
    local rel="${file#$base_dir/}"
    local r2_key="$rel"
    local fname=$(basename "$file")

    # Content type
    case "$fname" in
        *.svg) ct="image/svg+xml" ;;
        *.webp) ct="image/webp" ;;
        *.png) ct="image/png" ;;
        *.jpg|*.jpeg) ct="image/jpeg" ;;
        *.gif) ct="image/gif" ;;
        *.tif) ct="image/tiff" ;;
        *) ct="application/octet-stream" ;;
    esac

    if npx wrangler r2 object put "cr-images/${r2_key}" \
        --file="$file" --content-type="$ct" --remote 2>/dev/null | grep -q "Upload complete"; then
        echo "OK: $r2_key"
    else
        echo "FAIL: $r2_key" >&2
    fi
}

export -f upload_file

IMAGE_DIR="data/images"
TOTAL=$(find "$IMAGE_DIR" -type f | wc -l)
echo "Uploading $TOTAL files to R2..."
echo "Using $(nproc) parallel workers"

cd "$WORKER_DIR"

find "../../$IMAGE_DIR" -type f | \
    parallel -j 4 --bar upload_file {} "../../$IMAGE_DIR" 2>/tmp/r2_upload_errors.log | \
    tee /tmp/r2_upload_ok.log | wc -l

echo ""
echo "Uploaded: $(wc -l < /tmp/r2_upload_ok.log)"
echo "Failed: $(wc -l < /tmp/r2_upload_errors.log)"
