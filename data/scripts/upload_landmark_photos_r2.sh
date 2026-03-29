#!/bin/bash
# Upload landmark photos to Cloudflare R2 with SEO-friendly names
# Format: landmarks/{slug}-{id}.webp
# Uses production DB IDs for naming (different from dev!)

set -e

BUCKET="cr-images"
IMG_DIR="/home/jirka/Olbrasoft/cr/data/images/landmarks"
LOG_OK="/tmp/r2_landmarks_ok.log"
LOG_ERR="/tmp/r2_landmarks_err.log"
LOG_SKIP="/tmp/r2_landmarks_skip.log"

# Use production DB for correct IDs
DB_HOST="${DB_HOST:-46.225.101.253}"
DB_PORT="${DB_PORT:-2222}"

echo "Fetching landmark slug+id mapping from production DB..."

# Get mapping: catalog_id -> slug, id from production
ssh -p $DB_PORT root@$DB_HOST "docker exec cr-db-1 psql -U cr -d cr -t -A -F'|' -c \"
SELECT l.npu_catalog_id, l.slug, l.id
FROM landmarks l
WHERE l.npu_catalog_id IS NOT NULL
ORDER BY l.id
\"" > /tmp/landmark_mapping.txt

TOTAL=$(wc -l < /tmp/landmark_mapping.txt)
echo "Total landmarks with catalog_id: $TOTAL"

UPLOADED=0
SKIPPED=0
FAILED=0
COUNT=0

upload_one() {
    local CID="$1"
    local SLUG="$2"
    local LID="$3"
    local SRC="${IMG_DIR}/${CID}.webp"
    local KEY="landmarks/${SLUG}-${LID}.webp"

    if [ ! -f "$SRC" ]; then
        return 1  # no photo
    fi

    # Upload to R2
    npx wrangler r2 object put "${BUCKET}/${KEY}" \
        --file="$SRC" \
        --content-type="image/webp" \
        --remote 2>&1 | grep -q "Upload complete"
}

while IFS='|' read -r CID SLUG LID; do
    [ -z "$CID" ] && continue
    COUNT=$((COUNT + 1))

    SRC="${IMG_DIR}/${CID}.webp"
    KEY="landmarks/${SLUG}-${LID}.webp"

    if [ ! -f "$SRC" ]; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Upload
    if npx wrangler r2 object put "${BUCKET}/${KEY}" \
        --file="$SRC" \
        --content-type="image/webp" \
        --remote 2>&1 | grep -q "Upload complete"; then
        UPLOADED=$((UPLOADED + 1))
        echo "$KEY" >> "$LOG_OK"
    else
        FAILED=$((FAILED + 1))
        echo "$KEY" >> "$LOG_ERR"
    fi

    if [ $((COUNT % 100)) -eq 0 ]; then
        echo "  Progress: $COUNT/$TOTAL (uploaded: $UPLOADED, skipped: $SKIPPED, failed: $FAILED)"
    fi

done < /tmp/landmark_mapping.txt

echo ""
echo "Done! Uploaded: $UPLOADED, Skipped: $SKIPPED, Failed: $FAILED, Total: $TOTAL"
