#!/bin/bash
# Periodic prehraj.to sitemap sync (#645, parent epic #642).
#
# Two modes:
#   full          — download all 487 sub-sitemaps, run importer with mark-dead.
#                   Used by the daily cron at 03:00 UTC.
#   incremental   — HEAD index.xml, only fetch sub-sitemaps whose ETag changed
#                   since last run. Used by the 6×daily cron.
#                   Skips mark-dead (partial coverage would mis-flag rows).
#
# Side effects:
#   - Writes downloaded sitemap files into $SITEMAP_DIR (default /var/cache/cr/prehrajto-sitemap/)
#   - Persists per-file ETags in $ETAG_DIR/etags.json for incremental runs
#   - Appends progress to $LOG_FILE (default /var/log/cr/prehrajto-sync.log)
#   - Calls scripts/import-prehrajto-uploads.py with $DATABASE_URL from env
#
# Required env:
#   DATABASE_URL  — postgres URL the importer connects to
#   MATCHES_CSV   — path to the IMDB-matches CSV the importer joins against
#                   (built once via the pilot pipeline; refresh policy is a
#                   separate concern outside this cron)
#
# Optional env:
#   SITEMAP_DIR   — default /var/cache/cr/prehrajto-sitemap/
#   ETAG_DIR      — default /var/cache/cr/prehrajto-sitemap/etags/
#   LOG_FILE      — default /var/log/cr/prehrajto-sync.log
#   IMPORTER      — default $(dirname "$0")/import-prehrajto-uploads.py

set -euo pipefail

MODE="${1:-}"
if [[ "$MODE" != "full" && "$MODE" != "incremental" ]]; then
    echo "Usage: $0 {full|incremental}" >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SITEMAP_DIR="${SITEMAP_DIR:-/var/cache/cr/prehrajto-sitemap}"
ETAG_DIR="${ETAG_DIR:-$SITEMAP_DIR/etags}"
ETAG_FILE="$ETAG_DIR/etags.json"
LOG_FILE="${LOG_FILE:-/var/log/cr/prehrajto-sync.log}"
IMPORTER="${IMPORTER:-$SCRIPT_DIR/import-prehrajto-uploads.py}"
INDEX_URL="https://prehraj.to/sitemap/index.xml"

if [[ -z "${DATABASE_URL:-}" ]]; then
    echo "ERROR: DATABASE_URL is required" >&2
    exit 2
fi
if [[ -z "${MATCHES_CSV:-}" ]]; then
    echo "ERROR: MATCHES_CSV path is required" >&2
    exit 2
fi
if [[ ! -f "$MATCHES_CSV" ]]; then
    echo "ERROR: MATCHES_CSV not found: $MATCHES_CSV" >&2
    exit 2
fi

mkdir -p "$SITEMAP_DIR" "$ETAG_DIR" "$(dirname "$LOG_FILE")"

log() {
    printf "[%s] [%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$MODE" "$*" | tee -a "$LOG_FILE"
}

log "starting (importer=$IMPORTER)"

# --- 1. Index.xml — list all sub-sitemap URLs --------------------------------
INDEX_FILE="$SITEMAP_DIR/index.xml"
log "fetching index.xml..."
if ! curl -fsS -o "$INDEX_FILE.tmp" -m 60 "$INDEX_URL"; then
    log "ERROR: failed to fetch $INDEX_URL"
    exit 1
fi
mv "$INDEX_FILE.tmp" "$INDEX_FILE"
SUB_URLS=$(grep -oE "https://prehrajto\.cz/sitemap/video-sitemap-[0-9]+\.xml" "$INDEX_FILE" | sort -u)
SUB_COUNT=$(echo "$SUB_URLS" | wc -l)
log "$SUB_COUNT sub-sitemaps listed in index"

# --- 2. Decide which sub-sitemaps to download -------------------------------
# Incremental: HEAD each sub-sitemap, compare ETag against $ETAG_FILE, fetch
# only changes. Full: download everything unconditionally so a stale or
# corrupt cache can't poison the importer's view.
declare -A OLD_ETAGS=()
if [[ -f "$ETAG_FILE" ]]; then
    while IFS=$'\t' read -r url etag; do
        OLD_ETAGS[$url]=$etag
    done < <(python3 -c "
import json,sys
try:
    d = json.load(open('$ETAG_FILE'))
    for k,v in d.items(): print(f'{k}\\t{v}')
except Exception:
    pass
")
fi

CHANGED_COUNT=0
TO_DOWNLOAD=()
declare -A NEW_ETAGS=()
while IFS= read -r url; do
    [[ -z "$url" ]] && continue
    if [[ "$MODE" == "full" ]]; then
        TO_DOWNLOAD+=("$url")
        continue
    fi
    # Incremental: HEAD to get ETag, skip if unchanged
    etag=$(curl -sIfm 30 "$url" | grep -i "^etag:" | tr -d '\r' | awk '{print $2}' || true)
    if [[ -z "$etag" ]]; then
        # No ETag — treat as changed to be safe
        TO_DOWNLOAD+=("$url")
        continue
    fi
    NEW_ETAGS[$url]=$etag
    if [[ "${OLD_ETAGS[$url]:-}" != "$etag" ]]; then
        TO_DOWNLOAD+=("$url")
        ((CHANGED_COUNT++))
    fi
done <<< "$SUB_URLS"

log "$( [[ "$MODE" == full ]] && echo "${#TO_DOWNLOAD[@]} files to download (full)" \
                              || echo "$CHANGED_COUNT of $SUB_COUNT changed (incremental)" )"

# --- 3. Download (parallel curls, capped at 8 concurrent) -------------------
if (( ${#TO_DOWNLOAD[@]} > 0 )); then
    printf "%s\n" "${TO_DOWNLOAD[@]}" | xargs -P 8 -I {} sh -c '
        url="$1"
        out_dir="$2"
        fname=$(basename "$url")
        if curl -fsSL -o "$out_dir/$fname.tmp" -m 300 "$url"; then
            mv "$out_dir/$fname.tmp" "$out_dir/$fname"
        else
            echo "WARN: failed $url" >&2
            rm -f "$out_dir/$fname.tmp"
        fi
    ' _ {} "$SITEMAP_DIR"
fi
log "downloads complete"

# Persist ETags for next incremental run (full runs also update so subsequent
# incrementals start from a correct baseline).
if (( ${#NEW_ETAGS[@]} > 0 )); then
    python3 -c "
import json
existing = {}
try:
    existing = json.load(open('$ETAG_FILE'))
except Exception:
    pass
$(for u in "${!NEW_ETAGS[@]}"; do
    printf "existing[%q] = %q\n" "$u" "${NEW_ETAGS[$u]}"
done)
json.dump(existing, open('$ETAG_FILE','w'), indent=2, sort_keys=True)
"
fi

# --- 4. Run the importer ----------------------------------------------------
IMPORTER_ARGS=(--sitemap-dir "$SITEMAP_DIR" --matches "$MATCHES_CSV")
if [[ "$MODE" == "incremental" ]]; then
    # Partial sitemap coverage — disable mark-dead; the daily full run is
    # the authoritative source for is_alive transitions.
    IMPORTER_ARGS+=(--no-mark-dead)
fi
log "running importer: ${IMPORTER_ARGS[*]}"
if python3 "$IMPORTER" "${IMPORTER_ARGS[@]}" >> "$LOG_FILE" 2>&1; then
    log "importer OK"
else
    rc=$?
    log "ERROR: importer exited $rc"
    exit "$rc"
fi

log "done"
