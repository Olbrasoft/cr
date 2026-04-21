#!/bin/bash
# Loop: run gemma phase until no more pending rows remain.
# Safe to run while TMDB fetch is still filling the queue in parallel —
# each iteration picks up whatever new rows are ready.
#
# Required env:
#   STAGING_DATABASE_URL — libpq connection string for cr_staging (hosts the queue)
#   DATABASE_URL         — libpq connection string for cr_dev (apply target)
#   GEMINI_API_KEY_1..4  — forwarded to the Python child
# Optional env:
#   GEMMA_LOG            — path of the loop log (default: /tmp/backfill-gemma-loop.log)
#   GEMMA_DONE_MARKER    — path of the completion marker touched on success

set -euo pipefail
: "${STAGING_DATABASE_URL:?STAGING_DATABASE_URL must be set (same connection string the Python pipeline uses)}"
cd "$(dirname "$0")/.."

LOG="${GEMMA_LOG:-/tmp/backfill-gemma-loop.log}"
DONE_MARKER="${GEMMA_DONE_MARKER:-/tmp/backfill-gemma.done}"

is_int() { [[ "$1" =~ ^[0-9]+$ ]]; }

echo "[$(date -Iseconds)] gemma loop start" | tee -a "$LOG"

ITER=0
while true; do
  ITER=$((ITER+1))
  echo "[$(date -Iseconds)] iter=$ITER starting gemma pass" | tee -a "$LOG"
  # Don't let a single failed pass kill the loop — the Python script may fail
  # transiently (429, network blip) and the next iteration will pick up what's
  # still pending. RC is logged for postmortem.
  RC=0
  python3 scripts/backfill_prehrajto_gemma.py gemma >> "$LOG" 2>&1 || RC=$?
  echo "[$(date -Iseconds)] iter=$ITER gemma pass exit=$RC" | tee -a "$LOG"

  PENDING=$(psql "$STAGING_DATABASE_URL" -tAc "
    SELECT COUNT(*) FROM films_gemma_queue
     WHERE tmdb_fetched_at IS NOT NULL
       AND gemma_text IS NULL
       AND (tmdb_cs IS NOT NULL OR tmdb_en IS NOT NULL)" | tr -d '[:space:]')
  FETCH_PENDING=$(psql "$STAGING_DATABASE_URL" -tAc "
    SELECT COUNT(*) FROM films_gemma_queue WHERE tmdb_fetched_at IS NULL" | tr -d '[:space:]')

  if ! is_int "$PENDING" || ! is_int "$FETCH_PENDING"; then
    echo "[$(date -Iseconds)] ABORT: non-numeric counts from psql (pending='$PENDING' fetch='$FETCH_PENDING') — DB unreachable or auth failed" | tee -a "$LOG" >&2
    exit 2
  fi

  echo "[$(date -Iseconds)] iter=$ITER pending_gemma=$PENDING pending_fetch=$FETCH_PENDING" | tee -a "$LOG"

  if [ "$PENDING" -eq 0 ] && [ "$FETCH_PENDING" -eq 0 ]; then
    echo "[$(date -Iseconds)] ALL DONE — no more pending work" | tee -a "$LOG"
    break
  fi

  if [ "$PENDING" -eq 0 ] && [ "$FETCH_PENDING" -gt 0 ]; then
    echo "[$(date -Iseconds)] no gemma work but fetch still running — sleep 60" | tee -a "$LOG"
    sleep 60
  fi
done

# Final stats
psql "$STAGING_DATABASE_URL" -c "
  SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE gemma_text IS NOT NULL) AS gemma_done,
    COUNT(*) FILTER (WHERE gemma_error IS NOT NULL) AS gemma_err,
    COUNT(*) FILTER (WHERE tmdb_cs IS NULL AND tmdb_en IS NULL) AS no_source
  FROM films_gemma_queue" | tee -a "$LOG"

touch "$DONE_MARKER"
echo "[$(date -Iseconds)] loop exit" | tee -a "$LOG"
