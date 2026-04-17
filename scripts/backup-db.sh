#!/bin/bash
# ------------------------------------------------------------
# backup-db.sh — denní full backup produkční PostgreSQL DB na Cloudflare R2
#
# Spouští se systemd timerem cr-backup-db.timer každý den v 03:00 UTC.
# Ruční běh: `scripts/backup-db.sh manual`
#
# Krok za krokem:
#   1. INSERT row do backup_runs se status='running'
#   2. docker exec db pg_dump -Fc cr | gzip > /tmp/…
#   3. rclone copyto /tmp/… cr-r2-backup:cr-backups/auto/cr_prod_YYYY-MM-DD.dump.gz
#   4. UPDATE backup_runs SET status='ok', size, filename, finished_at
#   5. Cleanup tmp souboru
#
# Retence starších záloh řeší R2 lifecycle rule (10 dní na prefix auto/)
# — tento skript pouze uploaduje, nikdy neřízeně nemaže.
#
# Při jakékoli chybě uprostřed pipeline (pg_dump selže, rclone selže,
# ztráta sítě) se row v backup_runs označí jako status='error' s err. hláškou
# a skript skončí s non-zero exit kódem. Systemd pak pošle alert dle potřeby.
#
# Vyžaduje:
#   - docker compose s běžícím kontejnerem `db` (postgres:16)
#   - rclone (apt install rclone)
#   - env proměnné v /opt/cr/.env:
#       R2_BACKUP_ACCESS_KEY_ID
#       R2_BACKUP_SECRET_ACCESS_KEY
#       R2_BACKUP_ENDPOINT  (https://<account>.r2.cloudflarestorage.com)
# ------------------------------------------------------------

set -euo pipefail

# --- Config ---
COMPOSE_FILE="${COMPOSE_FILE:-/opt/cr/docker-compose.yml}"
TRIGGER="${1:-auto}"   # 'auto' (timer) nebo 'manual' (operator)
TODAY=$(date -u +%F)
FILENAME="cr_prod_${TODAY}.dump.gz"
R2_KEY="auto/${FILENAME}"
TMP="/tmp/cr_backup_$$.dump.gz"

# rclone remote jméno (přes env var — žádný rclone.conf na disku není potřeba).
# Viz sekci "rclone env mapping" v deploy/systemd/cr-backup-db.service.
R2_REMOTE="cr_r2_backup:cr-backups"

log() { echo "[$(date -u +%H:%M:%S)] $*"; }
die() { echo "[FAIL $(date -u +%H:%M:%S)] $*" >&2; exit 1; }

# --- Preflight ---
command -v docker >/dev/null || die "docker not found"
command -v rclone >/dev/null || die "rclone not found (apt install rclone)"
[ -f "$COMPOSE_FILE" ] || die "compose file $COMPOSE_FILE missing"
[ -n "${R2_BACKUP_ACCESS_KEY_ID:-}" ] || die "R2_BACKUP_ACCESS_KEY_ID not set"
[ -n "${R2_BACKUP_SECRET_ACCESS_KEY:-}" ] || die "R2_BACKUP_SECRET_ACCESS_KEY not set"
[ -n "${R2_BACKUP_ENDPOINT:-}" ] || die "R2_BACKUP_ENDPOINT not set"

# --- Helpers ---
psql_exec() {
    # Runs a single SQL statement inside the db container. Stdin can carry
    # multi-line SQL. Uses -v ON_ERROR_STOP=1 so any SQL error exits non-zero.
    docker compose -f "$COMPOSE_FILE" exec -T db \
        psql -U cr -d cr -v ON_ERROR_STOP=1 -q -t -A "$@"
}

cleanup() {
    rm -f "$TMP"
}
trap cleanup EXIT

# --- 1. Create running row, capture ID ---
RUN_ID=$(psql_exec -c "INSERT INTO backup_runs (trigger) VALUES ('$TRIGGER') RETURNING id;" | tr -d '[:space:]')
[ -n "$RUN_ID" ] || die "failed to INSERT into backup_runs"
log "backup_runs #$RUN_ID created (trigger=$TRIGGER)"

# From here on, on any error, mark the row as failed before bailing out.
mark_failed() {
    local msg="$1"
    local escaped
    escaped=$(printf '%s' "$msg" | sed "s/'/''/g")
    # Best-effort — if even this fails (DB down), the row stays 'running'
    # forever, which the admin banner will surface as alarm.
    psql_exec -c "UPDATE backup_runs SET status='error', finished_at=NOW(), error_message='$escaped' WHERE id=$RUN_ID;" || true
}

# --- 2. pg_dump + gzip ---
log "pg_dump starting..."
if ! docker compose -f "$COMPOSE_FILE" exec -T db \
        pg_dump -Fc -U cr cr 2>/tmp/cr_backup_pgdump_err.$$ | gzip > "$TMP"; then
    ERR=$(head -c 500 "/tmp/cr_backup_pgdump_err.$$" 2>/dev/null || echo "pg_dump pipeline failed")
    rm -f "/tmp/cr_backup_pgdump_err.$$"
    mark_failed "pg_dump failed: $ERR"
    die "$ERR"
fi
rm -f "/tmp/cr_backup_pgdump_err.$$"

SIZE=$(stat -c %s "$TMP")
[ "$SIZE" -gt 1024 ] || { mark_failed "pg_dump produced only $SIZE bytes"; die "dump suspiciously small: $SIZE bytes"; }
log "pg_dump done — $SIZE bytes"

# --- 3. Upload to R2 ---
# --s3-no-check-bucket: R2 token je scoped jen na bucket `cr-backups` a nemá
# ListBuckets/CreateBucket právo. Bez tohoto flagu rclone volá HeadBucket nebo
# CreateBucket před uploadem a dostane 403 AccessDenied. Náš bucket existuje,
# takže sanity check prostě přeskočíme.
log "uploading to R2 ($R2_REMOTE/$R2_KEY)..."
if ! rclone copyto --s3-no-check-bucket "$TMP" "$R2_REMOTE/$R2_KEY" 2>/tmp/cr_backup_rclone_err.$$; then
    ERR=$(head -c 500 "/tmp/cr_backup_rclone_err.$$" 2>/dev/null || echo "rclone copyto failed")
    rm -f "/tmp/cr_backup_rclone_err.$$"
    mark_failed "rclone upload failed: $ERR"
    die "$ERR"
fi
rm -f "/tmp/cr_backup_rclone_err.$$"
log "upload done"

# --- 4. Mark as ok ---
psql_exec -c "UPDATE backup_runs SET status='ok', finished_at=NOW(), size_bytes=$SIZE, dump_filename='$R2_KEY' WHERE id=$RUN_ID;" >/dev/null
log "backup_runs #$RUN_ID marked ok"
log "Done: s3://cr-backups/$R2_KEY ($SIZE bytes)"
