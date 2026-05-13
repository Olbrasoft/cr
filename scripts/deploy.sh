#!/usr/bin/env bash
# Deploy cr-web to production in one command.
#
# Steps:
#   0. Drift check  — dry-run rsync of scripts/ so we see what changed since
#                     the last deploy (warns if a previous deploy was incomplete)
#   1. Cross-compile cr-web for aarch64-unknown-linux-musl with cargo zigbuild
#   2. rsync scripts/ to /opt/cr/scripts/ (excludes __pycache__, --delete) —
#      done BEFORE the container restart so the new scripts are already in
#      place if a timer-driven importer fires during the restart window.
#   3. docker compose stop web → scp binary to /opt/cr/cr-web-bin →
#      docker compose start web. Stopping the container is necessary because:
#        - plain scp over /opt/cr/cr-web-bin fails with ETXTBSY (kernel
#          refuses to overwrite a running executable),
#        - `docker cp` onto /app/cr-web fails with "device or resource busy"
#          because /app/cr-web inside the container is a bind mount of
#          /opt/cr/cr-web-bin (per the prod-only compose edit).
#      A trap covers the gap: if anything between `stop` and the final
#      `start` fails, we attempt to restart the container so prod doesn't
#      stay down with an old binary still on disk.
#   4. curl /health and wait up to 10s for a 200 response

set -euo pipefail

VPS_HOST="46.225.101.253"
VPS_PORT="2222"
VPS_USER="root"
TARGET="aarch64-unknown-linux-musl"
PROD_URL="https://ceskarepublika.wiki"
COMPOSE_FILE="/opt/cr/docker-compose.yml"
REMOTE_BIN="/opt/cr/cr-web-bin"
COMPOSE_SERVICE="web"
REMOTE_SCRIPTS="/opt/cr/scripts/"

cd "$(dirname "$0")/.."  # project root

ssh_cmd=(ssh -p "$VPS_PORT" "${VPS_USER}@${VPS_HOST}")
rsync_ssh="ssh -p $VPS_PORT"

# 0. Drift check — dry-run rsync, count files that would change. Mirror the
#    real sync flags from step 2 (sans -v/-z) so the warning is honest.
echo "==> Checking scripts/ drift against ${VPS_HOST}:${REMOTE_SCRIPTS} ..."
drift_output=$(rsync --archive --dry-run --delete --itemize-changes \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  -e "$rsync_ssh" \
  scripts/ "${VPS_USER}@${VPS_HOST}:${REMOTE_SCRIPTS}" \
  | grep -v '^$' || true)
if [ -z "$drift_output" ]; then
  drift_count=0
else
  drift_count=$(printf '%s\n' "$drift_output" | wc -l | tr -d '[:space:]')
fi
if [ "$drift_count" -gt 0 ]; then
  echo "    $drift_count file(s) differ — the rsync in step 2 will bring them in sync:"
  echo "$drift_output" | sed 's/^/      /'
else
  echo "    scripts/ is already in sync."
fi

# 1. Cross-compile
echo "==> Cross-compiling cr-web for $TARGET ..."
SQLX_OFFLINE=true cargo zigbuild --release --target "$TARGET" -p cr-web

bin_path="target/$TARGET/release/cr-web"
if [ ! -f "$bin_path" ]; then
  echo "ERROR: build did not produce $bin_path" >&2
  exit 1
fi

# 2. Sync scripts/ FIRST (before the container goes down) so timer-driven
#    importers that fire during our restart window already see the new code.
#    --delete so removed files disappear from the server.
echo "==> Syncing scripts/ ..."
rsync -avz --delete \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  -e "$rsync_ssh" \
  scripts/ "${VPS_USER}@${VPS_HOST}:${REMOTE_SCRIPTS}"

# 3. Stop web → replace binary on host → start web.
#    A trap covers the gap so a mid-deploy failure (e.g. scp interrupted)
#    doesn't leave the container stopped and the site offline.
container_stopped=0
restart_on_failure() {
  local rc=$?
  if [ "$rc" -ne 0 ] && [ "$container_stopped" = "1" ]; then
    echo "ERROR mid-deploy (rc=$rc) — restarting ${COMPOSE_SERVICE} to recover prod ..." >&2
    "${ssh_cmd[@]}" "docker compose -f ${COMPOSE_FILE} start ${COMPOSE_SERVICE}" >&2 || \
      echo "WARN: recovery start also failed; investigate manually." >&2
  fi
}
trap restart_on_failure EXIT

echo "==> Stopping ${COMPOSE_SERVICE} container ..."
"${ssh_cmd[@]}" "docker compose -f ${COMPOSE_FILE} stop ${COMPOSE_SERVICE}"
container_stopped=1

echo "==> Uploading binary to ${REMOTE_BIN} ..."
scp -P "$VPS_PORT" "$bin_path" "${VPS_USER}@${VPS_HOST}:${REMOTE_BIN}"

echo "==> Starting ${COMPOSE_SERVICE} container ..."
"${ssh_cmd[@]}" "docker compose -f ${COMPOSE_FILE} start ${COMPOSE_SERVICE}"
container_stopped=0

# 4. Health check — poll /health until 200 or 10s deadline.
echo "==> Waiting for /health ..."
for i in $(seq 1 10); do
  sleep 1
  code=$(curl -s -o /dev/null -w "%{http_code}" "${PROD_URL}/health" || echo "000")
  if [ "$code" = "200" ]; then
    echo "==> Healthy after ${i}s. Deploy done."
    exit 0
  fi
done

echo "ERROR: /health did not return 200 within 10s." >&2
echo "       Check logs: ssh -p $VPS_PORT ${VPS_USER}@${VPS_HOST} docker logs --tail 50 cr-web-1" >&2
exit 1
