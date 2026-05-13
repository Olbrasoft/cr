#!/usr/bin/env bash
# Deploy cr-web to production in one command.
#
# Steps:
#   0. Drift check  — dry-run rsync of scripts/ so we see what changed since
#                     the last deploy (warns if a previous deploy was incomplete)
#   1. Cross-compile cr-web for aarch64-unknown-linux-musl with cargo zigbuild
#   2. rsync scripts/ to /opt/cr/scripts/ (excludes __pycache__, --delete) —
#      done BEFORE the container restart so the new scripts are already in
#      place if the web container or one of the timer-driven importers fires
#      during the restart window.
#   3. docker compose stop web → scp binary to /opt/cr/cr-web-bin →
#      docker compose start web. Stopping the container is necessary because:
#        - plain scp over /opt/cr/cr-web-bin fails with ETXTBSY (kernel
#          refuses to overwrite a running executable),
#        - `docker cp` onto /app/cr-web fails with "device or resource busy"
#          because /app/cr-web inside the container is a bind mount of
#          /opt/cr/cr-web-bin (per the prod-only compose edit).
#      Downtime is a few seconds. The web container's restart policy is
#      `unless-stopped` so the explicit start in step 3c is required.
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

# 0. Drift check — dry-run rsync, count files that would change. Same exclude
#    list as the real rsync in step 3 so the warning is honest.
echo "==> Checking scripts/ drift against ${VPS_HOST}:${REMOTE_SCRIPTS} ..."
drift_output=$(rsync -an --delete --itemize-changes \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  -e "$rsync_ssh" \
  scripts/ "${VPS_USER}@${VPS_HOST}:${REMOTE_SCRIPTS}" \
  | grep -v '^$' || true)
drift_count=$(echo "$drift_output" | grep -cv '^\s*$' || true)
if [ "$drift_count" -gt 0 ]; then
  echo "    $drift_count file(s) differ — the rsync in step 3 will bring them in sync:"
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

# 3. Stop web → replace binary on host → start web. Stopping is required
#    because /opt/cr/cr-web-bin is bind-mounted into a running container
#    and neither scp (ETXTBSY) nor docker cp (device-busy on bind mount)
#    can overwrite it while the container runs.
echo "==> Stopping ${COMPOSE_SERVICE} container ..."
"${ssh_cmd[@]}" "docker compose -f ${COMPOSE_FILE} stop ${COMPOSE_SERVICE}"

echo "==> Uploading binary to ${REMOTE_BIN} ..."
scp -P "$VPS_PORT" "$bin_path" "${VPS_USER}@${VPS_HOST}:${REMOTE_BIN}"

echo "==> Starting ${COMPOSE_SERVICE} container ..."
"${ssh_cmd[@]}" "docker compose -f ${COMPOSE_FILE} start ${COMPOSE_SERVICE}"

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
