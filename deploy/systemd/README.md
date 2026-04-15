# Auto-import systemd unit

Daily cron for SK Torrent scanner. See issue #423.

Set `$VPS_HOST` and `$VPS_PORT` to the production SSH host/port (the exact
values live in `~/Dokumenty/přístupy/` and are intentionally kept out of
this repo so the repo stays reusable if Hetzner changes or the repo is
ever published).

## Install / enable on VPS

```bash
# Copy unit files
scp -P "$VPS_PORT" deploy/systemd/cr-auto-import.{service,timer} \
    "root@$VPS_HOST:/etc/systemd/system/"

# Install Python deps (once)
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "apt-get update && apt-get install -y python3-psycopg2 python3-requests python3-pil"

# Add to /opt/cr/.env (once):
#   TMDB_API_KEY=...
#   GEMINI_API_KEY=...        (production key, NOT the dev ones)
#   ADMIN_IMPORT_RUN_ENABLED=1
#   CR_REPO_ROOT=/opt/cr
# CZ_PROXY_URL + CZ_PROXY_KEY must already be set — the scanner routes
# SK Torrent traffic through the same ASP.NET proxy the Rust app uses
# because Hetzner ASNs are blocked by SK Torrent.

# Enable + start the timer
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "systemctl daemon-reload && \
     systemctl enable --now cr-auto-import.timer && \
     systemctl list-timers cr-auto-import.timer"
```

## Disable

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "systemctl disable --now cr-auto-import.timer"
```

## One-off run (for testing — also available via dashboard button)

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "systemctl start cr-auto-import.service"
```

## Logs

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "tail -200 /var/log/cr-auto-import.log"
# or via journalctl
ssh -p "$VPS_PORT" "root@$VPS_HOST" "journalctl -u cr-auto-import.service --since today"
```

## Dashboard

Every run writes a row to `import_runs` visible at
<https://ceskarepublika.wiki/admin/import/>.
