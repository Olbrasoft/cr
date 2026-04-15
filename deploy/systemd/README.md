# Auto-import systemd unit

Daily cron for SK Torrent scanner. See issue #413.

## Install / enable on VPS

```bash
# Copy unit files
scp -P 2222 deploy/systemd/cr-auto-import.{service,timer} \
    root@46.225.101.253:/etc/systemd/system/

# Install Python deps (once)
ssh -p 2222 root@46.225.101.253 \
    "apt-get update && apt-get install -y python3-pip && \
     pip3 install --break-system-packages psycopg2-binary requests Pillow"

# Add to /opt/cr/.env (once):
#   TMDB_API_KEY=...
#   GEMINI_API_KEY=...        (production key, NOT the dev ones)

# Enable + start the timer
ssh -p 2222 root@46.225.101.253 \
    "systemctl daemon-reload && \
     systemctl enable --now cr-auto-import.timer && \
     systemctl list-timers cr-auto-import.timer"
```

## Disable

```bash
ssh -p 2222 root@46.225.101.253 "systemctl disable --now cr-auto-import.timer"
```

## One-off run (for testing — also available via dashboard button)

```bash
ssh -p 2222 root@46.225.101.253 "systemctl start cr-auto-import.service"
```

## Logs

```bash
ssh -p 2222 root@46.225.101.253 "tail -200 /var/log/cr-auto-import.log"
# or via journalctl
ssh -p 2222 root@46.225.101.253 "journalctl -u cr-auto-import.service --since today"
```

## Dashboard

Every run writes a row to `import_runs` visible at
<https://ceskarepublika.wiki/admin/import/>.
