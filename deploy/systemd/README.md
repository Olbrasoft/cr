# Systemd units (produkční VPS)

Dvě nezávislé noční úlohy:

| Unit | Čas | Účel | Admin přehled |
|------|-----|------|---------------|
| `cr-auto-import.timer` | 05:00 UTC | SK Torrent → films/series/tv_shows | `/admin/import/` |
| `cr-backup-db.timer`   | 03:00 UTC | `pg_dump` celé DB → Cloudflare R2 (10 dní retence) | `/admin/backups/` |

## Auto-import (issue #423)

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

---

## Auto-zálohy DB (task #97)

Denní `pg_dump` celé produkční DB → Cloudflare R2 bucket `cr-backups`.
Každá záloha je self-contained (custom format `-Fc` + gzip) — z jediné
zálohy lze obnovit celou DB přes `pg_restore`. Retenci 10 dní řeší
R2 lifecycle rule (bucket-level, mimo skript).

### Instalace / enable na VPS

```bash
# 1. Instaluj rclone (jednou)
ssh -p "$VPS_PORT" "root@$VPS_HOST" "apt-get install -y rclone"

# 2. Doplň /opt/cr/.env (jednou — credentials viz ~/Dokumenty/přístupy/cloudflare/r2-cr-db-backups.md):
#   R2_BACKUP_ACCESS_KEY_ID=...
#   R2_BACKUP_SECRET_ACCESS_KEY=...
#   R2_BACKUP_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com

# 3. Zkopíruj skript + unit files
scp -P "$VPS_PORT" scripts/backup-db.sh "root@$VPS_HOST:/opt/cr/scripts/"
scp -P "$VPS_PORT" deploy/systemd/cr-backup-db.{service,timer} \
    "root@$VPS_HOST:/etc/systemd/system/"
ssh -p "$VPS_PORT" "root@$VPS_HOST" "chmod +x /opt/cr/scripts/backup-db.sh"

# 4. Enable + start timer
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "systemctl daemon-reload && \
     systemctl enable --now cr-backup-db.timer && \
     systemctl list-timers cr-backup-db.timer"
```

### Ruční spuštění (test / mimořádná záloha)

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "systemctl start cr-backup-db.service"
# nebo skript rovnou (zapíše do backup_runs s trigger='manual'):
ssh -p "$VPS_PORT" "root@$VPS_HOST" "/opt/cr/scripts/backup-db.sh manual"
```

### Logy

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "tail -200 /var/log/cr-backup-db.log"
ssh -p "$VPS_PORT" "root@$VPS_HOST" "journalctl -u cr-backup-db.service --since today"
```

### Dashboard

Každý běh zapíše row do `backup_runs` viditelnou na
<https://ceskarepublika.wiki/admin/backups/>.

### Retention (Cloudflare R2 lifecycle)

Mimo skript — nastavit jednou v R2 dashboardu na bucketu `cr-backups`:
- Prefix: `auto/`
- Expire objects: 10 days after upload
