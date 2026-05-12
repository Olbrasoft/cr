# Systemd units (produkční VPS)

Šest nezávislých nočních úloh:

| Unit | Čas | Účel | Admin přehled |
|------|-----|------|---------------|
| `cr-backup-db.timer`         | 03:00 UTC | `pg_dump` celé DB → Cloudflare R2 (30 dní retence) | `/admin/backups/` |
| `cr-prehrajto-sync.timer`    | 04:00 UTC | prehraj.to sitemap → DB + mark-dead rotated IDs | (TODO) |
| `cr-imdb-rating-sync.timer`  | 04:30 UTC | IMDb datasets TSV → `imdb_rating` + `imdb_votes` na films/series/tv_shows | (logs) |
| `cr-tmdb-rating-sync.timer`  | 04:45 UTC | TMDB `/movie/changes` + `/tv/changes` → `tmdb_rating` + `tmdb_vote_count` (inkrementální) | (logs) |
| `cr-auto-import.timer`       | 05:00 UTC | SK Torrent → films/series/tv_shows | `/admin/import/` |
| `cr-llm-resolver.timer`      | 06:30 UTC | LLM resolver: prehraj.to unmatched clusters → TMDB ID (Gemma + TMDB API) | `/admin/prehrajto/unmatched` |

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

## Prehraj.to sitemap sync (issue #646, epic #642)

Daily sync prehraj.to XML sitemap → `video_sources(prehrajto)`. Two-step
service (sequential ExecStart=):

1. `scripts/sync-prehrajto-sitemap.py --mode full --keep-days 2`
   downloads all 487 sub-sitemaps in parallel, prunes >2-day-old files
   (~15 GB per snapshot, ~30 GB peak during overlap).
2. `scripts/import-prehrajto-uploads.py --from-films-table` matches
   sitemap clusters against the `films` table, upserts uploads, and
   runs the end-of-run mark-dead pass that flips rotated upload_ids
   to `is_alive=FALSE`.

Runs at 04:00 UTC — between the 03:00 backup and the 05:00 SK Torrent
import, so a bad sync can be rolled back from that morning's snapshot.

### Install / enable on VPS

```bash
# Copy unit files
scp -P "$VPS_PORT" deploy/systemd/cr-prehrajto-sync.{service,timer} \
    "root@$VPS_HOST:/etc/systemd/system/"

# Copy scripts (rsync the whole scripts/ dir is the simplest)
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "mkdir -p /opt/cr/scripts /var/cache/cr/prehrajto-sitemap"
scp -P "$VPS_PORT" scripts/sync-prehrajto-sitemap.py \
                   scripts/import-prehrajto-uploads.py \
                   scripts/video_sources_helper.py \
    "root@$VPS_HOST:/opt/cr/scripts/"

# Enable + smoke-run
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "systemctl daemon-reload && \
     systemctl enable --now cr-prehrajto-sync.timer && \
     systemctl start cr-prehrajto-sync.service && \
     journalctl -u cr-prehrajto-sync.service -f"
```

### Smoke checks after first run

```bash
# Disk usage (~15 GB, prune keeps it bounded)
ssh -p "$VPS_PORT" "root@$VPS_HOST" "du -sh /var/cache/cr/prehrajto-sitemap"

# DB freshness
ssh -p "$VPS_PORT" "root@$VPS_HOST" "docker exec cr-db-1 psql -U cr -d cr -c \\
  \"SELECT COUNT(*) FILTER (WHERE is_alive) AS alive,
           COUNT(*) FILTER (WHERE NOT is_alive) AS dead,
           MAX(updated_at) AS most_recent
     FROM video_sources WHERE provider_id=2;\""

# Spasitel (test case from #642)
ssh -p "$VPS_PORT" "root@$VPS_HOST" "docker exec cr-db-1 psql -U cr -d cr -c \\
  \"SELECT external_id, is_alive FROM video_sources
     WHERE film_id=(SELECT id FROM films WHERE slug='spasitel') AND provider_id=2
     ORDER BY is_alive DESC, external_id;\""
```

## Auto-zálohy DB (task #97)

Denní `pg_dump` celé produkční DB → Cloudflare R2 bucket `cr-backups`.
Každá záloha je self-contained (custom format `-Fc` + gzip) — z jediné
zálohy lze obnovit celou DB přes `pg_restore`. Retenci 30 dní řeší
R2 lifecycle rule (bucket-level, mimo skript) — stejné okno jako
„posledních 30 běhů" v `/admin/backups/` UI.

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

Skript loguje do journald (ne do souboru — žádná log-rotate config potřeba).

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "journalctl -u cr-backup-db.service --since today"
ssh -p "$VPS_PORT" "root@$VPS_HOST" "journalctl -u cr-backup-db.service -n 200"
```

### Dashboard

Každý běh zapíše row do `backup_runs` viditelnou na
<https://ceskarepublika.wiki/admin/backups/>.

### Retention (Cloudflare R2 lifecycle)

Mimo skript — nastavit jednou v R2 dashboardu na bucketu `cr-backups`:
- Prefix: `auto/`
- Expire objects: 30 days after upload

---

## IMDb rating sync (issue #690, parent #588)

Daily refresh of `imdb_rating` + `imdb_votes` on `films`, `series` and
`tv_shows` from the public IMDb datasets TSV
(<https://datasets.imdbws.com/title.ratings.tsv.gz>). The TSV is ~8 MB
compressed, contains ~1.5 M titles, and IMDb republishes it once a day
around 00:30 UTC. We pull at 04:30 UTC — well after that, but ahead of
the 05:00 SK Torrent import.

Conditional GET via `Last-Modified` keeps re-runs in the same day cheap:
when IMDb hasn't republished, the script logs `TSV unchanged` and falls
back to the cached copy under `data/imdb-cache/`. Total runtime ~3 s
(load IMDb IDs from DB → stream-parse 1.5 M TSV rows → batched UPDATE).

### Install / enable on VPS

```bash
# Copy unit files + script
scp -P "$VPS_PORT" deploy/systemd/cr-imdb-rating-sync.{service,timer} \
    "root@$VPS_HOST:/etc/systemd/system/"
scp -P "$VPS_PORT" scripts/sync-imdb-ratings.py \
    "root@$VPS_HOST:/opt/cr/scripts/"

# Enable + smoke-run
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "systemctl daemon-reload && \
     systemctl enable --now cr-imdb-rating-sync.timer && \
     systemctl start cr-imdb-rating-sync.service && \
     tail -f /var/log/cr-imdb-rating-sync.log"
```

### Logs

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "tail -200 /var/log/cr-imdb-rating-sync.log"
```

---

## TMDB rating sync (issue #591, parent #588)

Daily incremental refresh of `tmdb_rating` + `tmdb_vote_count` on
`films`, `series` and `tv_shows`. Unlike IMDb, TMDB has no public batch
dataset with rating values — the only practical refresh path is the
`/movie/changes` and `/tv/changes` endpoints, which return ID lists of
titles that have changed in the last 24 h. We intersect those with our
`tmdb_id` set and re-fetch only those rows via `/movie/{id}` / `/tv/{id}`
to read `vote_average` + `vote_count`.

Typical run: ~10k changed IDs in window across all of TMDB, ~1.5k of
which match our DB. With 8 worker threads + 429 retries the whole pass
finishes in ~30–90 s. Runs at 04:45 UTC, between the 04:30 IMDb tick
and the 05:00 SK Torrent import.

### Install / enable on VPS

```bash
# Copy unit files + script
scp -P "$VPS_PORT" deploy/systemd/cr-tmdb-rating-sync.{service,timer} \
    "root@$VPS_HOST:/etc/systemd/system/"
scp -P "$VPS_PORT" scripts/sync-tmdb-ratings.py \
    "root@$VPS_HOST:/opt/cr/scripts/"

# TMDB_API_KEY must already be set in /opt/cr/.env (used by auto-import too).

# Enable + smoke-run
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "systemctl daemon-reload && \
     systemctl enable --now cr-tmdb-rating-sync.timer && \
     systemctl start cr-tmdb-rating-sync.service && \
     tail -f /var/log/cr-tmdb-rating-sync.log"
```

### Manual catch-up

```bash
# Pull the last 7 days instead of the default 24 h — useful after an outage.
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "cd /opt/cr && set -a && . ./.env && \
     export DATABASE_URL=\${DATABASE_URL//@db:/@127.0.0.1:} && set +a && \
     python3 scripts/sync-tmdb-ratings.py --days 7"
```

### Logs

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "tail -200 /var/log/cr-tmdb-rating-sync.log"
```

---

## LLM resolver (issue #652)

Daily resolver for prehraj.to unmatched clusters using Gemma 3 27B
(via Google AI Studio free tier) + TMDB API. Reads
`prehrajto_unmatched_clusters` rows where the regex importer couldn't
match the upload string against the `films` table — extracts a
canonical title with Gemma, resolves to a stable TMDB ID, and writes
either `resolved_film_id` (existing film) or `resolved_tmdb_id`
(NEW_TMDB candidate, awaiting #652 auto-import).

Capped at `--limit 200 --min-uploads 2` per run — drains the ~10k
backlog over weeks, not days, so a buggy resolver iteration can't burn
the whole backlog before false positives surface. Skip-window
(`--retry-after-days 7`) avoids paying Gemma quota for the same
already-attempted clusters daily.

### Install / enable on VPS

```bash
# Copy unit files + script
scp -P "$VPS_PORT" deploy/systemd/cr-llm-resolver.{service,timer} \
    "root@$VPS_HOST:/etc/systemd/system/"
scp -P "$VPS_PORT" scripts/resolve-unmatched-via-llm.py \
    "root@$VPS_HOST:/opt/cr/scripts/"

# Required env vars in /opt/cr/.env (already present from auto-import):
#   DATABASE_URL=postgres://cr:...@db:5432/cr
#   GEMINI_API_KEY=...
#   TMDB_API_KEY=...

# Enable + smoke-run
ssh -p "$VPS_PORT" "root@$VPS_HOST" \
    "systemctl daemon-reload && \
     systemctl enable --now cr-llm-resolver.timer && \
     systemctl start cr-llm-resolver.service && \
     tail -f /var/log/cr-llm-resolver.log"
```

### Logs

```bash
ssh -p "$VPS_PORT" "root@$VPS_HOST" "tail -300 /var/log/cr-llm-resolver.log"
```

### Dashboard

Resolution outcomes are written back to `prehrajto_unmatched_clusters`:
- `resolved_film_id IS NOT NULL` — cluster mapped to existing film
- `resolved_tmdb_id IS NOT NULL AND resolved_film_id IS NULL` —
  candidate awaiting auto-import (separate pipeline #652)
- `last_failure_reason` — `tmdb_no_hit`, `tmdb_runtime_mismatch`,
  `tmdb_title_mismatch`, `llm_not_film`, `llm_no_title`, `llm_gemini_failed`

Visible at <https://ceskarepublika.wiki/admin/prehrajto/unmatched>.
