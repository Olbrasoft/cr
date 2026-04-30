# Prehraj.to sitemap sync — operations guide

Runs the prehraj.to sitemap importer on a periodic schedule so DB stays
close to live. Replaces the search-then-video flow that today's epic #631
attempted (see #642 for the pivot reversal).

## Components

- `scripts/cron-prehrajto-sync.sh` — wrapper that downloads sitemap files
  and invokes the importer.
- `scripts/import-prehrajto-uploads.py` — actual import logic (#644 added
  the mark-dead pass that flips rotated upload_ids to `is_alive=FALSE`).

## Modes

| Mode | When | What it does |
|---|---|---|
| `full` | daily 03:00 UTC | Downloads all 487 sub-sitemaps, runs importer with mark-dead. Authoritative is_alive snapshot. |
| `incremental` | every 4h (00, 04, 08, 12, 16, 20 UTC) | HEADs each sub-sitemap, downloads only the ones whose ETag changed, runs importer with `--no-mark-dead` (partial coverage would mis-flag rows). |

## Required environment

```bash
DATABASE_URL=postgres://cr:cr_secret@localhost:5432/cr
MATCHES_CSV=/opt/cr/data/prehrajto-matches.csv
```

## Optional environment (with defaults)

```bash
SITEMAP_DIR=/var/cache/cr/prehrajto-sitemap
ETAG_DIR=/var/cache/cr/prehrajto-sitemap/etags
LOG_FILE=/var/log/cr/prehrajto-sync.log
IMPORTER=/opt/cr/scripts/import-prehrajto-uploads.py
```

## Crontab template

Install with `sudo crontab -u root -e`:

```cron
# Prehraj.to sitemap sync (#642 / #645)
DATABASE_URL=postgres://cr:cr_secret@127.0.0.1:5432/cr
MATCHES_CSV=/opt/cr/data/prehrajto-matches.csv
LOG_FILE=/var/log/cr/prehrajto-sync.log

# Daily full sync at 03:00 UTC — authoritative mark-dead snapshot
0 3 * * * /opt/cr/scripts/cron-prehrajto-sync.sh full

# Incremental check every 4h (only ETag-changed sub-sitemaps)
0 0,4,8,12,16,20 * * * /opt/cr/scripts/cron-prehrajto-sync.sh incremental
```

## Setup checklist for prod VPS

```bash
# 1. Copy scripts
sudo mkdir -p /opt/cr/scripts /opt/cr/data /var/cache/cr/prehrajto-sitemap /var/log/cr
sudo cp scripts/cron-prehrajto-sync.sh \
       scripts/import-prehrajto-uploads.py \
       scripts/video_sources_helper.py \
       /opt/cr/scripts/
sudo chmod +x /opt/cr/scripts/cron-prehrajto-sync.sh

# 2. Place the matches CSV (built once via the pilot pipeline)
sudo cp /tmp/prehrajto-pilot/matches-full.csv /opt/cr/data/prehrajto-matches.csv

# 3. Install crontab (see above)
sudo crontab -e

# 4. Smoke-test full mode manually, watch the log tail
sudo /opt/cr/scripts/cron-prehrajto-sync.sh full
sudo tail -f /var/log/cr/prehrajto-sync.log

# 5. Logrotate config (optional)
cat <<'EOF' | sudo tee /etc/logrotate.d/cr-prehrajto-sync
/var/log/cr/prehrajto-sync.log {
    weekly
    rotate 4
    compress
    missingok
    notifempty
    create 0640 root adm
}
EOF
```

## Expected first-run timing

| Mode | Cold | Warm |
|---|---|---|
| `full` | 15-30 min (downloads ~15 GB, parses ~9.6M URLs, batched UPSERTs) | 8-15 min if sitemap dir already populated |
| `incremental` | 1-5 min (only ETag-changed sub-sitemaps + per-film mark-dead skipped) | 30 s if no sitemaps changed |

## Monitoring

- `tail -f /var/log/cr/prehrajto-sync.log` — live progress.
- After 24h: `SELECT COUNT(*) FILTER (WHERE is_alive) FROM video_sources WHERE provider_id = (SELECT id FROM video_providers WHERE slug='prehrajto');`
  should be in the millions and within ~1% of the sitemap URL count.
- After 24h: `SELECT MAX(updated_at) FROM video_sources WHERE provider_id = ...`
  should be no older than 4h.

## Refreshing matches CSV

The matches CSV (`MATCHES_CSV`) is built once by joining sitemap clusters
to TMDB IMDB IDs (see `/tmp/prehrajto-pilot/match_tmdb.py` from the
original pilot). It only needs refreshing when:

- Many new films have been added to our DB without matching prehrajto
  uploads (so the importer's existing CSV doesn't know about them).
- prehraj.to title clustering drifts significantly (rare).

Refresh procedure is out of scope for this README — see the matches
generation script in the pilot directory.
