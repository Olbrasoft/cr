# Development Workflow

> How to develop, test locally, and deploy to production.

---

## Local Development

### Prerequisites

- Rust (latest stable via `rustup`)
- PostgreSQL 16+ (local, running on default port 5432)
- sqlx-cli (`cargo install sqlx-cli`)
- GDAL tools (`sudo apt install gdal-bin`) — for GeoJSON conversion

### Quick Start

```bash
cd ~/Olbrasoft/cr

# 1. Database (already set up as cr_dev)
#    Connection: postgres://jirka@localhost/cr_dev

# 2. Run migrations
sqlx migrate run --source cr-infra/migrations

# 3. Build and run
cargo run -p cr-web

# 4. Open in browser
#    http://dev.localhost:3000
```

### Local URL

Use `http://dev.localhost:3000` for local development.

`dev.localhost` resolves to `127.0.0.1` automatically (RFC 6761) — no DNS or `/etc/hosts` configuration needed.

### Environment Variables (.env)

```bash
DATABASE_URL=postgres://jirka@localhost/cr_dev
IMAGE_BASE_URL=https://ceskarepublika.wiki
```

| Variable | Dev | Production | Purpose |
|----------|-----|-----------|---------|
| `DATABASE_URL` | `postgres://jirka@localhost/cr_dev` | `postgres://cr:***@db:5432/cr` | Database connection |
| `IMAGE_BASE_URL` | `https://ceskarepublika.wiki` | *(empty)* | Image URL prefix. In dev, images load from production. In production, empty = served via Cloudflare Worker at `/img/` |
| `GEOJSON_DATA_DIR` | `data/geojson` (default) | `/app/data/geojson` (Docker) | GeoJSON polygon data directory |
| `STATIC_DIR` | `cr-web/static` (default) | `/app/static` (Docker) | Static assets directory |
| `RUST_LOG` | `info` | `info` | Log level |

### Database

Local database: `cr_dev` on localhost PostgreSQL.

The database is small (~9 MB) and can be fully recreated from:
1. Migrations in `cr-infra/migrations/`
2. CSV import: `cargo run -p cr-infra --bin import-csv`
3. Centroid import: `psql -d cr_dev -f data/geojson/import_centroids.sql`

### Full Database Reset

```bash
# Drop and recreate
dropdb cr_dev
createdb cr_dev

# Run migrations (auto-runs on app startup, or manually:)
sqlx migrate run --source cr-infra/migrations

# Import territorial data
cargo run -p cr-infra --bin import-csv

# Import GPS centroids
psql -d cr_dev -f data/geojson/import_centroids.sql
```

---

## Development Cycle

### Rule: Test locally first, deploy in batches

```
1. Make changes locally
2. cargo check / cargo test / cargo clippy
3. Test in browser at http://dev.localhost:3000
4. Iterate until satisfied (multiple commits OK)
5. When a feature is complete and tested → deploy to production
```

### DO NOT deploy after every small change. Batch related changes together.

### Git Workflow

```bash
# Work on a feature
git add <files>
git commit -m "feat: description"

# More iterations...
git commit -m "fix: refinement"
git commit -m "style: visual tweak"

# When ready to deploy:
git push origin main
# Then run deployment (see below)
```

---

## Deployment to Production

### Server Details

- **Host:** Hetzner Cloud CAX11 (ARM64)
- **IP:** 46.225.101.253
- **SSH:** `ssh -p 2222 root@46.225.101.253`
- **Project path:** `/opt/cr/`
- **Stack:** Docker Compose (web + PostgreSQL)

### Deploy Script

```bash
# 1. Sync files to server
rsync -avz --delete \
  --exclude 'target/' \
  --exclude '.git/' \
  --exclude 'data/geojson/*_wgs84.geojson' \
  --exclude '.playwright-mcp/' \
  --exclude '*.png' \
  --exclude '.env' \
  -e "ssh -p 2222" \
  ~/Olbrasoft/cr/ root@46.225.101.253:/opt/cr/

# 2. Rebuild and restart
ssh -p 2222 root@46.225.101.253 "cd /opt/cr && docker compose build web && docker compose up -d web"

# 3. Check logs
ssh -p 2222 root@46.225.101.253 "docker compose -f /opt/cr/docker-compose.yml logs web --tail 10"
```

### Database Deployment

Since there is no admin UI yet and no user-generated content, the production database can be fully replaced from local:

```bash
# Export local database
pg_dump -U jirka cr_dev > /tmp/cr_dev.sql

# Upload and import to production
scp -P 2222 /tmp/cr_dev.sql root@46.225.101.253:/tmp/
ssh -p 2222 root@46.225.101.253 "docker exec -i cr-db-1 psql -U cr -d cr < /tmp/cr_dev.sql"
```

For migrations only (no data reset):
```bash
# Migrations run automatically on app startup (sqlx::migrate! in main.rs)
# Just rebuild and restart the web container
```

### When to Deploy Database vs Just Code

| Change | Deploy |
|--------|--------|
| Template/CSS changes only | Code only (rsync + rebuild) |
| New migration + code | Code (migration runs on startup) |
| New data import (centroids, monuments...) | Database (pg_dump + restore) |
| Schema change + data | Both |

---

## Project Structure Reminder

```
~/Olbrasoft/cr/           # Local development
/opt/cr/                  # Production server (no .git, synced via rsync)
```

### Key Paths on Server

| Path | Purpose |
|------|---------|
| `/opt/cr/` | Project root |
| `/opt/cr/docker-compose.yml` | Docker Compose config |
| `/opt/cr/Dockerfile` | Multi-stage Rust build |
| `/opt/cr/data/geojson/` | GeoJSON data files (copied into Docker image) |
| `/opt/cr/cr-web/static/` | Static assets (copied into Docker image) |

### Docker Containers

| Container | Purpose |
|-----------|---------|
| `cr-web-1` | Rust web server (port 80 → 3000) |
| `cr-db-1` | PostgreSQL 16 |

---

## Useful Commands

```bash
# Local
cargo check                    # Fast compilation check
cargo test                     # Run tests
cargo clippy -- -D warnings    # Lint
cargo run -p cr-web            # Run web server
cargo run -p cr-web --release  # Run optimized

# Production
ssh -p 2222 root@46.225.101.253 "docker compose -f /opt/cr/docker-compose.yml logs web --tail 20"
ssh -p 2222 root@46.225.101.253 "docker compose -f /opt/cr/docker-compose.yml restart web"
ssh -p 2222 root@46.225.101.253 "docker exec cr-db-1 psql -U cr -d cr -c 'SELECT count(*) FROM regions;'"
```
