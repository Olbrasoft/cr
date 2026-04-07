# ceskarepublika.wiki

**Encyklopedický portál o České republice a její kultuře a historii** — [ceskarepublika.wiki](https://ceskarepublika.wiki)

[![Live](https://img.shields.io/badge/Live-ceskarepublika.wiki-blue?style=flat&logo=globe)](https://ceskarepublika.wiki)
[![Rust](https://img.shields.io/badge/Rust-2024_edition-DEA584?logo=rust)](https://www.rust-lang.org/)
[![Axum](https://img.shields.io/badge/Axum-0.8-blue)](https://github.com/tokio-rs/axum)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![CI](https://github.com/Olbrasoft/cr/actions/workflows/ci.yml/badge.svg)](https://github.com/Olbrasoft/cr/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## About

[ceskarepublika.wiki](https://ceskarepublika.wiki) is a comprehensive encyclopedic portal about the Czech Republic, providing detailed information about all territorial units, cultural monuments, swimming facilities, and more.

### Content

- **14 regions** (kraje) with maps and statistics
- **206 ORP** (municipalities with extended powers) with landmarks and pools
- **6,258 municipalities** (obce) with Wikipedia photos and descriptions
- **18,000+ cultural monuments** from NPÚ (National Heritage Institute) with photos and rewritten descriptions
- **249 swimming facilities** — aquaparks, pools, outdoor pools, natural swimming
- **43,000+ photos** served via Cloudflare R2 with SEO-friendly URLs

### Features

- Server-side rendered pages optimized for SEO
- Interactive maps with OpenStreetMap / Leaflet
- Photo gallery with lightbox, slideshow, and on-the-fly resize proxy
- Automated CI/CD — merge PR → automatic deploy
- Clean Architecture with typed domain layer

---

## Architecture

Cargo Workspace with Clean Architecture layers:

```
cr/
├── cr-domain/   # Entities, value objects, repository traits (zero deps)
├── cr-app/      # Use-cases, queries, AppError
├── cr-infra/    # SQLx repositories, CSV import
└── cr-web/      # Axum server, Askama SSR templates
```

```
cr-web ──→ cr-app ──→ cr-domain
              ↑
cr-infra ─────┘
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Rust (edition 2024, Tokio async) |
| Web | Axum + Askama (compile-time SSR) |
| Database | PostgreSQL 16 |
| DB Access | SQLx |
| Images | Cloudflare R2 + on-the-fly resize proxy |
| Maps | Leaflet + OpenStreetMap |
| CI/CD | GitHub Actions (auto-deploy on merge) |
| Hosting | Hetzner CAX11 (ARM64) + Cloudflare CDN |
| Domain | [ceskarepublika.wiki](https://ceskarepublika.wiki) |

---

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs/) (latest stable)
- [PostgreSQL 16+](https://www.postgresql.org/)
- [sqlx-cli](https://github.com/launchbadge/sqlx/tree/main/sqlx-cli)

### Setup

```bash
# Clone
git clone https://github.com/Olbrasoft/cr.git
cd cr

# Configure
echo 'DATABASE_URL=postgres://cr:cr@localhost:5432/cr' > .env

# Build & test
cargo build
cargo test

# Run locally
cargo run -p cr-web
# Open http://dev.localhost:3000
```

---

## Documentation

- [Development Workflow](docs/DEVELOPMENT.md) — local setup, testing, deployment
- [Project Blueprint](docs/BLUEPRINT.md) — architecture, database model, conventions
- [URL Structure](docs/URL_STRUCTURE.md) — routing, territorial vs commercial URLs
- [CLAUDE.md](CLAUDE.md) — instructions for AI-assisted development

---

## Data Sources

- [ČSÚ — Territorial Codebooks](https://www.czso.cz/csu/czso/i_zakladni_uzemni_ciselniky_na_uzemi_cr_a_klasifikace_cz_nuts) — regions, districts, ORP, municipalities
- [NPÚ — Památkový katalog](https://pamatkovykatalog.cz/) — cultural monuments, photos, descriptions
- [Czech Wikipedia](https://cs.wikipedia.org/) — municipality photos and texts
- [jduplavat.cz](https://www.jduplavat.cz/) — swimming facilities

---

## License

MIT

---

**Created by [Olbrasoft](https://github.com/Olbrasoft)** · [ceskarepublika.wiki](https://ceskarepublika.wiki)
