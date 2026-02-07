# cr

**Modern SEO portal about the Czech Republic**

[![Rust](https://img.shields.io/badge/Rust-2024_edition-DEA584?logo=rust)](https://www.rust-lang.org/)
[![Axum](https://img.shields.io/badge/Axum-0.8-blue)](https://github.com/tokio-rs/axum)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Overview

Hierarchical territorial navigation for the Czech Republic:

- **Kraje** (14 regions)
- **Okresy** (77 districts)
- **ORP** (206 municipalities with extended powers)
- **Obce** (~6,258 municipalities)

Built with Rust for maximum performance and SEO-friendly server-side rendering.

---

## Architecture

Cargo Workspace with Clean Architecture layers:

```
cr/
├── cr-domain/   # Entities, traits (zero deps)
├── cr-app/      # Use-cases, queries, commands, DTOs
├── cr-infra/    # SQLx, CSV import, external APIs
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
| Language | Rust (edition 2024, Tokio) |
| Web | Axum + Askama (SSR) |
| Database | PostgreSQL + pgvector |
| DB Access | SQLx (compile-time checked) |
| AI | pgvector embeddings |
| Images | Cloudflare R2 |

---

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs/) (latest stable)
- [PostgreSQL 16+](https://www.postgresql.org/) with pgvector
- [sqlx-cli](https://github.com/launchbadge/sqlx/tree/main/sqlx-cli)
- Docker (optional, for local DB)

### Setup

```bash
# Clone
git clone https://github.com/Olbrasoft/cr.git
cd cr

# Start local PostgreSQL
docker run -d --name cr-postgres \
  -e POSTGRES_DB=cr -e POSTGRES_USER=cr -e POSTGRES_PASSWORD=cr \
  -p 5432:5432 ankane/pgvector:latest

# Configure
echo 'DATABASE_URL=postgres://cr:cr@localhost:5432/cr' > .env

# Build
cargo build

# Run tests
cargo test
```

---

## Documentation

- [Project Blueprint](docs/BLUEPRINT.md) — architecture, tech stack, database model, conventions
- [CLAUDE.md](CLAUDE.md) — instructions for Claude Code AI agent

---

## Data Sources

- [ČSÚ — Territorial Codebooks](https://www.czso.cz/csu/czso/i_zakladni_uzemni_ciselniky_na_uzemi_cr_a_klasifikace_cz_nuts)
- [ČSÚ — Geodata Portal](https://geodata.csu.gov.cz/)
- [ČÚZK — RÚIAN](https://www.cuzk.cz/ruian/)

---

## License

MIT

---

**Author:** [Olbrasoft](https://github.com/Olbrasoft)
