# Project Blueprint: Olbrasoft/cr

Modern SEO portal about the Czech Republic (Regions → Districts/ORP → Municipalities) with AI features and high performance.

---

## 1. Architecture (Cargo Workspace)

Instead of a .NET Solution, we use a Rust Workspace. All directory/package names use hyphens (`-`); in code they are referenced with underscores (`_`).

| Crate | Layer | Responsibility | Dependencies |
|-------|-------|---------------|--------------|
| **cr-domain** | Domain / Business Logic | Pure entities (structs), enums, traits. Zero external framework deps. | none |
| **cr-app** | Application | Use-cases, query/command functions, DTOs. Orchestration between DB and AI. | cr-domain |
| **cr-infra** | Infrastructure | SQLx (PostgreSQL) persistence, CSV importer for territorial units, GitHub integration (Octocrab). | cr-domain, cr-app |
| **cr-web** | Presentation | Axum server, SSR via Askama templates, route handlers, view DTOs. | cr-domain, cr-app, cr-infra |

### Dependency Flow

```
cr-web ──→ cr-app ──→ cr-domain
              ↑
cr-infra ─────┘
```

---

## 2. Technology Stack

| Component | Technology | Notes |
|-----------|-----------|-------|
| **Language** | Rust (edition 2024) | Tokio async runtime |
| **Web Framework** | Axum | Modern, fast, built on Tokio/Tower |
| **HTML Templates** | Askama | Compile-time templates, SEO friendly, Jinja2-like syntax |
| **Database** | PostgreSQL + pgvector | pgvector for AI embeddings |
| **DB Access** | SQLx | Async, compile-time SQL query checking |
| **Identity** | argon2 + tower-sessions | Password hashing + cookie sessions |
| **AI** | rig or aws-sdk-bedrock | Embedding generation |
| **Image Storage** | Cloudflare R2 | S3-compatible, for ~10 GB of images |
| **Error Handling** | thiserror + anyhow | Typed errors in domain/infra, convenience in web |
| **Logging** | tracing + tracing-subscriber | Structured logging, Tower integration |
| **Serialization** | serde + serde_json | Universal serialization |
| **CSV Import** | csv crate | ČSÚ territorial data import |
| **Configuration** | dotenvy | .env file loading |

---

## 3. Database Model

### Primary Keys

- **i32 (SERIAL)** everywhere — consistent across all tables and foreign keys.
- No UUID, no i64. Simpler code, smaller indexes, no implicit conversions in Rust.

### Table Structure

Separate tables for each territorial level (not a single table with type enum):

```sql
-- 1. Regions (14 regions / kraje)
CREATE TABLE regions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    slug VARCHAR(250) NOT NULL UNIQUE,
    region_code VARCHAR(10) NOT NULL,
    nuts_code VARCHAR(10) NOT NULL,
    created_by INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Districts (77 districts / okresy)
CREATE TABLE districts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    slug VARCHAR(250) NOT NULL UNIQUE,
    district_code VARCHAR(10) NOT NULL,
    region_id INT NOT NULL REFERENCES regions(id) ON DELETE RESTRICT,
    created_by INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. ORP (206 municipalities with extended powers)
CREATE TABLE orp (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    slug VARCHAR(250) NOT NULL UNIQUE,
    orp_code VARCHAR(10) NOT NULL,
    district_id INT NOT NULL REFERENCES districts(id) ON DELETE RESTRICT,
    created_by INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Municipalities (~6,258 municipalities / obce)
CREATE TABLE municipalities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    slug VARCHAR(250) NOT NULL UNIQUE,
    municipality_code VARCHAR(10) NOT NULL,
    pou_code VARCHAR(10) NOT NULL,
    orp_id INT NOT NULL REFERENCES orp(id) ON DELETE RESTRICT,
    created_by INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Indexes

```sql
CREATE INDEX idx_districts_region_id ON districts(region_id);
CREATE INDEX idx_orp_district_id ON orp(district_id);
CREATE INDEX idx_municipalities_orp_id ON municipalities(orp_id);
```

Slug columns already have UNIQUE constraint which creates an implicit index.

### Migrations

Pure SQL scripts managed via `sqlx-cli`:

```bash
# Create migration
sqlx migrate add initial_schema

# Run migrations
sqlx migrate run

# Revert last migration
sqlx migrate revert
```

### SEO

- `slug` column in every table with unique index.
- URL structure: `/stredocesky-kraj/`, `/stredocesky-kraj/benesov/`, etc.

---

## 4. CQRS Pattern

**Direct function calls** — no mediator pattern (unlike .NET MediatR).

```
Axum handler → query/command function → SQLx
```

Code organization:

```
cr-app/src/
├── queries/            # Read operations (SELECT)
│   ├── region.rs       # get_region_by_slug, list_regions, etc.
│   ├── district.rs
│   ├── orp.rs
│   └── municipality.rs
├── commands/           # Write operations (INSERT/UPDATE/DELETE)
│   └── import.rs       # import_regions_from_csv
└── dto/                # Data Transfer Objects
    ├── region.rs
    ├── district.rs
    └── breadcrumb.rs
```

---

## 5. Entity Design (Rust)

Composition instead of inheritance (no `CreationInfo<T>` base class):

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Region {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub region_code: String,
    pub nuts_code: String,
    pub created_by: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct District {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub district_code: String,
    pub region_id: i32,
    pub created_by: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Orp {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub orp_code: String,
    pub district_id: i32,
    pub created_by: i32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Municipality {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub municipality_code: String,
    pub pou_code: String,
    pub orp_id: i32,
    pub created_by: i32,
    pub created_at: DateTime<Utc>,
}
```

---

## 6. Development Environment & Deployment

| Aspect | Tool |
|--------|------|
| **Editor** | VS Code / Cursor (rust-analyzer extension) |
| **AI Coding** | Claude Code (terminal agent on Debian 13) |
| **Local DB** | Docker: `ankane/pgvector:latest` |
| **Dev Hosting** | Shuttle.dev (prototyping) |
| **Prod Hosting** | Hetzner Cloud (Debian 13 VPS, 10 GB data) |

### Local Development Setup

```bash
# 1. Start PostgreSQL with pgvector
docker run -d --name cr-postgres \
  -e POSTGRES_DB=cr \
  -e POSTGRES_USER=cr \
  -e POSTGRES_PASSWORD=cr \
  -p 5432:5432 \
  ankane/pgvector:latest

# 2. Set DATABASE_URL
echo 'DATABASE_URL=postgres://cr:cr@localhost:5432/cr' > .env

# 3. Run migrations
sqlx migrate run

# 4. Build and run
cargo run -p cr-web
```

---

## 7. Conventions

### Code Style

| Element | Convention | Example |
|---------|-----------|---------|
| Functions, variables | `snake_case` | `get_region_by_slug` |
| Structs, enums, traits | `PascalCase` | `Region`, `District` |
| Constants | `SCREAMING_SNAKE_CASE` | `MAX_PAGE_SIZE` |
| Modules, files | `snake_case` | `queries/region.rs` |
| Crate names | `kebab-case` | `cr-domain` |

### Architecture

- **Composition over inheritance** — use traits and struct embedding, not class hierarchies.
- **CQRS** — explicit query functions (read) and command functions (write).
- **SSR** — Server-Side Rendering for all main content (no client-side JS API for SEO pages).

### Testing

- **Unit tests** at the bottom of each source file in `#[cfg(test)]` module.
- **Integration tests** in `tests/` directory.
- Use `sqlx::test` attribute for database integration tests.

---

## 8. Data Sources

| Source | Location | Size | Usage |
|--------|----------|------|-------|
| ČSÚ territorial CSV | `~/Dokumenty/ProofOfConcepts/CzechRepublic/struktura_uzemi_cr_2025.csv` | 2.7 MB | Region import |
| GeoJSON boundaries | `~/Dokumenty/ProofOfConcepts/CzechRepublic/GeoJSON/` | ~150 MB | Map polygons |
| RÚIAN addresses | `~/Dokumenty/ProofOfConcepts/CzechRepublic/CSV/` | ~6,000 files | GPS coordinates |

---

## 9. Roadmap

### Phase 1 — Foundation (Current)
- [x] Repository setup, Cargo workspace
- [x] Domain entities (Region, District, Orp, Municipality)
- [x] SQLx migrations, CSV import (6,258 municipalities loaded)
- [x] Basic Axum + Askama SSR (homepage, region, ORP, municipality pages)
- [x] SEO-friendly URLs (`/kraj/orp/obec/`)
- [x] Docker Compose deployment (Rust + Postgres on Hetzner CAX11)
- [x] Domain registered: `ceskarepublika.wiki`
- [x] DNS A records → 46.225.101.253
- [ ] Cloudflare setup (DNS proxy, SSL/TLS, CDN, Email Routing)
- [ ] Email: `info@ceskarepublika.wiki` via Cloudflare Email Routing

### Phase 2 — Content
- Monuments module (historic landmarks catalog)
- AI embeddings for semantic search (pgvector)
- Image management (Cloudflare R2)

### Phase 3 — Extensions
- Additional business modules (accommodation, businesses, real estate)
- User authentication (argon2 + tower-sessions)
- Previo.cz API integration

### Phase 4 — Scale
- GeoJSON polygon boundaries on interactive maps
- GPS coordinates from RÚIAN
- Caching strategy
- GitHub Actions CI/CD pipeline

---

**Created:** 2026-02-07
**Author:** Olbrasoft + Claude Code
**Version:** 0.2.0 (Phase 1 — deployed)
**License:** MIT
**Last updated:** 2026-02-12
