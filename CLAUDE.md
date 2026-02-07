# CLAUDE.md

Instructions for Claude Code when working in this repository.

## What This Is

**Olbrasoft/cr** — Modern SEO portal about the Czech Republic. Hierarchical territorial navigation: Regions → Districts → ORP → Municipalities, with AI features and high performance.

**Language:** Rust (edition 2024)
**Architecture:** Clean Architecture via Cargo Workspace

## Build & Test

```bash
# Build all crates
cargo build

# Run tests
cargo test

# Run web server (when implemented)
cargo run -p cr-web

# Check without building
cargo check

# Lint
cargo clippy -- -D warnings

# Format
cargo fmt --check
```

## Architecture — Cargo Workspace

```
cr/
├── cr-domain/   # Entities, traits, error types. ZERO framework deps.
├── cr-app/      # Use-cases, queries, commands, DTOs. Depends on cr-domain.
├── cr-infra/    # SQLx, CSV import, external APIs. Depends on cr-domain + cr-app.
└── cr-web/      # Axum server, Askama templates. Depends on all above.
```

### Dependency Flow (Clean Architecture)

```
cr-web ──→ cr-app ──→ cr-domain
              ↑
cr-infra ─────┘
```

**cr-domain** has NO dependency on cr-app, cr-infra, or cr-web.

## Key Design Decisions

### Primary Keys
- **i32 everywhere** (PostgreSQL SERIAL). No UUID, no i64. Consistent across all tables and FKs.

### CQRS
- **Direct function calls**, no mediator pattern.
- Organize code into `queries/` (SELECT) and `commands/` (INSERT/UPDATE/DELETE) modules.
- Axum handler → service function → SQLx query. No magic, no runtime dispatch.

### Database
- **PostgreSQL** with **pgvector** extension for AI embeddings.
- **SQLx** with compile-time query checking.
- **Migrations:** SQL scripts managed via `sqlx-cli`.
- **Separate tables** for each territorial level: `regions`, `districts`, `orp`, `municipalities` (NOT a single table with type enum).

### Web
- **Axum** web framework (Tokio-based).
- **Askama** templates (compile-time, SEO-friendly SSR).
- **Server-Side Rendering** for all main content (no client-side JS API for SEO pages).

### Error Handling
- `thiserror` in cr-domain and cr-infra for typed errors.
- `anyhow` in cr-web for convenience.

### Logging
- `tracing` + `tracing-subscriber` (structured logging).

## Naming Conventions

### Rust Code
- `snake_case` for functions, variables, modules, file names
- `PascalCase` for structs, enums, traits
- `SCREAMING_SNAKE_CASE` for constants

### Crate Names
- Lowercase with hyphens: `cr-domain`, `cr-app`
- In code referenced with underscores: `cr_domain`, `cr_app`

### Database
- `snake_case` for table names, column names
- Plural table names: `regions`, `districts`, `orp`, `municipalities`
- FK columns: `{table_singular}_id` (e.g., `region_id`, `district_id`)

## Entity Design

Separate structs for each territorial level (composition, not inheritance):

```rust
pub struct Region {
    pub id: i32,
    pub name: String,
    pub slug: String,
    pub region_code: String,
    pub nuts_code: String,
    pub created_by: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
```

Each entity has: `id` (i32), `name`, `slug` (unique, SEO), ČSÚ code(s), audit fields.

Hierarchical FK chain: `municipality.orp_id → orp.district_id → district.region_id → region.id`

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Rust (edition 2024, Tokio async runtime) |
| Web Framework | Axum |
| Templates | Askama (compile-time SSR) |
| Database | PostgreSQL + pgvector |
| DB Access | SQLx (compile-time checked queries) |
| Auth | argon2 + tower-sessions |
| AI | rig or aws-sdk-bedrock |
| Images | Cloudflare R2 (S3 compatible) |
| Logging | tracing |
| Error Handling | thiserror + anyhow |
| Serialization | serde + serde_json |
| CSV Import | csv crate |

## Data Sources

- **ČSÚ territorial structure CSV:** `~/Dokumenty/ProofOfConcepts/CzechRepublic/struktura_uzemi_cr_2025.csv`
- **GeoJSON boundaries:** `~/Dokumenty/ProofOfConcepts/CzechRepublic/GeoJSON/`
- **RÚIAN address points:** `~/Dokumenty/ProofOfConcepts/CzechRepublic/CSV/`

## Testing

- Unit tests at bottom of each source file (`#[cfg(test)]`)
- Integration tests in `tests/` directory
- Use `sqlx::test` for database integration tests
- Mock external services with trait implementations

## Current Project Status

**Phase 1 — Foundation** (in progress)

### Completed
- Cargo workspace scaffolded (cr-domain, cr-app, cr-infra, cr-web)
- Domain entities implemented in `cr-domain/src/entities/`:
  - `Region` (14 regions / kraje) — region_code, nuts_code
  - `District` (77 districts / okresy) — district_code, FK → regions
  - `Orp` (206 ORP) — orp_code, FK → districts
  - `Municipality` (~6,258 municipalities / obce) — municipality_code, pou_code, FK → orp
- All entities: `#[derive(Debug, Clone, Serialize, Deserialize)]`, all fields `pub`
- No `sqlx::FromRow` in domain (belongs in cr-infra)
- Documentation updated (CLAUDE.md, docs/BLUEPRINT.md) — English entity names

### Next Steps
- **Issue #1:** Add SQLx migrations for territorial hierarchy tables
- Repository traits in cr-domain (planned)
- SQLx implementations in cr-infra
- CSV import from ČSÚ data
- Basic Axum + Askama SSR

## Engineering Handbook

General development standards are in `~/GitHub/Olbrasoft/engineering-handbook/`. This CLAUDE.md contains only project-specific instructions for Olbrasoft/cr.
