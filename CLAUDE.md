# CLAUDE.md

Instructions for Claude Code when working in this repository.

## CRITICAL — Issue Completion Rules (MANDATORY)

**An issue is NOT done after creating a PR.** An issue is done ONLY after ALL of these:
1. PR created and pushed
2. CI passes (build, tests, clippy, format)
3. Code review addressed
4. PR merged to main
5. Deploy to production succeeds
6. **curl checks (URLs, HTML attributes) + Playwright screenshot (visual confirmation)**

**Production verification has two steps:** First curl checks (HTTP status, HTML grep). Then Playwright opens the page, takes a screenshot, and you VISUALLY confirm images render, layout is correct, and no elements are broken. Both steps are mandatory — curl alone cannot detect broken images.

**NEVER say "Issue done" or "Hotovo" after just creating a PR.** That is only ~20% of the work. The issue is complete only after step 6 — you have SEEN a Playwright screenshot confirming the changes work as described in the issue.

**After creating ANY Pull Request, you MUST immediately set up CronCreate monitoring.**
This is NOT optional. The CronCreate runs the full pipeline autonomously (CI → review → merge → deploy → Playwright verify) without asking the user. See `ci-workflow-monitor` skill for the CronCreate prompt template.

**NEVER close a GitHub issue before production verification.** Closing an issue means the work is DONE and verified on production. The sequence is:
1. PR merged → deploy runs → Playwright verifies → THEN close issue
2. If Playwright shows the changes are NOT visible or broken → fix, push, new PR → repeat

**NEVER use `gh issue close` before Playwright confirms the changes work on production.**

**Branch protection is enabled on main.** Required CI checks: Check & Clippy, Format, Test.

**Before merging, ALWAYS check Copilot review status:**
```bash
HEAD=$(gh pr view <PR> --repo Olbrasoft/cr --json headRefOid --jq '.headRefOid')
gh api "repos/Olbrasoft/cr/commits/${HEAD}/check-runs" --jq '.check_runs[] | select(.name == "Agent") | .status'
```
- `in_progress`/`queued` → Copilot review still running → WAIT, do NOT merge
- `completed` → read review comments, fix ALL, push. Then merge.
- empty (no output) → Copilot not active → merge after CI passes

**NEVER merge before Copilot review finishes.** Copilot almost always finds something to fix (~92%). Read comments, fix them, push. Only then merge.

**Progress notifications should say:**
- After PR: "PR vytvořen, CI běží. Sleduji pipeline." (NOT "Issue hotová")
- Merge blocked: "Merge blokován — čekám na dokončení checks." (NOT "merguju")
- After merge: "PR mergnut, sleduji deploy." (NOT "Issue hotová")
- After deploy + verify OK: "Issue #N hotová — změny ověřeny na produkci: [what was verified]" → THEN close issue

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

- **ČSÚ territorial structure CSV:** `data/csu/struktura_uzemi_cr_2025.csv` (local copy, 6,258 municipalities)
- **ČSÚ metadata:** `data/csu/struktura_uzemi_cr_metadata.json`
- **GeoJSON boundaries:** TODO — copy from `~/Dokumenty/ProofOfConcepts/CzechRepublic/GeoJSON/` when needed
- **RÚIAN address points:** TODO — copy from `~/Dokumenty/ProofOfConcepts/CzechRepublic/CSV/` when needed

## Testing

- Unit tests at bottom of each source file (`#[cfg(test)]`)
- Integration tests in `tests/` directory
- Use `sqlx::test` for database integration tests
- Mock external services with trait implementations

## Database Safety Rules

**CRITICAL: NEVER use `dropdb`, `DROP DATABASE`, or any destructive database operation.**

- NEVER drop or recreate `cr_dev` or `cr_staging` databases
- NEVER truncate tables with imported data
- To fix migration issues: fix the `_sqlx_migrations` table rows, NOT the database
- Use `cr_dev_user` (restricted, cannot DROP DATABASE) — configured in `.env`
- Staging DB (`cr_staging`) stores downloaded source data (Wikipedia texts, etc.) — NEVER modify or delete
- If migrations fail: delete the problematic row from `_sqlx_migrations`, NOT the database

## Development Workflow

### Issue-Driven Development with Autonomous CI/CD Feedback

All work is **issue-driven** and the CI/CD pipeline runs **fully autonomously** — never ask the user, just act.

**After creating a PR, ALWAYS set up CronCreate monitoring.** This is mandatory, not optional.

#### Full Lifecycle (automated after PR creation)

1. **Plan** — Create GitHub issues (use `github-issues` skill for parent + sub-issues)
2. **Implement** — Create feature branch, write code, test locally
3. **PR** — Push branch, create PR
4. **CronCreate** — Immediately set up autonomous pipeline monitor (see below)
5. **Continue working** — Start next issue while CI/review runs (pipeline processing)
6. *(Autonomous)* CI fails → analyze logs, fix, push
7. *(Autonomous)* Review has comments → fix all, push
8. *(Autonomous)* CI passes + review done → merge PR
9. *(Autonomous)* Deploy completes → verify production
10. *(Autonomous)* Read issue description → verify issue-specific changes on production via Playwright/curl
11. *(Autonomous)* Notify result → CronDelete

#### CronCreate Setup (MANDATORY after every PR)

After creating a PR, immediately run CronCreate with the template from `ci-workflow-monitor` skill. Example:

```
CronCreate({
  cron: "*/2 * * * *",
  prompt: "AUTONOMOUS issue-driven CI/CD monitor for Olbrasoft/cr. Working on issue #<NUM>, PR #<NUM>...",
  recurring: true
})
```

The CronCreate prompt must:
- Monitor CI status (`gh pr checks`)
- Monitor review status (`gh pr view --json reviewDecision`)
- **Autonomously merge** when ready (no asking!)
- **Autonomously fix** CI failures and review comments
- Monitor deploy after merge (`gh run list --branch main`)
- **Verify production** — health check + issue-specific changes on `https://ceskarepublika.wiki`
- CronDelete when pipeline complete

**Full template:** See `ci-workflow-monitor` skill in `.claude/skills/` or `~/GitHub/Olbrasoft/GitHub.Actions.Notify/skills/ci-workflow-monitor/SKILL.md`

#### Parent Issues with Sub-Issues (Pipeline Processing)

Follow [Continuous PR Processing Workflow](~/GitHub/Olbrasoft/engineering-handbook/development-guidelines/workflow/continuous-pr-processing-workflow.md):
- **Independent sub-issues**: start next issue immediately after creating PR (don't wait for review)
- **Dependent sub-issues**: wait for previous PR to be merged before starting next
- Multiple PRs run in parallel with separate CronCreate monitors
- After ALL sub-issues done: verify all changes on production

### Branch Naming

- `feat/description` — new features
- `fix/description` — bug fixes
- `refactor/description` — code restructuring

### Local Development

```bash
# Database: postgres://jirka@localhost/cr_dev
cargo run -p cr-web    # Listens on port 3000
# Open http://dev.localhost:3000
```

- Test locally before creating PR
- Run `cargo check`, `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`
- Use Playwright for browser verification

### Deploy to Production

**Automatic:** Merge PR to main → GitHub Actions CI (cloud) → Deploy on self-hosted runner → TTS notification → Playwright verify.

Pipeline:
1. Check & Clippy (cloud)
2. Format check (cloud)
3. Tests (cloud)
4. Deploy: rsync + docker build + health check (self-hosted runner)
5. **Notify**: TTS notification via VirtualAssistant (self-hosted runner)
6. **Verify**: Playwright health + homepage check (self-hosted runner)

**Manual deploy (emergency only):**
```bash
rsync -avz --delete --exclude 'target/' --exclude '.git/' --exclude '.env' --exclude 'data/images/' -e "ssh -p 2222" ~/Olbrasoft/cr/ root@46.225.101.253:/opt/cr/
ssh -p 2222 root@46.225.101.253 "cd /opt/cr && docker compose build web && docker compose up -d web"
```

## Current Project Status

**Phase 1 — Foundation** (deployed, live at ceskarepublika.wiki)

### Completed
- Cargo workspace (cr-domain, cr-app, cr-infra, cr-web)
- Domain entities: Region, District, Orp, Municipality (with latitude/longitude)
- SQLx migrations, CSV import (6,258 municipalities)
- Axum + Askama SSR (homepage, region, ORP, municipality pages)
- SEO-friendly URLs (`/kraj/orp/obec/`)
- Interactive Leaflet maps with GeoJSON polygons on all pages
- GeoJSON API endpoints (`/api/geojson/municipality/{code}`, `/api/geojson/orp/{code}`)
- Docker Compose deployment on Hetzner CAX11
- Domain `ceskarepublika.wiki` with Cloudflare CDN/SSL
- Image serving via Cloudflare R2

### Phase 2+ tracked in GitHub Issues

## Engineering Handbook

General development standards are in `~/GitHub/Olbrasoft/engineering-handbook/`. This CLAUDE.md contains only project-specific instructions for Olbrasoft/cr.
