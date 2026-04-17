# CLAUDE.md

Instructions for Claude Code when working in this repository.

## CRITICAL — Change Workflow Order (MANDATORY)

Every code change in this repo goes through FOUR stages, in this exact order. Never skip, never reorder.

1. **Local implementation** — edit code on the feature branch.
2. **Local verification** — build + run locally (`cargo check`, `cargo test`, `cargo run -p cr-web`, unit tests for Python scripts). The change MUST work end-to-end on the dev environment before moving on.
3. **Production deploy + verification** — cross-compile, scp binary (or rsync Python), restart the web container, then **open Playwright against `https://ceskarepublika.wiki` from the local PC** and exercise the feature as a real user (see "Playwright Testing Rules" below). Fix any issues locally, redeploy, re-verify until prod is green.
4. **PR + code review + merge** — ONLY after step 3 is green: `git push`, `gh pr create`, wait for CI + Copilot review, address comments, merge.

**Why this order matters:** Opening a PR before prod verification means Copilot reviews code that will likely need more fixes once it's been tested on production — triggering a PR-fix-review-fix cycle. Testing on prod first means Copilot reviews the final working version **once**. It also prevents "issue hotová" messages after a merged PR when the actual feature is still broken on the live site.

**Concrete don'ts:**
- Do NOT `gh pr create` before you have paste-ready "deployed + Playwright-verified" evidence for the PR body's Test plan.
- Do NOT merge a PR just because CI is green — CI only tests code correctness, not feature correctness.
- `curl` smoke tests are helpful but do NOT replace Playwright interaction for UI changes.

## CRITICAL — Issue Completion Rules (MANDATORY)

**An issue is NOT done after creating a PR.** An issue is done ONLY after ALL of these:
1. PR created and pushed
2. CI passes (build, tests, clippy, format)
3. Code review addressed
4. PR merged to main
5. Deploy to production succeeds
6. **Playwright end-to-end test against production (see below)**

**Production verification — Playwright interactive testing (MANDATORY):**

After deploy, you MUST open the production URL in Playwright **from our local PC** and perform a FULL end-to-end test. This is NOT just a screenshot — you must **interact with the page** as a real user would:

- **Static pages** (displaying data): navigate to URL, verify content is visible, take screenshot
- **Interactive features** (forms, buttons, downloads): fill inputs, click buttons, wait for results, verify the feature actually works end-to-end, take screenshot of the result
- **API features**: call the API, verify correct JSON response, then test via the UI too

**Example for a video download page:**
1. Open `https://ceskarepublika.wiki/stahnout-video/` in Playwright
2. Fill the URL input with a test URL
3. Click "Načíst info"
4. Wait for video preview to appear — verify title, thumbnail, duration are shown
5. Click "Stáhnout"
6. Verify the download starts (response with Content-Disposition header)
7. Take screenshot at each step
8. If ANY step fails → fix, new PR, repeat

**curl checks alone are NOT sufficient.** They only verify the API works. The full user flow through the UI must be tested.

**Console errors check (MANDATORY):**
Every Playwright test MUST also capture and verify browser console. There must be ZERO errors, warnings, or DevTools issues on page load and during interaction. Use Playwright's `page.on('console')` and `page.on('pageerror')` to capture all messages. If any console error is found, the test FAILS — fix before marking done.

**ALL UI elements MUST be tested (MANDATORY):**
Before marking any UI change as done, EVERY visible element on the page must be tested:
- Every button must be clicked and its effect verified
- Every toggle/switch must be toggled and both states verified
- Every input must be filled and its validation tested
- Every link must be verified (href, target, opens correctly)
- Disabled elements must be verified as intentionally disabled with proper UX (or hidden)
- If a control doesn't work or is not needed → it must NOT be shown on the page

**NEVER say "Issue done" or "Hotovo" after just creating a PR.** That is only ~20% of the work. The issue is complete only after step 6 — you have performed a full Playwright interactive test confirming the feature works as described in the issue.

## Playwright Testing Rules

**Playwright runs ONLY from our local PC, NEVER on the server.**

- Playwright + Chromium is installed locally on our development machine
- Tests run against the production URL (`https://ceskarepublika.wiki`)
- The Docker image / VPS server MUST NOT contain Playwright, Chromium, or any testing tools
- Docker image must be minimal — only production binary + static assets + data
- No Python, no test frameworks, no browsers on the server

**Code review feedback is push-based via FIFO pipes — no CronCreate polling needed.**
Code review events arrive automatically via event files + FIFO wake. Claude Code wakes instantly from idle when events arrive.

**NEVER close a GitHub issue before production verification.** Closing an issue means the work is DONE and verified on production. The sequence is:
1. PR merged → local deploy → Playwright verifies → THEN close issue
2. If Playwright shows the changes are NOT visible or broken → fix, push, new PR → repeat

**NEVER use `gh issue close` before Playwright confirms the changes work on production.**

**Branch protection is enabled on main.** Required CI checks: Check & Clippy, Format, Test.

**Copilot review — rely on the ghnotify classifier, not on polling.** The ghnotify classifier now emits an explicit next-action hint on every wake, so the old "check the Agent check-run yourself" dance is no longer needed.

- **`ci-success` wake with `pr!=none`** → merge **now**: `gh pr merge <pr> --squash` (append `--delete-branch` if the branch is safe to delete). Do NOT poll for a Copilot re-review and do NOT wait for an `Agent` check-run to appear. Copilot reviews each PR once automatically; follow-up pushes won't produce another review unless you explicitly request one (`/copilot review` comment — reserve for *substantial* new changes).
- **`ci-failure` wake** → diagnose AND fix, push. Do NOT stop at "pre-existing skip" or similar excuses; that is the failure mode this rule prevents.
- **`code-review-complete` wake** → read comments (`gh api repos/Olbrasoft/cr/pulls/<pr>/comments`), address ALL, push. The next `ci-success` wake will tell you to merge.
- **Only exception to "no wait":** the very first push on a brand-new PR — wait for Copilot's initial `code-review-complete` wake before merging. Copilot finds something to fix ~92% of the time on that first look.

**Progress notifications should say:**
- After PR: "PR vytvořen, CI běží. Sleduji pipeline." (NOT "Issue hotová")
- Merge blocked: "Merge blokován — čekám na dokončení checks." (NOT "merguju")
- After merge: "PR mergnut, deployuji lokálně." (NOT "Issue hotová")
- After local deploy + verify OK: "Issue #N hotová — změny ověřeny na produkci: [what was verified]" → THEN close issue

## Startup — Edge Browser (MANDATORY)

**At the start of every session**, ensure Edge is running and open project tabs using Playwright.

### Step 1: Ensure Edge is running on this workspace

```bash
~/.local/bin/edge-claude-start.sh
```

This ensures Edge is running with CDP port 9222 (user's real profile, logged in) and has a window on this workspace.

### Step 2: Check existing tabs and open missing ones

First list existing tabs:
```
mcp__playwright__browser_tabs(action: "list")
```

Two URLs must be open. **Only open tabs for URLs that are NOT already present:**

1. **`https://ceskarepublika.wiki/`** — production site
2. **`http://issues.localhost/?Query=&Repos=7051&State=open&PageSize=25&Lang=cs&PageNum=1`** — local issues

For each **missing** URL (not already in tab list):
```
mcp__playwright__browser_tabs(action: "new")
mcp__playwright__browser_navigate(url: "<missing URL>")
```

**If both URLs are already open → do nothing, no new tabs.**

Then position the window:
```bash
~/.local/bin/playwright-window-right.sh
```

### Why this matters

Edge runs with the user's real profile (logged in). Playwright connects via CDP. Each Claude Code has its own Playwright MCP process managing its own tabs.

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
- Use `cr_dev_user` (restricted, cannot DROP DATABASE) — configured in `.env`
- Staging DB (`cr_staging`) stores downloaded source data (Wikipedia texts, etc.) — NEVER modify or delete
- If migrations fail: delete the problematic row from `_sqlx_migrations`, NOT the database

### SQLx Migration Checksum Rules

**SQLx uses SHA-384 (NOT SHA-256) for migration checksums.** The `_sqlx_migrations` table stores a 48-byte BYTEA checksum per migration.

- **NEVER manually edit migration files** that have already been applied to production — it changes the checksum and breaks startup
- **NEVER manually UPDATE `_sqlx_migrations.checksum`** unless you are 100% certain you're using SHA-384 of the file content
- If a migration was modified after deployment (e.g. by a refactoring PR), the correct fix is to recalculate SHA-384:
  ```python
  import hashlib
  hashlib.sha384(open("migration.sql", "rb").read()).hexdigest()
  ```
- After fixing checksums, you MUST do `cargo clean && cargo zigbuild` — cargo caches embedded migration hashes and won't recompile unless forced
- **New migrations** should be added as new files with new version numbers, never by modifying existing ones

## Development Workflow

### Issue-Driven Development with Local Deploy

All work is **issue-driven**. CI checks run on GitHub, **deploy runs locally** from the developer's machine.

**Code review feedback is push-based via FIFO pipes** — no CronCreate polling needed.

#### Full Lifecycle

1. **Plan** — Create GitHub issues (use `github-issues` skill for parent + sub-issues)
2. **Implement** — Create feature branch, write code, test locally
3. **PR** — Push branch, create PR → CI checks ("Check & Clippy", fmt, test) + Copilot code review run on GitHub
4. **Continue working** — Start next issue while CI/review runs (pipeline processing)
5. *(FIFO push)* Code review completes → `wake-claude.sh` wakes session by branch → read comments, fix, push
6. CI passes + review done → merge PR
7. **Local deploy** — Pull main, cross-compile locally, upload binary to VPS (~20s)
8. **Playwright verify** — Test issue-specific changes on production
9. Notify result → close issue

#### FIFO-Based Push Wake Notifications

Code review events arrive automatically. No polling, no inotifywait, no flock.

**Code review notification:**
`gh webhook forward` service receives `pull_request_review` events via WebSocket → `webhook-receiver.py` writes event file + calls `wake-claude.sh Olbrasoft/cr {branch}` → wakes ONLY the session on that PR's branch.

**How to react to push events:**

| Event | Status | Action |
|---|---|---|
| `code-review-complete` | `commented` | Read comments: `gh api repos/Olbrasoft/cr/pulls/{PR}/comments`. Fix ALL. Push. |

#### Parent Issues with Sub-Issues (Pipeline Processing)

Follow [Continuous PR Processing Workflow](~/GitHub/Olbrasoft/engineering-handbook/development-guidelines/workflow/continuous-pr-processing-workflow.md):
- **Independent sub-issues**: start next issue immediately after creating PR (don't wait for review)
- **Dependent sub-issues**: wait for previous PR to be merged before starting next
- After ALL sub-issues done: deploy and verify all changes on production

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

**CRITICAL: Deploy is ALWAYS done from the LOCAL machine. NEVER run `docker compose build` on the server.**

The deploy process is: cross-compile locally → scp binary → docker cp → restart. This takes ~20 seconds for incremental builds and keeps the server minimal (no Rust toolchain, no build tools).

**Standard deploy (after merge to main):**
```bash
# 1. Pull latest main
git checkout main && git pull

# 2. Cross-compile locally (~10s incremental, ~90s clean build)
#    IMPORTANT: If migration files changed, do `cargo clean` first to force recompile!
SQLX_OFFLINE=true cargo zigbuild --release --target aarch64-unknown-linux-musl -p cr-web

# 3. Upload binary to VPS + replace in container + restart (~10s)
scp -P 2222 target/aarch64-unknown-linux-musl/release/cr-web root@46.225.101.253:/tmp/cr-web-new
ssh -p 2222 root@46.225.101.253 "docker cp /tmp/cr-web-new cr-web-1:/app/cr-web && docker compose -f /opt/cr/docker-compose.yml restart web"

# 4. Health check (MUST return 200)
curl -s -o /dev/null -w "%{http_code}" https://ceskarepublika.wiki/health

# 5. Playwright verify (from local PC)
```

**For template/static file changes** (no Rust recompilation needed):
```bash
ssh -p 2222 root@46.225.101.253 "docker cp /opt/cr/cr-web/templates/. cr-web-1:/app/templates/ && docker cp /opt/cr/cr-web/static/. cr-web-1:/app/static/ && docker compose -f /opt/cr/docker-compose.yml restart web"
```

**Full Docker rebuild (ONLY when Dockerfile or system dependencies change, ~4min):**
```bash
rsync -avz --delete --exclude 'target/' --exclude '.git/' --exclude '.env' --exclude 'data/images/' --exclude 'data/porovnani/' -e "ssh -p 2222" ~/Olbrasoft/cr/ root@46.225.101.253:/opt/cr/
ssh -p 2222 root@46.225.101.253 "cd /opt/cr && docker compose build web && docker compose up -d web"
```

### Deploy Rules (MANDATORY)

1. **NEVER run `docker compose build` on the server** for routine deploys — it takes 4+ minutes and the server has limited resources. Use local cross-compile + scp instead.
2. **Use `cargo zigbuild`** with `aarch64-unknown-linux-musl` target (static linking). Regular `cargo build --target aarch64-unknown-linux-gnu` does NOT work — glibc version mismatch.
3. **If migration files were modified**, run `cargo clean` before `cargo zigbuild` — cargo caches embedded migration checksums and won't detect file content changes without a clean build.
4. **After deploy, ALWAYS health check** — if `/health` returns non-200, check `docker logs cr-web-1` immediately.
5. **If startup fails with "migration was previously applied but has been modified"**, the fix is to update `_sqlx_migrations.checksum` in the DB using **SHA-384** (not SHA-256!) of the current file content. See "SQLx Migration Checksum Rules" above.

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
