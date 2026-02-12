# CeskaRepublika.wiki — Technical Vision

> Concept document created from Gemini brainstorming session. This is a proof-of-concept vision for the future direction of the project.

## 1. Identity & SEO Strategy

- **Domain:** `ceskarepublika.wiki` (chosen for high trustworthiness, encyclopedic character, better CTR)
- **Language mutation:** `ceskarepublika.wiki/en/` (subdirectory to share domain authority)

### URL Structure (Web)

| Content Type | Pattern | Example |
|---|---|---|
| Territorial entities | `domain/kraj/orp/obec` | `/stredocesky-kraj/benesov-u-prahy/lhota` |
| Landmarks | `domain/pamatky/obec/nazev-pamatky` | `/pamatky/lhota/zamek-lhota` |
| Accommodation | `domain/ubytovani/obec` | `/ubytovani/benesov` |

## 2. Backend & Database (Rust & PostgreSQL)

- **Technology:** Rust (high performance, low memory footprint on VPS)
- **Database:** PostgreSQL in standalone Docker container (isolation + performance)

### Routing System

- Unique **Router table** mapping unique URL path (slug) → `EntityID` + `EntityType`
- Unique index on `slug` column to prevent duplicate URLs
- Connection pooling via SQLx for fast DB response

### Deployment

- Automated via **GitHub Actions**
- Compilation happens on GitHub side, server receives ready binary/image (saves VPS resources)

## 3. Infrastructure & Hosting

| Component | Choice |
|---|---|
| Hosting | Hetzner Cloud (Falkenstein/Nuremberg, Germany) |
| Plan | Shared Cost-Optimized (e.g. CAX11 ARM64 ~€3.80/month) |
| OS | Ubuntu 24.04 LTS via Docker Compose |
| CDN & Security | Cloudflare (DNS, SSL, DDoS protection) |

### Firewall

- Hetzner firewall: ports 80/443 accept requests exclusively from Cloudflare IP ranges

## 4. Image & Static Content Management

- **Storage:** Cloudflare R2 (10 GB Free Tier, zero egress fees)

### Proxying

- Web uses URLs like `ceskarepublika.wiki/img/...`
- Cloudflare rule (Transform Rule/Worker) redirects `/img/` path directly to R2 bucket
- Hetzner server (Rust) does NOT serve images → saves CPU and bandwidth

### IMG URL Structure (mirrors web hierarchy)

```
ceskarepublika.wiki/img/stredocesky-kraj/benesov-u-prahy/lhota/naves.webp
ceskarepublika.wiki/img/pamatky/lhota/zamek-lhota/brana.webp
```

- **Format:** Always WebP (upload and conversion handled by Rust backend)

## 5. Business Model (Monetization)

### Primary Partner: Previo.cz

- Integration with thousands of Czech accommodation providers
- **Real-time queries** for availability and prices via Previo API
- **Reservation forms** directly on the site (custom Rust UI), data sent to Previo in background
- DB binding: each entity (municipality/landmark) has optional `previo_id` field

## 6. Email Strategy

### Receiving Email (info@ceskarepublika.wiki)

- **Solution:** Cloudflare Email Routing (free)
- **How it works:** `info@ceskarepublika.wiki` → forwarded to personal Gmail
- **No server-side mail setup** — no Postfix/Dovecot, no IP reputation issues
- **Address:** `info@ceskarepublika.wiki` (universal, trustworthy for encyclopedic portal)
- **Alternative:** `redakce@ceskarepublika.wiki` (editorial feel, good for wiki/magazine)

### Sending Email (transactional from Rust backend)

- **Do NOT send from VPS directly** — Hetzner IPs have poor mail reputation
- **Use a dedicated service:** Resend, Brevo, Mailgun, or Postmark
- **Resend** recommended (developer-friendly, good free tier, Rust SDK)

### Required DNS Records (to be set up in Cloudflare)

| Record | Type | Value | Purpose |
|--------|------|-------|---------|
| `@` | MX | Cloudflare Email Routing | Receive mail |
| `@` | TXT | SPF record | Authorize mail senders |
| `selector._domainkey` | TXT | DKIM key | Email authentication |

## 7. Recommended Development Roadmap

| # | Step | Status |
|---|------|--------|
| 1 | Set up GitHub project | Done |
| 2 | Configure Hetzner VPS (CAX11 ARM64, Ubuntu 24.04, SSH hardened) | Done |
| 3 | Create Docker Compose (Rust + Postgres) | Done |
| 4 | Implement territorial hierarchy (Regions → Districts → ORP → Municipalities) | Done |
| 5 | CSV import from ČSÚ data (14 regions, 77 districts, 206 ORP, 6,258 municipalities) | Done |
| 6 | Basic Axum + Askama SSR (homepage, region, ORP, municipality pages) | Done |
| 7 | SEO-friendly URL structure (`/kraj/orp/obec/`) | Done |
| 8 | Register domain `ceskarepublika.wiki` (Spaceship.com) | Done |
| 9 | DNS A records pointing to Hetzner server | Done |
| 10 | **Configure Cloudflare** (DNS proxy, SSL, CDN, Email Routing) | **Next** |
| 11 | Cloudflare R2 Bucket for images | Planned |
| 12 | GitHub Actions deployment pipeline | Planned |
| 13 | Previo.cz API integration | Planned |

---

*This document captures the vision discussed in Gemini. It will evolve as implementation progresses.*
*Last updated: 2026-02-12*
