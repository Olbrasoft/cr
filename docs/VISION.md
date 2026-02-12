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

## 6. Recommended Development Roadmap

1. Set up GitHub project and configure Hetzner VPS
2. Configure Cloudflare DNS and R2 Bucket
3. Create basic Docker Compose (Rust + Postgres)
4. Implement Router table for slugs as first backend component
5. GitHub Actions deployment pipeline
6. Previo.cz API integration

---

*This document captures the vision discussed in Gemini. It will evolve as implementation progresses.*
