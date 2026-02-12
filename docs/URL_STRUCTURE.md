# URL Structure — ceska-republika.info

Living document for planning all URLs on the portal.

**Status:** DRAFT — under discussion

---

## Core Principle: Routing Table as Single Source of Truth

**Every URL on the portal is stored in a database routing table.** There are no hardcoded route patterns in code (except `/` homepage and technical endpoints like `/api/`, `/sitemap.xml`).

### Why

- URLs are **SEO-driven** — shaped by what people actually search for, not by data hierarchy
- URLs can **change over time** as SEO data evolves
- **Links between pages** are resolved via the routing table, not constructed in code
- A municipality page doesn't "know" its accommodation URL — it queries the table: "give me the URL for accommodation listing in city_id=452"

### How It Works

```
HTTP request: GET /ubytovani-benesov/hotel-posta/

1. Split path into segments: ["ubytovani-benesov", "hotel-posta"]
2. Look up "ubytovani-benesov" in routing table → page_type=listing, category=accommodation, city_id=452
3. Look up "hotel-posta" as child of that parent → page_type=detail, entity_id=789
4. Render the appropriate template with the resolved entity data
```

### Routing Table Schema (draft)

```sql
CREATE TABLE routes (
    id          SERIAL PRIMARY KEY,
    slug        TEXT NOT NULL,              -- URL segment (e.g., "ubytovani-benesov")
    parent_id   INT REFERENCES routes(id),  -- NULL = top-level, otherwise nested under parent
    page_type   TEXT NOT NULL,              -- "region", "orp", "municipality", "listing", "detail", "static", "article"
    category_id INT,                        -- FK to categories (ubytování, hotely, památky...)
    region_id   INT,                        -- FK to regions (optional context)
    city_id     INT,                        -- FK to municipalities (optional context)
    entity_id   INT,                        -- FK to specific entity (hotel, restaurant, etc.)
    meta_title  TEXT,                       -- SEO <title>
    meta_desc   TEXT,                       -- SEO meta description
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(slug, parent_id)                 -- slug unique within its parent
);

CREATE INDEX idx_routes_slug ON routes(slug);
```

### Two URL Regimes

The portal uses **two different URL strategies** depending on content type:

| Regime | Max Depth | Why |
|--------|-----------|-----|
| **Territorial** | 4+ levels | Full path required to disambiguate duplicates (multiple Benešov, Adamov, etc.) |
| **Commercial/SEO** | 2 levels | Flat, search-friendly URLs driven by what people Google |

---

## 1. Territorial Pages (full hierarchy — DECIDED)

Territorial pages **must use the full hierarchical path** because municipality names repeat across regions. Without the full path, there's no way to distinguish which "Benešov" we mean without adding ugly IDs to the URL.

### Hierarchy: Region → ORP → Municipality → Part

| Depth | Pattern | Example |
|-------|---------|---------|
| 1 | `/{region}/` | `/stredocesky-kraj/` |
| 2 | `/{region}/{orp}/` | `/stredocesky-kraj/benesov/` |
| 3 | `/{region}/{orp}/{municipality}/` | `/stredocesky-kraj/benesov/bystrice/` |
| 4 | `/{region}/{orp}/{municipality}/{part}/` | `/stredocesky-kraj/benesov/bystrice/dolni-lhota/` |

All segments are resolved via the routing table (parent_id chain), not hardcoded Axum routes.

### Why Full Path Is Required

```
/stredocesky-kraj/benesov/benesov/      → Benešov (Středočeský kraj)
/olomoucky-kraj/olomouc/benesov/        → Benešov (Olomoucký kraj) — different municipality!
```

591 municipality names are duplicated. The full path is the only clean disambiguation.

### Prague

`/hlavni-mesto-praha/` — acts as both region page and city page. Has direct content instead of listing a single ORP.

### All 14 Region Slugs

| Region | Slug |
|--------|------|
| Hlavní město Praha | `hlavni-mesto-praha` |
| Středočeský kraj | `stredocesky-kraj` |
| Jihočeský kraj | `jihocesky-kraj` |
| Plzeňský kraj | `plzensky-kraj` |
| Karlovarský kraj | `karlovarsky-kraj` |
| Ústecký kraj | `ustecky-kraj` |
| Liberecký kraj | `liberecky-kraj` |
| Královéhradecký kraj | `kralovehradecky-kraj` |
| Pardubický kraj | `pardubicky-kraj` |
| Kraj Vysočina | `kraj-vysocina` |
| Jihomoravský kraj | `jihomoravsky-kraj` |
| Olomoucký kraj | `olomoucky-kraj` |
| Zlínský kraj | `zlinsky-kraj` |
| Moravskoslezský kraj | `moravskoslezsky-kraj` |

---

## 2. Commercial / Category Listing Pages

Flat, SEO-driven URLs. Category + city combined into one slug.

| URL Example | page_type | Category | City |
|-------------|-----------|----------|------|
| `/hotely-benesov/` | listing | hotels | Benešov |
| `/ubytovani-benesov/` | listing | accommodation (all) | Benešov |
| `/penziony-brno/` | listing | pensions | Brno |
| `/restaurace-cesky-krumlov/` | listing | restaurants | Český Krumlov |
| `/pamatky-kutna-hora/` | listing | monuments | Kutná Hora |

**Note:** `/ubytovani-benesov/` includes hotels too, but with different text/focus than `/hotely-benesov/`.

### Detail Pages (nested under listing)

| URL Example | page_type | Parent |
|-------------|-----------|--------|
| `/ubytovani-benesov/hotel-posta/` | detail | ubytovani-benesov |
| `/hotely-brno/grandhotel/` | detail | hotely-brno |
| `/restaurace-cesky-krumlov/u-dvou-kocouru/` | detail | restaurace-cesky-krumlov |

### Category Slugs (draft — TODO: complete list)

| Category (CZ) | Slug prefix | Notes |
|----------------|-------------|-------|
| Ubytování | `ubytovani-` | All accommodation |
| Hotely | `hotely-` | Hotels only |
| Penziony | `penziony-` | Pensions only |
| Kempy | `kempy-` | Camping |
| Restaurace | `restaurace-` | Restaurants |
| Kavárny | `kavarny-` | Cafés |
| Památky | `pamatky-` | Monuments |
| Hrady a zámky | `hrady-` | Castles |
| Rozhledny | `rozhledny-` | Lookout towers |
| Muzea | `muzea-` | Museums |

---

## 3. Cross-Links Between Pages

A municipality page (e.g., Benešov) wants to show a link "Ubytování v Benešově". It does NOT hardcode `/ubytovani-benesov/`. Instead:

```sql
-- "Give me all listing pages for city_id=452"
SELECT slug, page_type, category_id FROM routes
WHERE city_id = 452 AND page_type = 'listing' AND is_active = TRUE;
```

The template then renders whatever links exist for that city. If tomorrow we add `/wellness-benesov/`, it automatically appears.

---

## 4. Static Pages

These CAN be hardcoded routes (they don't change):

| URL | Page | Routing |
|-----|------|---------|
| `/` | Homepage — 14 regions + SVG map | Hardcoded |
| `/o-projektu/` | About | Routing table (page_type=static) |
| `/kontakt/` | Contact | Routing table |
| `/podminky-pouziti/` | Terms of use | Routing table |
| `/ochrana-soukromi/` | Privacy policy | Routing table |

---

## 5. Search

| URL | Page | Routing |
|-----|------|---------|
| `/hledani/` | Search page | Hardcoded |
| `/hledani/?q={query}` | Search results | Hardcoded |

---

## 6. Blog / Articles (TODO)

| URL | Page | Routing |
|-----|------|---------|
| `/clanky/` | Article listing | Hardcoded or routing table |
| `/clanky/{slug}/` | Article detail | Routing table |

---

## 7. API Endpoints

Hardcoded (not in routing table):

| URL | Purpose |
|-----|---------|
| `/api/v1/search/` | Search API |
| `/api/v1/suggest/` | Autocomplete |

---

## 8. Technical / SEO

Hardcoded:

| URL | Purpose |
|-----|---------|
| `/sitemap.xml` | XML sitemap (generated from routing table!) |
| `/robots.txt` | Crawler rules |

---

## 9. External Integrations (TODO)

| Service | Purpose | Status |
|---------|---------|--------|
| Previo.cz | Hotel/accommodation reservations | Planned |
| ? | Restaurant reservations | ? |
| ? | Ticket booking (monuments) | ? |

---

## Slug Rules

1. Lowercase everything
2. Remove Czech diacritics: č→c, ř→r, š→s, ž→z, ů→u, ú→u, ě→e, ý→y, á→a, í→i, é→e, ó→o, ď→d, ť→t, ň→n
3. Replace spaces with hyphens
4. Keep existing hyphens (Frýdek-Místek → frydek-mistek)
5. Collapse multiple hyphens
6. Trim leading/trailing hyphens

---

## Axum Router (implementation concept)

```rust
// Only hardcoded routes are homepage + technical endpoints
// Everything else is a catch-all that resolves via routing table
Router::new()
    .route("/", get(handlers::homepage))
    .route("/hledani/", get(handlers::search))
    .route("/api/v1/search/", get(api::search))
    .route("/api/v1/suggest/", get(api::suggest))
    .route("/sitemap.xml", get(handlers::sitemap))
    .route("/robots.txt", get(handlers::robots))
    // Catch-all: resolve any path via routing table
    .fallback(get(handlers::resolve_path))
```

The `resolve_path` handler:
1. Splits the URL path into segments
2. Walks the routing table parent_id chain: root → seg1 → seg2 → seg3 → ...
3. If the final segment is found → render the appropriate template based on `page_type`
4. If not found → 404

This supports **any depth** — 1 segment for regions, 2 for ORP, 3 for municipalities, 4 for parts of municipalities, 2 for commercial detail pages — all with the same handler.

---

## Decisions Made

- [x] **Territorial pages use full hierarchical path** (region/orp/municipality/part) — required for disambiguation of duplicate names
- [x] **Commercial pages are flat** (max 2 levels) — SEO-driven
- [x] **Routing table is single source of truth** — all URLs stored in DB
- [x] **Cross-links resolved via routing table** — pages don't hardcode URLs to other pages

## Open Questions

- [ ] Complete list of commercial categories
- [ ] Parts of municipalities (části obcí) — how many exist? Need data analysis
- [ ] Admin area — needed from start or later phase?
- [ ] Blog/articles — needed from start or later phase?
- [ ] Previo.cz integration details
- [ ] What happens when a slug changes (SEO redirect)? 301 redirect table?
- [ ] Should the routing table also store breadcrumb labels or derive them from entities?
