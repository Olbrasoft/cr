# Functional Modules

> Specification of all functional modules and their capabilities across project phases.

---

## Module: Territorial Units (Core — Phase 1)

The foundation of the entire portal. All other modules connect to it.

### Features
- **Hierarchical navigation:** Regions -> Districts -> ORP -> Municipalities -> (Parts of municipalities)
- **Breadcrumb navigation:** Path in hierarchy displayed on every page
- **Detail page:** Name, statistics (number of subordinate units), description, map
- **Interactive map:** Map with boundary polygons (future phase)
- **GPS coordinates:** Municipality location on map (from RUIAN data)
- **Drill-down:** From region to districts, from district to ORP, from ORP to municipalities
- **Prague:** Special case — acts as both region and city

### Municipality Detail Page Shows
- Basic information about the municipality
- List of monuments in the municipality (from Monuments module)
- Accommodation nearby (from Accommodation module)
- Real estate listings in the municipality (from Real Estate module)
- Link to map with surroundings

---

## Module: Monuments (Phase 2)

Catalog of historic and cultural monuments of the Czech Republic.

### Features
- **Monument catalog:** Listing with photos and descriptions
- **Detail page:** Name, description, photo gallery, map with location, ratings
- **Monument types:** Castles, chateaux, lookout towers, museums, churches, natural monuments
- **Municipality link:** Each monument belongs to a specific municipality
- **Tags and categories:** Classification for filtering
- **User ratings:** 1-5 star ratings
- **Comments:** User reviews and observations
- **Photo gallery:** Multiple photos per monument, ordering, captions
- **Interactive map:** Map with all monuments in the area

### URL Subpages
- `/pamatky-{city}/` — List of monuments in city/municipality
- `/pamatky-{city}/{monument-slug}/` — Monument detail
- `/hrady-{city}/` — Castles only in city
- `/zamky-{city}/` — Chateaux only in city
- `/rozhledny-{city}/` — Lookout towers only

---

## Module: Accommodation (Phase 4)

Reservation and presentation module for accommodation facilities.

### Features
- **Accommodation catalog:** Listing of hotels, pensions, camps, apartments
- **Detail page:** Name, description, photo gallery, contact, map, ratings
- **Reservation form:** Date selection, number of guests, reservation submission
- **Previo.cz integration:** Real-time availability and price queries via API
- **Nearby monuments:** Monument display on accommodation detail page
- **Proximity on municipality page:** Link to nearby accommodation from municipality page
- **Filtering:** By type, price, rating, distance from monument
- **Owner takeover:** Operator can verify identity and take over presentation management

### URL Subpages
- `/ubytovani-{city}/` — All accommodation in city
- `/hotely-{city}/` — Hotels only
- `/penziony-{city}/` — Pensions only
- `/kempy-{city}/` — Camps only
- `/ubytovani-{city}/{facility-slug}/` — Specific facility detail

### Previo.cz Integration Flow
```
Visitor -> Municipality/monument page -> "Accommodation nearby"
  -> Select hotel -> Reservation form (date, guests)
  -> Submit via Previo API -> Confirmation
  -> Commission for CeskaRepublika.wiki
```

---

## Module: Real Estate (Phase 5)

Display of real estate listings in the context of territorial units.

### Features
- **Listing aggregation:** Import from external sources (Digi-reality etc.)
- **Contextual display:** Municipality page shows available real estate in that area
- **Filtering:** By type (sale/rent), category (apartment/house/land), price
- **Link to source:** Click-through to original listing at the real estate agency
- **Regular updates:** Automatic import of new and removal of expired listings

### URL Subpages
- `/reality-{city}/` — Real estate listings in city/municipality
- Detail pages likely won't be own — click-through to source

---

## Module: Search (Phase 3)

Smart search across the entire portal.

### Features
- **Fulltext search:** Classic text search across all entities with Czech language support
- **Vector (semantic) search:** Meaning-based search, not just word matching
  - Example: query "romantic place for a weekend" finds castles, chateaux, and pensions in nature
  - Uses vector embeddings stored in PostgreSQL (pgvector)
- **Autocomplete / suggestions:** Suggestions while typing in search field
- **Combined results:** Results from all modules (municipalities, monuments, accommodation, real estate)
- **Result filtering:** Ability to limit to specific content type or region

### Endpoints
- `/hledani/` — Search page
- `/hledani/?q={query}` — Search results
- `/api/v1/search/` — Search API (internal)
- `/api/v1/suggest/` — Autocomplete API (internal)

---

## Module: Images and Media (Phase 2+)

Visual content management for the portal.

### Features
- **External storage:** Images stored on Cloudflare R2 (not on the application server)
- **WebP format:** All images converted to WebP for optimal size
- **CDN distribution:** Images served via Cloudflare CDN for fast loading
- **URL structure mirrors web hierarchy:**
  ```
  ceskarepublika.wiki/img/stredocesky-kraj/benesov/hotel-posta/foto.webp
  ```
- **Lazy loading:** Images load only when visible on screen
- **Gallery:** Photo browsing with navigation (previous/next)

### Why Separate from Server
- Cheaper — Cloudflare R2 has generous free tier (10 GB, zero egress)
- Application server doesn't serve static content (saves CPU and bandwidth)
- Independent scaling of storage and application

---

## Module: Articles and Blog (Phase 7 — Future)

### Features
- `/clanky/` — Article listing
- `/clanky/{slug}/` — Article detail
- SEO content to attract visitors from search engines
- Linked to territorial units and monuments

---

## Module: Administration (Phase 6)

### Features
- Web interface for managing all data
- Editing imported territorial data
- Monument management (add, edit, delete)
- Accommodation facility management
- User content moderation (comments, ratings)
- User and role management
- Wiki-style editing with change history

---

## Module: Static Pages

### Pages
- `/o-projektu/` — About the portal
- `/kontakt/` — Contact information
- `/podminky-pouziti/` — Terms of use
- `/ochrana-soukromi/` — Privacy policy
