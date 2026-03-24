# Data Model — Future Entities

> This document describes data models for **Phases 2-6** (monuments, accommodation, real estate, users).
> For Phase 1 territorial entities (regions, districts, ORP, municipalities), see [BLUEPRINT.md](BLUEPRINT.md).

---

## Monuments (Phase 2)

### Monument

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `name` | VARCHAR(300) NOT NULL | Monument name (e.g., "Karlstejn") |
| `slug` | VARCHAR(350) NOT NULL UNIQUE | SEO-friendly identifier |
| `description` | TEXT | Textual description |
| `description_embedding` | vector(1536) | pgvector embedding for semantic search |
| `latitude` | DOUBLE PRECISION | GPS latitude (WGS84) |
| `longitude` | DOUBLE PRECISION | GPS longitude (WGS84) |
| `municipality_id` | INT NOT NULL REFERENCES municipalities(id) | Location |
| `monument_type` | VARCHAR(50) NOT NULL | castle, chateau, lookout_tower, museum, church, natural, other |
| `rating_avg` | REAL | Average user rating (1.0-5.0), computed |
| `rating_count` | INT DEFAULT 0 | Number of ratings |
| `created_by` | INT NOT NULL | |
| `created_at` | TIMESTAMPTZ DEFAULT NOW() | |

### Monument Tags

Many-to-many relationship for classification:

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `name` | VARCHAR(100) NOT NULL UNIQUE | Tag name |
| `slug` | VARCHAR(120) NOT NULL UNIQUE | SEO slug |

Join table: `monument_tags` (`monument_id`, `tag_id`)

### Monument Types

| Type slug | Czech name |
|-----------|-----------|
| `castle` | Hrad |
| `chateau` | Zamek |
| `lookout_tower` | Rozhledna |
| `museum` | Muzeum |
| `church` | Kostel |
| `natural` | Prirodni pamatka |
| `other` | Ostatni |

### Data Sources for Monuments

- **hrady.cz** — reference for basic data (name, GPS, type). Factual data only.
- Textual descriptions will be original (not copied).
- Photos from freely available sources or original.

---

## Photos (Phase 2)

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `url` | TEXT NOT NULL | URL on external storage (Cloudflare R2) |
| `caption` | VARCHAR(500) | Photo caption |
| `sort_order` | INT DEFAULT 0 | Order in gallery |
| `monument_id` | INT REFERENCES monuments(id) | FK to monument (nullable — photos can belong to other entities too) |
| `accommodation_id` | INT REFERENCES accommodations(id) | FK to accommodation (nullable) |
| `created_at` | TIMESTAMPTZ DEFAULT NOW() | |

Photos are stored on **Cloudflare R2**, not on the application server. URL structure mirrors web hierarchy:

```
ceskarepublika.wiki/img/stredocesky-kraj/benesov/hotel-posta/foto1.webp
ceskarepublika.wiki/img/pamatky/karlstejn/hlavni.webp
```

All images converted to **WebP** format for optimal size.

---

## User Ratings (Phase 2)

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `monument_id` | INT NOT NULL REFERENCES monuments(id) | |
| `user_id` | INT REFERENCES users(id) | NULL for anonymous ratings (future) |
| `rating` | SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5) | 1-5 stars |
| `created_at` | TIMESTAMPTZ DEFAULT NOW() | |

### User Comments (Phase 2)

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `monument_id` | INT NOT NULL REFERENCES monuments(id) | |
| `user_id` | INT REFERENCES users(id) | |
| `content` | TEXT NOT NULL | Comment text |
| `is_approved` | BOOLEAN DEFAULT FALSE | Moderation flag |
| `created_at` | TIMESTAMPTZ DEFAULT NOW() | |

---

## Accommodation (Phase 4)

### Accommodation Entity

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `name` | VARCHAR(300) NOT NULL | Hotel/pension name |
| `slug` | VARCHAR(350) NOT NULL UNIQUE | SEO slug |
| `accommodation_type` | VARCHAR(50) NOT NULL | hotel, pension, camp, apartment, hostel |
| `description` | TEXT | |
| `latitude` | DOUBLE PRECISION | GPS |
| `longitude` | DOUBLE PRECISION | GPS |
| `municipality_id` | INT NOT NULL REFERENCES municipalities(id) | |
| `phone` | VARCHAR(50) | |
| `email` | VARCHAR(200) | |
| `website` | VARCHAR(500) | |
| `previo_id` | VARCHAR(100) | ID in Previo.cz reservation system |
| `owner_verified` | BOOLEAN DEFAULT FALSE | Whether the operator has claimed the listing |
| `rating_avg` | REAL | |
| `rating_count` | INT DEFAULT 0 | |
| `created_by` | INT NOT NULL | |
| `created_at` | TIMESTAMPTZ DEFAULT NOW() | |

### Accommodation Types

| Type slug | Czech name |
|-----------|-----------|
| `hotel` | Hotel |
| `pension` | Penzion |
| `camp` | Kemp |
| `apartment` | Apartman |
| `hostel` | Hostel |

### Owner Verification Process

1. Operator notices reservations coming through the portal (via Previo)
2. Contacts us or uses "I am the operator of this facility" form
3. Identity verified (email from hotel domain, phone contact, etc.)
4. After verification, gains access to edit their listing (photos, descriptions, contacts)
5. Previo reservation integration is preserved

---

## Real Estate (Phase 5)

### Real Estate Listing

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `title` | VARCHAR(500) NOT NULL | Listing title |
| `listing_type` | VARCHAR(20) NOT NULL | sale, rent |
| `category` | VARCHAR(50) NOT NULL | apartment, house, land, commercial |
| `price` | BIGINT | Price in CZK (NULL if negotiable) |
| `municipality_id` | INT NOT NULL REFERENCES municipalities(id) | |
| `source` | VARCHAR(100) NOT NULL | External source (e.g., "digi-reality") |
| `external_url` | TEXT NOT NULL | Link to original listing |
| `expires_at` | TIMESTAMPTZ | Listing expiration date |
| `is_active` | BOOLEAN DEFAULT TRUE | |
| `created_at` | TIMESTAMPTZ DEFAULT NOW() | |

Real estate listings are **imported from external sources** (Digi-reality or similar aggregators) and periodically refreshed. Detail pages link back to the source — no own detail pages for real estate.

---

## Users and Roles (Phase 6)

### Users

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `username` | VARCHAR(100) NOT NULL UNIQUE | |
| `email` | VARCHAR(300) NOT NULL UNIQUE | |
| `password_hash` | TEXT NOT NULL | argon2 hash |
| `role` | VARCHAR(50) NOT NULL DEFAULT 'user' | admin, editor, operator, user |
| `is_active` | BOOLEAN DEFAULT TRUE | |
| `created_at` | TIMESTAMPTZ DEFAULT NOW() | |

### Roles

| Role | Permissions |
|------|------------|
| `system` | Automated imports (seed user, created_by references) |
| `admin` | Full control over all content |
| `editor` | Edit/add content (wiki-style editing) |
| `operator` | Manage own accommodation listing only |
| `user` | Comments, ratings |

### Edit History (Wiki Feature)

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | |
| `entity_type` | VARCHAR(50) NOT NULL | monument, accommodation, municipality, etc. |
| `entity_id` | INT NOT NULL | |
| `user_id` | INT NOT NULL REFERENCES users(id) | |
| `diff` | JSONB NOT NULL | Changes made |
| `approved` | BOOLEAN DEFAULT FALSE | Moderation status |
| `created_at` | TIMESTAMPTZ DEFAULT NOW() | |

---

## Data Sources Summary

| Source | Data | Format |
|--------|------|--------|
| CSU (Czech Statistical Office) | Territorial hierarchy (regions, districts, ORP, municipalities) | CSV (2.7 MB, ~6,258 records) |
| CSU Geodata Portal | GeoJSON boundary polygons | GeoJSON (~300 MB, S-JTSK format) |
| CUZK — RUIAN | GPS coordinates of address points | CSV (~6,000 files, Windows-1250 encoding) |
| hrady.cz | Basic monument data (name, location, GPS) | Web scraping / manual |
| Previo.cz | Accommodation facilities and availability | API |
| Digi-reality | Real estate listings | API / feed |

### Coordinate System Notes

- GeoJSON data from CSU uses **S-JTSK** coordinate system (Czech national system)
- Conversion to **WGS84** (GPS) required for web map display
- RUIAN CSV files use **Windows-1250** encoding

---

## Initial Data Management

### Development Phase
- Data imported from CSV/external sources via CLI scripts
- Database can be easily replaced with a new version (drop + reimport)
- No admin UI — manipulation via scripts and CLI

### Production Phase (Phase 6+)
- Web admin interface for content management
- Direct editing of imported data via web
- Wiki-style editing for registered users
- Moderation and approval workflow
