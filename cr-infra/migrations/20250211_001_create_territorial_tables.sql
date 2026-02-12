-- Territorial hierarchy tables: regions → districts → orp → municipalities
-- All primary keys are SERIAL (i32). All slugs are unique within their scope.

CREATE TABLE regions (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    region_code TEXT NOT NULL UNIQUE,
    nuts_code   TEXT NOT NULL UNIQUE,
    created_by  INT NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE districts (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    slug          TEXT NOT NULL,
    district_code TEXT NOT NULL UNIQUE,
    region_id     INT NOT NULL REFERENCES regions(id) ON DELETE RESTRICT,
    created_by    INT NOT NULL DEFAULT 1,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(slug, region_id)
);

CREATE INDEX idx_districts_region_id ON districts(region_id);

CREATE TABLE orp (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL,
    orp_code    TEXT NOT NULL UNIQUE,
    district_id INT NOT NULL REFERENCES districts(id) ON DELETE RESTRICT,
    created_by  INT NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(slug, district_id)
);

CREATE INDEX idx_orp_district_id ON orp(district_id);

CREATE TABLE municipalities (
    id                SERIAL PRIMARY KEY,
    name              TEXT NOT NULL,
    slug              TEXT NOT NULL,
    municipality_code TEXT NOT NULL UNIQUE,
    pou_code          TEXT NOT NULL,
    orp_id            INT NOT NULL REFERENCES orp(id) ON DELETE RESTRICT,
    created_by        INT NOT NULL DEFAULT 1,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(slug, orp_id)
);

CREATE INDEX idx_municipalities_orp_id ON municipalities(orp_id);
