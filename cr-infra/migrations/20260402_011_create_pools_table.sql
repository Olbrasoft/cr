-- Swimming pools, aquaparks, outdoor pools, natural swimming spots

CREATE TABLE pools (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL UNIQUE,
    description     TEXT,
    address         TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    website         TEXT,
    email           TEXT,
    phone           TEXT,
    facebook        TEXT,
    facilities      TEXT,
    pool_length_m   INT,
    is_aquapark     BOOLEAN NOT NULL DEFAULT FALSE,
    is_indoor       BOOLEAN NOT NULL DEFAULT FALSE,
    is_outdoor      BOOLEAN NOT NULL DEFAULT FALSE,
    is_natural      BOOLEAN NOT NULL DEFAULT FALSE,
    photo_count     SMALLINT NOT NULL DEFAULT 0,
    municipality_id INT REFERENCES municipalities(id),
    orp_id          INT REFERENCES orp(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pools_municipality ON pools(municipality_id);
CREATE INDEX idx_pools_orp ON pools(orp_id);
CREATE INDEX idx_pools_aquapark ON pools(is_aquapark) WHERE is_aquapark;
CREATE INDEX idx_pools_indoor ON pools(is_indoor) WHERE is_indoor;
CREATE INDEX idx_pools_outdoor ON pools(is_outdoor) WHERE is_outdoor;
CREATE INDEX idx_pools_natural ON pools(is_natural) WHERE is_natural;
