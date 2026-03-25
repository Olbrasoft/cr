-- Landmarks (památky) - castles, chateaux, churches, museums, etc.

CREATE TABLE landmark_types (
    id   SERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL
);

INSERT INTO landmark_types (slug, name) VALUES
    ('castle', 'Hrad'),
    ('chateau', 'Zámek'),
    ('church', 'Kostel'),
    ('monastery', 'Klášter'),
    ('museum', 'Muzeum'),
    ('lookout_tower', 'Rozhledna'),
    ('ruins', 'Zřícenina'),
    ('other', 'Ostatní');

CREATE TABLE landmarks (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL,
    wikidata_id     TEXT UNIQUE,
    type_id         INT NOT NULL REFERENCES landmark_types(id),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    municipality_id INT REFERENCES municipalities(id),
    description     TEXT,
    wikipedia_url   TEXT,
    image_ext       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(slug, municipality_id)
);

CREATE INDEX idx_landmarks_type ON landmarks(type_id);
CREATE INDEX idx_landmarks_municipality ON landmarks(municipality_id);
