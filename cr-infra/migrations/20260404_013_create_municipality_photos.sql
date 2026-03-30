-- Municipality photos from Wikipedia
-- Stores metadata for photos displayed on municipality pages.

CREATE TABLE municipality_photos (
    id SERIAL PRIMARY KEY,
    municipality_code TEXT NOT NULL,
    photo_index SMALLINT NOT NULL DEFAULT 1,
    slug TEXT NOT NULL,
    object_name TEXT,
    description TEXT,
    r2_key TEXT NOT NULL,
    source_url TEXT,
    wiki_filename TEXT,
    width SMALLINT,
    height SMALLINT,
    is_primary BOOLEAN DEFAULT true,
    UNIQUE (municipality_code, photo_index)
);

CREATE INDEX idx_municipality_photos_code ON municipality_photos(municipality_code);
