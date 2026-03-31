-- Table for additional landmark photos (index 2+)
-- Primary photo (index 1) is managed by photo_metadata table
CREATE TABLE landmark_photos (
    id SERIAL PRIMARY KEY,
    npu_catalog_id TEXT NOT NULL,
    photo_index SMALLINT NOT NULL DEFAULT 2,
    slug TEXT NOT NULL,
    r2_key TEXT NOT NULL,
    description TEXT,
    source_url TEXT,
    width SMALLINT,
    height SMALLINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(npu_catalog_id, photo_index)
);

CREATE INDEX idx_landmark_photos_catalog ON landmark_photos(npu_catalog_id);

-- Hero photo reference on regions
ALTER TABLE regions ADD COLUMN hero_landmark_id INTEGER REFERENCES landmarks(id);
ALTER TABLE regions ADD COLUMN hero_photo_index SMALLINT DEFAULT 2;
