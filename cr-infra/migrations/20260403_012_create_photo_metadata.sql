-- Photo metadata: dimensions for all entity photos (landmarks, pools, etc.)
-- Used for proper aspect ratio rendering (no layout shift) and gallery display.

CREATE TABLE photo_metadata (
    id SERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,          -- 'landmark' or 'pool'
    entity_id INT NOT NULL,
    photo_index SMALLINT NOT NULL,      -- 1-based
    r2_key TEXT NOT NULL,               -- actual key in R2 bucket
    width SMALLINT NOT NULL,
    height SMALLINT NOT NULL,
    file_size INT,
    UNIQUE (entity_type, entity_id, photo_index)
);

CREATE INDEX idx_photo_metadata_entity ON photo_metadata(entity_type, entity_id);

-- Add photo_count to landmarks (pools already has it)
ALTER TABLE landmarks ADD COLUMN photo_count SMALLINT NOT NULL DEFAULT 0;
