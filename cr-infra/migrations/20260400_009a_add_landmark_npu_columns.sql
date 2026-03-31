-- Add NPÚ columns to landmarks (previously added manually during data import)
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS npu_catalog_id TEXT;
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS npu_uskp_id TEXT;
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS orp_id INTEGER REFERENCES orp(id);
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS photo_count SMALLINT NOT NULL DEFAULT 0;
