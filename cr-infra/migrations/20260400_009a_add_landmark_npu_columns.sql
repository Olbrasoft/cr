-- Add NPÚ columns to landmarks (previously added manually during data import)
-- Note: orp_id is added in migration 010 (landmarks_orp_unique_slug)
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS npu_catalog_id TEXT;
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS npu_uskp_id TEXT;
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS photo_count SMALLINT NOT NULL DEFAULT 0;
