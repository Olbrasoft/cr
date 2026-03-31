-- Add NPÚ columns to landmarks (previously added manually during data import)
-- Note: orp_id is added in migration 010, photo_count in migration 012
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS npu_catalog_id TEXT;
ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS npu_uskp_id TEXT;
