-- Rename image URL columns to extension-only columns.
-- Full path is always: /img/{entity_type}/{code}/coat-of-arms.{ext}
-- So we only need to store the extension (svg, webp, png).

-- Regions
ALTER TABLE regions DROP COLUMN IF EXISTS coat_of_arms_url;
ALTER TABLE regions DROP COLUMN IF EXISTS flag_url;
ALTER TABLE regions ADD COLUMN coat_of_arms_ext TEXT;
ALTER TABLE regions ADD COLUMN flag_ext TEXT;

-- Municipalities
ALTER TABLE municipalities DROP COLUMN IF EXISTS coat_of_arms_url;
ALTER TABLE municipalities DROP COLUMN IF EXISTS flag_url;
ALTER TABLE municipalities ADD COLUMN coat_of_arms_ext TEXT;
ALTER TABLE municipalities ADD COLUMN flag_ext TEXT;
