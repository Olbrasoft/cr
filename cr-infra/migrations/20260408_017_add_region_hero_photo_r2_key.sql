-- Direct hero photo for regions (collages, city views)
-- Fallback when hero_landmark_id is NULL
ALTER TABLE regions ADD COLUMN hero_photo_r2_key TEXT;
