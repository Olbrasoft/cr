-- Hero photo can reference a municipality photo
ALTER TABLE regions ADD COLUMN hero_municipality_code TEXT;
ALTER TABLE regions ADD COLUMN hero_municipality_photo_index SMALLINT DEFAULT 2;
