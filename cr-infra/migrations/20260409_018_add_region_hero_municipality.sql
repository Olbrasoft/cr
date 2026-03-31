-- Hero photo can reference a municipality photo
ALTER TABLE regions ADD COLUMN hero_municipality_code TEXT;
ALTER TABLE regions ADD COLUMN hero_municipality_photo_index SMALLINT DEFAULT 2;

-- Backfill: move 4 regions from region-direct to municipality reference
UPDATE regions SET hero_municipality_code = '569810', hero_photo_r2_key = NULL WHERE slug = 'kralovehradecky-kraj';
UPDATE regions SET hero_municipality_code = '554821', hero_photo_r2_key = NULL WHERE slug = 'moravskoslezsky-kraj';
UPDATE regions SET hero_municipality_code = '564567', hero_photo_r2_key = NULL WHERE slug = 'ustecky-kraj';
UPDATE regions SET hero_municipality_code = '558371', hero_photo_r2_key = NULL WHERE slug = 'plzensky-kraj';
