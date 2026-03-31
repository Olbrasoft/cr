-- Fix Jihočeský kraj hero: reassign from "zámek" Omlenice to "Jindřichův Hradec" castle
-- Move landmark_photos entry to correct NPÚ catalog ID
UPDATE landmark_photos SET npu_catalog_id = '1000147769', r2_key = 'landmarks/1000147769-2.webp'
WHERE npu_catalog_id = '1000125654' AND photo_index = 2;

-- Update region hero_landmark_id to correct landmark
UPDATE regions SET hero_landmark_id = (SELECT id FROM landmarks WHERE npu_catalog_id = '1000147769' LIMIT 1)
WHERE slug = 'jihocesky-kraj';
