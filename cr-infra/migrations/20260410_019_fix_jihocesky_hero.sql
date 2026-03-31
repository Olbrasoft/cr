-- Fix Jihočeský kraj hero: reassign from "zámek" Omlenice to "Jindřichův Hradec" castle
-- Safe: no-op on empty CI database (WHERE clause won't match any rows)
UPDATE landmark_photos SET npu_catalog_id = '1000147769', r2_key = 'landmarks/1000147769-2.webp'
WHERE npu_catalog_id = '1000125654' AND photo_index = 2;

-- Update region hero_landmark_id to correct landmark (no-op if landmark doesn't exist)
UPDATE regions SET hero_landmark_id = sub.id
FROM (SELECT id FROM landmarks WHERE npu_catalog_id = '1000147769' LIMIT 1) sub
WHERE regions.slug = 'jihocesky-kraj';
