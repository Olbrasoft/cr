-- Make landmark slugs unique within ORP scope.
-- URL structure: /{region}/{orp}/{slug}/ requires slug unique per ORP.

-- Step 1: Add orp_id column
ALTER TABLE landmarks ADD COLUMN orp_id INT REFERENCES orp(id);

-- Step 2: Populate orp_id from municipalities
UPDATE landmarks l
SET orp_id = m.orp_id
FROM municipalities m
WHERE l.municipality_id = m.id;

-- Step 3: Drop old constraint first (was per-municipality, need per-ORP)
ALTER TABLE landmarks DROP CONSTRAINT landmarks_slug_municipality_id_key;

-- Step 4: Pass 1 - For all colliding slugs within ORP, append municipality slug
WITH collisions AS (
    SELECT l.slug, l.orp_id
    FROM landmarks l
    GROUP BY l.slug, l.orp_id
    HAVING COUNT(*) > 1
)
UPDATE landmarks l
SET slug = l.slug || '-' || m.slug
FROM municipalities m, collisions c
WHERE l.municipality_id = m.id
  AND l.slug = c.slug
  AND l.orp_id = c.orp_id;

-- Step 5: Pass 2 - Fix any remaining collisions (cross-collisions)
-- Keep lowest id unchanged, append sequential number to others
WITH remaining AS (
    SELECT l.id, l.slug, l.orp_id,
           ROW_NUMBER() OVER (PARTITION BY l.slug, l.orp_id ORDER BY l.id) as rn
    FROM landmarks l
    WHERE (l.slug, l.orp_id) IN (
        SELECT l2.slug, l2.orp_id
        FROM landmarks l2
        GROUP BY l2.slug, l2.orp_id
        HAVING COUNT(*) > 1
    )
)
UPDATE landmarks l
SET slug = r.slug || '-' || r.rn
FROM remaining r
WHERE l.id = r.id AND r.rn > 1;

-- Step 6: Add new unique constraint (per ORP)
CREATE UNIQUE INDEX idx_landmarks_slug_orp ON landmarks(slug, orp_id);
CREATE INDEX idx_landmarks_orp ON landmarks(orp_id);
