-- #366 — dedup key now includes the container format.
--
-- Before: UNIQUE (source_url, quality)
-- After:  UNIQUE (source_url, quality, format_ext)
--
-- The original key conflated a MP4 720p and a WebM 720p of the same
-- source URL, forcing the second request to collide on insert. With
-- the user-picked format selector from #366 we need both to coexist
-- as independent library rows.

ALTER TABLE videos DROP CONSTRAINT IF EXISTS videos_source_quality_unique;

ALTER TABLE videos
    ADD CONSTRAINT videos_source_quality_container_unique
    UNIQUE (source_url, quality, format_ext);
