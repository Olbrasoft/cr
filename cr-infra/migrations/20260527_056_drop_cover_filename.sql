-- Issue #577 — drop cover_filename from films / series / tv_shows.
--
-- Covers have been keyed by the immutable `id` since Sub A (#579) migrated
-- every existing file on R2 to `{table_prefix}/{id}/cover{,-large}.webp`.
-- The Rust handler and all Python writers have stopped reading/writing
-- the column. Remove it so slug renames no longer have to coordinate with
-- a cover_filename UPDATE, and so mismatches between the two can't
-- silently re-appear.
--
-- audiobooks.cover_filename is intentionally left alone — its layout is
-- `archive.org/download/{archive_id}/{cover_filename}` and doesn't follow
-- the same id-keyed pattern.

ALTER TABLE films      DROP COLUMN cover_filename;
ALTER TABLE series     DROP COLUMN cover_filename;
ALTER TABLE tv_shows   DROP COLUMN cover_filename;
