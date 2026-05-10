-- Backfill film-level SK Torrent columns that exist in production but were
-- never explicitly added to the `films` table by an in-repo migration.
--
-- History: migration 026 (`add_sktorrent_columns`) used `ALTER TABLE IF
-- EXISTS movies / IF EXISTS films` to add `sktorrent_video_id` and
-- `sktorrent_cdn`. On the original prod DB those ALTERs landed on the
-- legacy `movies` table and survived the rename to `films`. On a fresh CI
-- DB neither table exists at that point, so both ALTERs are no-ops, the
-- subsequent `CREATE TABLE films` in migration 028 creates the table
-- without any sktorrent columns, and nothing in the migration history
-- adds them back. Prod and CI schemas have silently diverged ever since
-- (`has_dub`, `has_subtitles`, `sktorrent_qualities` followed the same
-- path via undocumented manual ALTERs on prod).
--
-- Migration 068 below is the first migration that actually references
-- these columns at parse time (in the `_films_merge_map` snapshot CTE),
-- which surfaces the drift as a CI failure. This migration squashes the
-- gap: every column is `ADD COLUMN IF NOT EXISTS`, so prod is a strict
-- no-op while CI gets the missing columns before 068 runs.
ALTER TABLE films
    ADD COLUMN IF NOT EXISTS sktorrent_video_id  INTEGER,
    ADD COLUMN IF NOT EXISTS sktorrent_cdn       SMALLINT,
    ADD COLUMN IF NOT EXISTS sktorrent_qualities VARCHAR(50),
    ADD COLUMN IF NOT EXISTS has_dub             BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS has_subtitles       BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_films_sktorrent_video_id ON films (sktorrent_video_id);
