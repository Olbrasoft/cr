-- Add sktorrent.eu video hosting columns.
-- online.sktorrent.eu stores CZ/SK dubbed films with direct MP4 URLs (no tokens).
--
-- Historical note: when this migration was first written the table was
-- named `movies`. It was renamed to `films` before 028. To keep this
-- migration idempotent on existing DBs AND runnable on a fresh CI DB
-- (where `movies` never exists), we guard both sides with IF EXISTS
-- and duplicate the ALTERs onto `films` as well. Either one applies,
-- the other is a silent no-op — sqlx replays the whole history.

ALTER TABLE IF EXISTS movies ADD COLUMN IF NOT EXISTS sktorrent_video_id INTEGER;
ALTER TABLE IF EXISTS movies ADD COLUMN IF NOT EXISTS sktorrent_cdn SMALLINT;

ALTER TABLE IF EXISTS films ADD COLUMN IF NOT EXISTS sktorrent_video_id INTEGER;
ALTER TABLE IF EXISTS films ADD COLUMN IF NOT EXISTS sktorrent_cdn SMALLINT;

-- Index creation can't use IF EXISTS on the table, but CREATE INDEX IF
-- NOT EXISTS on a non-existent table raises; wrap both in DO blocks.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'movies') THEN
        CREATE INDEX IF NOT EXISTS idx_movies_sktorrent_video_id ON movies(sktorrent_video_id);
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'films') THEN
        CREATE INDEX IF NOT EXISTS idx_films_sktorrent_video_id ON films(sktorrent_video_id);
    END IF;
END$$;
