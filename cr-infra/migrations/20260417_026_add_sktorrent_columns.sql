-- Add sktorrent.eu video hosting columns to movies table
-- online.sktorrent.eu stores CZ/SK dubbed films with direct MP4 URLs (no tokens)

ALTER TABLE movies ADD COLUMN IF NOT EXISTS sktorrent_video_id INTEGER;
ALTER TABLE movies ADD COLUMN IF NOT EXISTS sktorrent_cdn SMALLINT;

CREATE INDEX IF NOT EXISTS idx_movies_sktorrent_video_id ON movies(sktorrent_video_id);
