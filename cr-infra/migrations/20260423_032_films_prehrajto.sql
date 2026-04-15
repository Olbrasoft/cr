-- Stable Přehraj.to URL per film. Same approach as episodes migration 031:
-- the slug-URL stays constant across searches and can be cached ahead of time
-- as a reliable fallback when SK Torrent doesn't have the title.

ALTER TABLE films
    ADD COLUMN IF NOT EXISTS prehrajto_url VARCHAR(500),
    ADD COLUMN IF NOT EXISTS prehrajto_has_dub  BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS prehrajto_has_subs BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_films_prehrajto_url ON films (prehrajto_url)
    WHERE prehrajto_url IS NOT NULL;
