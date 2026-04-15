-- Stable Přehraj.to URL per episode. The resolved video URL has short-lived
-- tokens but the slug-URL itself stays constant across searches, so we can
-- cache it ahead of time and use it as a reliable source even for episodes
-- that SK Torrent doesn't have.

ALTER TABLE episodes
    ADD COLUMN IF NOT EXISTS prehrajto_url VARCHAR(500),
    ADD COLUMN IF NOT EXISTS prehrajto_has_dub  BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS prehrajto_has_subs BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_episodes_prehrajto_url ON episodes (prehrajto_url)
    WHERE prehrajto_url IS NOT NULL;
