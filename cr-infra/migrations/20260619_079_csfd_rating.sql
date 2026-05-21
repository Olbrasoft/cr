-- =============================================================================
-- ČSFD audience ratings — fill in the missing pieces (#758, #760).
--
-- `csfd_rating SMALLINT` already exists on films/series/tv_shows since the
-- original CREATE TABLE migrations (028, 029, 041) — it just has never
-- been populated, since no scraper existed. Issue #758 introduces the
-- scraper (#761 -> fetch_csfd_ratings.mjs via bartholomej/node-csfd-api).
--
-- This migration adds the two companion columns the scraper needs:
--   * csfd_rating_count   — INTEGER, number of ČSFD users who rated
--   * csfd_rating_synced_at — TIMESTAMPTZ, when we last fetched
--
-- The naming mirrors the pattern established by migration 069
-- (tmdb_rating + tmdb_rating_synced_at, imdb_rating + imdb_rating_synced_at).
-- We keep `csfd_rating` as SMALLINT — ČSFD's own UI rounds to whole
-- percent ("77 %") so storing 0-100 is sufficient; the JSON-LD value
-- 76.666… is a back-calculation we don't preserve.
--
-- The scraper picks rows to refresh with:
--   SELECT id, csfd_id FROM <table>
--    WHERE csfd_id IS NOT NULL
--      AND (csfd_rating_synced_at IS NULL
--           OR csfd_rating_synced_at < now() - INTERVAL '7 days')
--    ORDER BY csfd_rating_synced_at NULLS FIRST
--    LIMIT N;
--
-- The partial indexes back that picker — NULLS FIRST keeps fresh inserts
-- at the head of the queue, then rotates the rest by oldest fetched.
-- WHERE csfd_id IS NOT NULL keeps the indexes tiny: most rows without a
-- csfd_id have nothing to scrape anyway.
-- =============================================================================

-- films -----------------------------------------------------------------
ALTER TABLE films
    ADD COLUMN csfd_rating_count       INTEGER,
    ADD COLUMN csfd_rating_synced_at   TIMESTAMPTZ;

CREATE INDEX idx_films_csfd_rating_synced_at
    ON films (csfd_rating_synced_at NULLS FIRST)
    WHERE csfd_id IS NOT NULL;

-- series ----------------------------------------------------------------
ALTER TABLE series
    ADD COLUMN csfd_rating_count       INTEGER,
    ADD COLUMN csfd_rating_synced_at   TIMESTAMPTZ;

CREATE INDEX idx_series_csfd_rating_synced_at
    ON series (csfd_rating_synced_at NULLS FIRST)
    WHERE csfd_id IS NOT NULL;

-- tv_shows --------------------------------------------------------------
ALTER TABLE tv_shows
    ADD COLUMN csfd_rating_count       INTEGER,
    ADD COLUMN csfd_rating_synced_at   TIMESTAMPTZ;

CREATE INDEX idx_tv_shows_csfd_rating_synced_at
    ON tv_shows (csfd_rating_synced_at NULLS FIRST)
    WHERE csfd_id IS NOT NULL;
