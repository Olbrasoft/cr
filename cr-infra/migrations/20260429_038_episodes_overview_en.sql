-- Store the TMDB English per-episode overview alongside the Czech one.
-- Mirrors series.tmdb_overview_en (migration 033). The Gemma rewriter
-- sends CS + EN when the CS blurb is too short to produce a distinctive
-- Czech summary on its own.

ALTER TABLE episodes
    ADD COLUMN IF NOT EXISTS overview_en TEXT;
