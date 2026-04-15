-- Cache TMDB English overview for each series. The Czech overview is already
-- stored in `description` (imported 1:1 from TMDB CS). For many series the
-- Czech text is short (<200 chars), so we combine CS + EN as input to Gemma 4
-- to produce a longer, unique Czech description.

ALTER TABLE series
    ADD COLUMN IF NOT EXISTS tmdb_overview_en TEXT;
