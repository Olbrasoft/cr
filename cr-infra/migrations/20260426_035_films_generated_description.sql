-- Add films.generated_description to mirror series.generated_description.
-- Auto-import pipeline (#423) fills this column with the Gemma 4 CS text
-- for brand-new films so the website displays a unique summary instead of
-- reusing the raw TMDB overview. Without this column upsert_film() in
-- scripts/auto_import/enricher.py fails on INSERT.
--
-- Existing films keep `description` as the primary copy; this column is
-- populated only when Gemma 4 returns a usable rewrite, which is why we
-- allow NULL.

ALTER TABLE films
    ADD COLUMN IF NOT EXISTS generated_description TEXT;
