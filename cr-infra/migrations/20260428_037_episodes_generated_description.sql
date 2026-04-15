-- Add episodes.generated_description so individual episodes can carry a
-- unique Gemma-4-rewritten Czech summary alongside the raw TMDB overview.
-- Mirrors films.generated_description (migration 035) and
-- series.generated_description (migration 029). Templates prefer this
-- column when present and fall back to `overview` when NULL.

ALTER TABLE episodes
    ADD COLUMN IF NOT EXISTS generated_description TEXT;
