-- 051 consolidate description
--
-- Cleanup of duplicate description / generated_description columns.
--
-- Historically the Gemma 4 rewrite pipeline wrote into `generated_description`
-- while the web layer rendered from `description`. Enricher + dev batch scripts
-- ended up writing the same Gemma output into BOTH columns (see #527, #392).
-- Web (cr-web/src/handlers/films.rs, series.rs, tv_porady.rs) reads only
-- `description`, so `generated_description` is effectively dead weight.
--
-- This migration:
--   1. Promotes `generated_description` → `description` where they differ
--      (preserves rows where the Gemma rerun never reached the display column).
--   2. Drops `generated_description` from films / series / tv_shows.
--   3. Renames `generated_description` → `description` on episodes / tv_episodes
--      (those two tables never had a separate `description` column).
--   4. Adds COMMENT ON COLUMN so the invariant is documented at the schema
--      level — future devs won't wonder what belongs in `description`.

BEGIN;

-- films ---------------------------------------------------------------
UPDATE films
   SET description = generated_description
 WHERE generated_description IS NOT NULL
   AND generated_description IS DISTINCT FROM description;

ALTER TABLE films DROP COLUMN generated_description;

COMMENT ON COLUMN films.description IS
'Unikátní český popis filmu zobrazovaný na webu. Povinně výstup z Gemma rewrite přes scripts/auto_import/gemma_writer.py (aktuální model viz GEMMA_MODEL). Zdroje: TMDB CS + TMDB EN (+ případně sktorrent popis). NIKDY sem nezapisuj raw TMDB/IMDB/bombuj/sktorrent text — SEO by to penalizovalo jako duplicate content. Fallback na raw TMDB jen když Gemma selže (safety filter, 429).';

-- series --------------------------------------------------------------
UPDATE series
   SET description = generated_description
 WHERE generated_description IS NOT NULL
   AND generated_description IS DISTINCT FROM description;

ALTER TABLE series DROP COLUMN generated_description;

COMMENT ON COLUMN series.description IS
'Unikátní český popis seriálu zobrazovaný na webu. Povinně výstup z Gemma rewrite přes scripts/auto_import/gemma_writer.py (is_series=True, 300-600 znaků). Zdroje: TMDB CS + TMDB EN. NIKDY sem nezapisuj raw TMDB text.';

-- tv_shows ------------------------------------------------------------
-- tv_shows.generated_description je prázdný (0 řádků), jen dropnout.
-- POZOR: tv_show_enricher dnes Gemmu nevolá — popisy jsou dosud raw TMDB.
-- Řeší se v #565.
ALTER TABLE tv_shows DROP COLUMN generated_description;

COMMENT ON COLUMN tv_shows.description IS
'Unikátní český popis TV pořadu. CÍLOVÝ stav: Gemma rewrite jako u series. AKTUÁLNÍ stav (2026-04): obsahuje raw TMDB cs-CZ/en-US overview, protože tv_show_enricher dosud Gemmu nevolá — viz #565. Po opravě: nikdy sem nezapisuj raw TMDB text.';

-- episodes ------------------------------------------------------------
ALTER TABLE episodes RENAME COLUMN generated_description TO description;

COMMENT ON COLUMN episodes.description IS
'Unikátní český popis epizody. Výstup z Gemma rewrite (scripts/generate-episode-descriptions.py). Zdroje: episode_name + overview + overview_en. Fallback na raw overview jen když Gemma selže.';

-- tv_episodes ---------------------------------------------------------
ALTER TABLE tv_episodes RENAME COLUMN generated_description TO description;

COMMENT ON COLUMN tv_episodes.description IS
'Unikátní český popis epizody TV pořadu. Zatím prakticky prázdné — per-episode popis u reality/zábavních pořadů typicky TMDB nemá. Až budeme generovat, musí to být Gemma rewrite output, ne raw TMDB.';

COMMIT;
