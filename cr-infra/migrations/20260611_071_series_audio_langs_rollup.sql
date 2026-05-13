-- =============================================================================
-- Series language rollups: aggregate episode.audio_langs / subtitle_langs up
-- to the parent series, with both UNION (any) and INTERSECTION (all) semantics.
--
-- Why two arrays per kind?
--   - `audio_langs_any` — languages where AT LEAST ONE episode has a source in
--     that language. Drives the "alespoň částečně v jazyce" filter mode.
--   - `audio_langs_all` — languages where EVERY episode of the series has at
--     least one source in that language. Drives the "kompletně v jazyce"
--     filter mode. A series with only S01 in CZ and S02+ in EN-only has
--     `audio_langs_any = {cs, en}` but `audio_langs_all = {en}` (assuming
--     every episode has EN).
--
-- Language is a property of the video source, not of the series. These
-- rollups are pure derived state: the source-of-truth is video_sources
-- (already rolled into episodes.audio_langs by migration 058). This
-- migration adds a second hop episode → series.
-- =============================================================================

ALTER TABLE series
    ADD COLUMN IF NOT EXISTS audio_langs_any    TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    ADD COLUMN IF NOT EXISTS audio_langs_all    TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    ADD COLUMN IF NOT EXISTS subtitle_langs_any TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    ADD COLUMN IF NOT EXISTS subtitle_langs_all TEXT[] NOT NULL DEFAULT '{}'::TEXT[];

CREATE INDEX IF NOT EXISTS idx_series_audio_langs_any_gin
    ON series USING GIN (audio_langs_any);
CREATE INDEX IF NOT EXISTS idx_series_audio_langs_all_gin
    ON series USING GIN (audio_langs_all);
CREATE INDEX IF NOT EXISTS idx_series_subtitle_langs_any_gin
    ON series USING GIN (subtitle_langs_any);
CREATE INDEX IF NOT EXISTS idx_series_subtitle_langs_all_gin
    ON series USING GIN (subtitle_langs_all);

-- =============================================================================
-- Recompute function — single atomic UPDATE per series, mirroring the pattern
-- from migration 058's recompute_video_rollups_for_parent().
--
-- INTERSECTION semantics:
--   We count occurrences of each language across episodes and keep only those
--   whose count equals the total episode count. Edge cases:
--     - 0 episodes  → both _any and _all are {} (empty array).
--     - 1 episode with audio_langs={'cs','en'} → both _any and _all = {cs,en}.
--     - 2 episodes {cs,en} + {cs} → _any={cs,en}, _all={cs}.
--     - 2 episodes {cs} + {} → _any={cs}, _all={} (one episode misses it).
-- =============================================================================

CREATE OR REPLACE FUNCTION recompute_series_lang_rollups(p_series_id INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE series s SET
        audio_langs_any = COALESCE(
            (SELECT array_agg(DISTINCT lang ORDER BY lang)
             FROM (
                 SELECT unnest(e.audio_langs) AS lang
                 FROM episodes e
                 WHERE e.series_id = p_series_id
             ) u),
            '{}'::TEXT[]),
        audio_langs_all = COALESCE(
            (WITH ep_total AS (
                 SELECT COUNT(*) AS n FROM episodes WHERE series_id = p_series_id
             ),
             lang_counts AS (
                 SELECT unnest(e.audio_langs) AS lang, COUNT(*) AS cnt
                 FROM episodes e
                 WHERE e.series_id = p_series_id
                 GROUP BY 1
             )
             SELECT array_agg(lang ORDER BY lang)
             FROM lang_counts, ep_total
             WHERE ep_total.n > 0 AND cnt = ep_total.n),
            '{}'::TEXT[]),
        subtitle_langs_any = COALESCE(
            (SELECT array_agg(DISTINCT lang ORDER BY lang)
             FROM (
                 SELECT unnest(e.subtitle_langs) AS lang
                 FROM episodes e
                 WHERE e.series_id = p_series_id
             ) u),
            '{}'::TEXT[]),
        subtitle_langs_all = COALESCE(
            (WITH ep_total AS (
                 SELECT COUNT(*) AS n FROM episodes WHERE series_id = p_series_id
             ),
             lang_counts AS (
                 SELECT unnest(e.subtitle_langs) AS lang, COUNT(*) AS cnt
                 FROM episodes e
                 WHERE e.series_id = p_series_id
                 GROUP BY 1
             )
             SELECT array_agg(lang ORDER BY lang)
             FROM lang_counts, ep_total
             WHERE ep_total.n > 0 AND cnt = ep_total.n),
            '{}'::TEXT[])
    WHERE s.id = p_series_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Trigger on episodes — fires when audio_langs / subtitle_langs change (which
-- happens whenever migration 058's video_sources trigger updates the episode).
-- Also fires on INSERT/DELETE/series_id change so the series rollup stays
-- accurate when episodes appear/disappear or are re-parented.
-- =============================================================================

CREATE OR REPLACE FUNCTION trg_episodes_series_rollup() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM recompute_series_lang_rollups(NEW.series_id);
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM recompute_series_lang_rollups(OLD.series_id);
    ELSE  -- UPDATE
        PERFORM recompute_series_lang_rollups(NEW.series_id);
        IF OLD.series_id IS DISTINCT FROM NEW.series_id THEN
            PERFORM recompute_series_lang_rollups(OLD.series_id);
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Idempotent trigger creation. CREATE TRIGGER doesn't support IF NOT EXISTS
-- on Postgres before 14; dropping first works on all supported versions and
-- keeps re-runs safe (e.g. fresh CI DB after a partial dev-only apply).
DROP TRIGGER IF EXISTS trg_episodes_series_rollup_aiud ON episodes;
CREATE TRIGGER trg_episodes_series_rollup_aiud
    AFTER INSERT OR DELETE ON episodes
    FOR EACH ROW EXECUTE FUNCTION trg_episodes_series_rollup();

DROP TRIGGER IF EXISTS trg_episodes_series_rollup_au ON episodes;
CREATE TRIGGER trg_episodes_series_rollup_au
    AFTER UPDATE OF audio_langs, subtitle_langs, series_id ON episodes
    FOR EACH ROW EXECUTE FUNCTION trg_episodes_series_rollup();

-- =============================================================================
-- One-shot backfill — populate the four rollup columns for every existing
-- series. After this runs the trigger keeps them in sync.
-- =============================================================================

DO $$
DECLARE
    sid INTEGER;
BEGIN
    FOR sid IN SELECT id FROM series LOOP
        PERFORM recompute_series_lang_rollups(sid);
    END LOOP;
END $$;
