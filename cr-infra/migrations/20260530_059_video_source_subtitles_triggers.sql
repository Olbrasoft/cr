-- Migration 059 — tighten trigger coverage on video_source_subtitles.
--
-- Issue #607 follow-up for Copilot review findings on migration 058:
--
--   1. trg_video_source_subtitles_rollup picks NEW.source_id on UPDATE and
--      recomputes rollups for that source's parent only. If an UPDATE ever
--      changes source_id (uncommon but possible via bulk repair / re-parent),
--      the OLD parent's subtitle_langs stays stale. Mirror the video_sources
--      trigger pattern: when OLD.source_id IS DISTINCT FROM NEW.source_id,
--      recompute for both parents.
--
--   2. TRUNCATE fires only STATEMENT-level triggers, not row-level. The
--      video_sources TRUNCATE trigger clears both audio_langs and
--      subtitle_langs arrays, but `TRUNCATE video_source_subtitles` alone
--      would leave subtitle_langs populated on parent rows. Add a matching
--      TRUNCATE trigger on video_source_subtitles that re-zeroes the
--      subtitle_langs columns (audio_langs stay untouched because the actual
--      audio data in video_sources wasn't touched).

-- Replace the existing subtitles row-level trigger function to correctly
-- handle source_id UPDATE (recompute for both parents).
CREATE OR REPLACE FUNCTION trg_video_source_subtitles_rollup() RETURNS TRIGGER AS $$
DECLARE
    v_film       INTEGER;
    v_episode    INTEGER;
    v_tv_episode INTEGER;
BEGIN
    -- Case 1: source_id changed on UPDATE — recompute for the OLD parent
    -- first, then fall through to recompute for NEW below.
    IF TG_OP = 'UPDATE' AND OLD.source_id IS DISTINCT FROM NEW.source_id THEN
        SELECT film_id, episode_id, tv_episode_id
        INTO   v_film, v_episode, v_tv_episode
        FROM   video_sources
        WHERE  id = OLD.source_id;
        IF FOUND THEN
            PERFORM recompute_video_rollups_for_parent(v_film, v_episode, v_tv_episode);
        END IF;
    END IF;

    -- Case 2: pick source_id from NEW on INSERT/UPDATE, OLD on DELETE, and
    -- recompute rollups for that parent. Same behaviour as before for the
    -- 99% path where source_id doesn't move.
    IF TG_OP = 'DELETE' THEN
        SELECT film_id, episode_id, tv_episode_id
        INTO   v_film, v_episode, v_tv_episode
        FROM   video_sources
        WHERE  id = OLD.source_id;
    ELSE
        SELECT film_id, episode_id, tv_episode_id
        INTO   v_film, v_episode, v_tv_episode
        FROM   video_sources
        WHERE  id = NEW.source_id;
    END IF;
    IF FOUND THEN
        PERFORM recompute_video_rollups_for_parent(v_film, v_episode, v_tv_episode);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- TRUNCATE trigger on video_source_subtitles: clears subtitle_langs rollups
-- across all parent tables. audio_langs is not reset here because
-- video_sources (and its own audio_lang column) are untouched by TRUNCATE
-- on the subs child table.
CREATE OR REPLACE FUNCTION trg_video_source_subtitles_truncate() RETURNS TRIGGER AS $$
BEGIN
    UPDATE films       SET subtitle_langs = '{}'::TEXT[]
                       WHERE cardinality(subtitle_langs) > 0;
    UPDATE episodes    SET subtitle_langs = '{}'::TEXT[]
                       WHERE cardinality(subtitle_langs) > 0;
    UPDATE tv_episodes SET subtitle_langs = '{}'::TEXT[]
                       WHERE cardinality(subtitle_langs) > 0;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_video_source_subtitles_truncate_s ON video_source_subtitles;
CREATE TRIGGER trg_video_source_subtitles_truncate_s
    AFTER TRUNCATE ON video_source_subtitles
    FOR EACH STATEMENT EXECUTE FUNCTION trg_video_source_subtitles_truncate();
