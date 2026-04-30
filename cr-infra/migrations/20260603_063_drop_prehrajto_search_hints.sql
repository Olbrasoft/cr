-- Drop the `prehrajto_search_hints` table introduced in #632 / migration 062.
--
-- Epic #631 (resolve-at-play-time) was reverted (see PR #647 / epic #642):
-- the sitemap-importer-driven `video_sources(prehrajto)` model is the
-- source of truth, search hints are unused. This migration retires the
-- empty table cleanly without leaving the orphan around.
--
-- Why a separate migration instead of deleting 062 from the tree:
-- `sqlx::migrate!` embeds the entire `migrations/` directory at build time
-- and refuses to start the app if a previously-applied migration version
-- has gone missing from that set ("previously applied migration is
-- missing"). Keeping 062 + adding 063 means:
--   - DBs that already ran 062 see 063 next and drop the table.
--   - Fresh DBs run 062 (CREATE) → 063 (DROP) and end up with no table.
--
-- The trigger function created by 062 is dropped via CASCADE since it
-- only existed to back the trigger on the table.

DROP TABLE IF EXISTS prehrajto_search_hints CASCADE;
DROP FUNCTION IF EXISTS prehrajto_search_hints_touch_updated_at() CASCADE;
