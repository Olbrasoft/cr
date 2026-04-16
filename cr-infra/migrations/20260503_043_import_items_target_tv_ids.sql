-- #481 — let import_items record which tv_shows / tv_episodes row a pipeline
-- run created or touched, mirroring the existing target_film_id,
-- target_series_id, target_episode_id columns. Soft references (no FK) to
-- match the style of the other target_* columns — deleting a tv_show should
-- not cascade-delete audit history, and the admin detail SELECT handles
-- dangling ids via LEFT JOIN.

ALTER TABLE import_items
    ADD COLUMN IF NOT EXISTS target_tv_show_id    INTEGER,
    ADD COLUMN IF NOT EXISTS target_tv_episode_id INTEGER;

CREATE INDEX IF NOT EXISTS idx_import_items_target_tv_show
    ON import_items (target_tv_show_id)
    WHERE target_tv_show_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_import_items_target_tv_episode
    ON import_items (target_tv_episode_id)
    WHERE target_tv_episode_id IS NOT NULL;
