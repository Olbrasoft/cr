-- Issue #566 — auto-import TV scanner is currently invisible on /admin/import/.
-- The pipeline writes tv_shows + tv_episodes (50 + 811 rows on prod), but
-- import_runs has no counters for them, so every run shows zeros for the TV
-- branch even when it really did import something.

ALTER TABLE import_runs
    ADD COLUMN added_tv_shows    INT NOT NULL DEFAULT 0,
    ADD COLUMN added_tv_episodes INT NOT NULL DEFAULT 0;
