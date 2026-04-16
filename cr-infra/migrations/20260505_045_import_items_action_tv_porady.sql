-- #484 — admit the two TV pořad action codes the auto-import pipeline now
-- writes (`added_tv_show`, `added_tv_episode`). The old CHECK constraint
-- pre-dated the TV pořady catalog and rejects those inserts, so without
-- this migration every tv-porady row the pipeline tries to persist bombs
-- with `import_items_action_check` and the whole run fails.
--
-- Postgres doesn't support `ALTER … ADD VALUE` on CHECK-with-IN, so drop
-- and recreate.

ALTER TABLE import_items
    DROP CONSTRAINT IF EXISTS import_items_action_check;

ALTER TABLE import_items
    ADD CONSTRAINT import_items_action_check
    CHECK (action IN (
        'added_film',
        'added_series',
        'added_episode',
        'added_tv_show',
        'added_tv_episode',
        'updated_film',
        'updated_episode',
        'skipped',
        'failed'
    ));
