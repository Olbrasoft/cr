-- #483 follow-up — detected_type CHECK rejected the new tv_show / tv_episode
-- values the TV pořad pipeline writes into import_items. Same deal as the
-- action CHECK in migration 045: extend the allowed set so audit rows can
-- actually land in the table.

ALTER TABLE import_items
    DROP CONSTRAINT IF EXISTS import_items_detected_type_check;

ALTER TABLE import_items
    ADD CONSTRAINT import_items_detected_type_check
    CHECK (detected_type IS NULL OR detected_type IN (
        'film',
        'series',
        'episode',
        'tv_show',
        'tv_episode',
        'unknown'
    ));
