-- Track whether the daily auto-import attempted (and matched) prehraj.to
-- search after each new SK Torrent film. Surfaced on /admin/import/{N} so
-- the operator sees which films picked up a prehrajto source automatically
-- and which need manual triage. Counters on `import_runs` summarize the
-- whole run for the dashboard list view.
--
-- `prehrajto_status` values written by `scripts/auto-import.py`:
--   matched        — at least one accepted hit was written / re-pointed
--   no_results     — search returned 0 hits
--   no_acceptable  — hits found but all rejected (low sim / wrong duration)
--   error          — non-blocking exception during search/write
--   blocked        — proxy / prehraj.to returned non-200 (run aborts)
ALTER TABLE import_items
    ADD COLUMN IF NOT EXISTS prehrajto_status      text,
    ADD COLUMN IF NOT EXISTS prehrajto_rows_written integer NOT NULL DEFAULT 0;

ALTER TABLE import_items
    ADD CONSTRAINT import_items_prehrajto_status_check
    CHECK (prehrajto_status IS NULL OR prehrajto_status IN
           ('matched', 'no_results', 'no_acceptable', 'error', 'blocked'));

ALTER TABLE import_runs
    ADD COLUMN IF NOT EXISTS prehrajto_attempted    integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS prehrajto_matched      integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS prehrajto_rows_written integer NOT NULL DEFAULT 0;
