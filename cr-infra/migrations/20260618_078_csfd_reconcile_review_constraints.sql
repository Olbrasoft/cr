-- =============================================================================
-- Harden csfd_id_reconcile_review against Copilot's #741 review findings:
--   1. Enumerate `reason` values with a CHECK constraint so future schema
--      readers see every valid state (`non_numeric_csfd` was missing from
--      the original comment block in migration 077; the script writes it
--      when Wikidata returns a non-integer P2529).
--   2. Add a partial UNIQUE index on (source_table, source_row_id) for
--      pending_review rows so re-running `--reconcile` (or running it
--      again before --apply-safe-rewrites consumes the queue) cannot
--      duplicate entries for the same source row. The script's INSERT
--      now uses ON CONFLICT DO NOTHING; this index is what enforces it.
--
-- Note: rows already in pending_review at migration time MAY violate the
-- unique index in theory. In practice the only writer up to now is the
-- single dry-run + apply pair from #740, which uses `handled.add(row_id)`
-- to prevent same-run duplicates, so the partial unique index is safe to
-- add online without cleanup.
-- =============================================================================

ALTER TABLE csfd_id_reconcile_review
    ADD CONSTRAINT csfd_id_reconcile_review_reason_check
    CHECK (reason IN (
        'wikidata_disagrees',
        'wikidata_missing_p2529',
        'duplicate_wikidata_entity',
        'label_mismatch_blocked_rewrite',
        'non_numeric_csfd'
    ));

CREATE UNIQUE INDEX idx_csfd_reconcile_review_pending_unique
    ON csfd_id_reconcile_review (source_table, source_row_id)
    WHERE action_taken = 'pending_review';

-- The non-unique index added in migration 077 with the same predicate is
-- now redundant; drop it so we don't carry two indexes that cover the
-- same query (postgres planner picks one and the other is dead weight).
DROP INDEX IF EXISTS idx_csfd_reconcile_review_pending;
