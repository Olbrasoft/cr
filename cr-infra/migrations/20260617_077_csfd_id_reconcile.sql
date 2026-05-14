-- =============================================================================
-- csfd_id reconcile pass — re-validate existing csfd_id values against
-- Wikidata P345→P2529 (epic #730 follow-up, #740).
--
-- The #732 resolver fills NULLs only. It will never touch a pre-existing
-- csfd_id even if Wikidata says it's wrong. A #733 Playwright sample of
-- 50 random rows from /api/csfd-watchlist.json found ~16 % of those
-- pre-existing values disagree with Wikidata, leaking wrong mappings
-- into the public feed that csfd-data-hub consumes.
--
-- This migration adds:
--   1. `mode` column on csfd_id_resolution_runs so a single runs table
--      tracks both the bulk resolve pass and the reconcile pass.
--   2. `csfd_id_reconcile_review` — one row per row whose cr.csfd_id
--      disagrees with Wikidata's P2529. Carries BOTH ids (current cr
--      value AND Wikidata's proposal) so a rewrite is reversible from
--      the audit log alone — no need to query Wikidata again.
--
-- Two-pass workflow:
--   * reconcile --dry-run   → populate review table with action='pending_review'
--   * reconcile --apply-safe-rewrites
--       → for every pending_review row where labelCs ≈ cr.title after
--         normalisation, UPDATE …csfd_id = wikidata_csfd_id WHERE
--         csfd_id = <original cr_csfd_id> (so manual edits made between
--         dry-run and apply are not clobbered). Set action='auto_rewritten'.
--       → everything else stays as 'pending_review' for human triage.
-- =============================================================================

ALTER TABLE csfd_id_resolution_runs
    ADD COLUMN mode TEXT NOT NULL DEFAULT 'resolve'
        CHECK (mode IN ('resolve', 'reconcile'));

CREATE INDEX idx_csfd_resolution_runs_mode_started
    ON csfd_id_resolution_runs (mode, started_at DESC);


CREATE TABLE csfd_id_reconcile_review (
    id                  SERIAL PRIMARY KEY,
    run_id              INT NOT NULL
        REFERENCES csfd_id_resolution_runs(id) ON DELETE CASCADE,
    source_table        TEXT NOT NULL
        CHECK (source_table IN ('films', 'series', 'tv_shows')),
    source_row_id       INT NOT NULL,
    -- Source-row snapshot at review time. cr_csfd_id is the suspect
    -- value (what the public feed is currently exposing); preserving it
    -- here makes the rewrite reversible without consulting external
    -- data sources.
    cr_imdb_id          TEXT,
    cr_tmdb_id          INT,
    cr_title            TEXT,
    cr_year             INT,
    cr_csfd_id          INT NOT NULL,
    -- Wikidata's proposal. wikidata_csfd_id is NULL when Wikidata
    -- knows the item (matched the IMDb/TMDB ID) but has no P2529 —
    -- that case is logged separately as reason='wikidata_missing_p2529'.
    wikidata_qid        TEXT,
    wikidata_csfd_id    INT,
    wikidata_label_cs   TEXT,
    -- Why this row was queued: 'wikidata_disagrees' (the common case),
    -- 'wikidata_missing_p2529' (item exists but no ČSFD link),
    -- 'duplicate_wikidata_entity' (multiple ?items for one external ID),
    -- 'label_mismatch_blocked_rewrite' (Wikidata disagrees AND labelCs
    --     doesn't match cr.title — too risky to auto-rewrite).
    reason              TEXT NOT NULL,
    -- Lifecycle:
    --   pending_review  — fresh entry, no decision yet (also the dry-run state).
    --   auto_rewritten  — apply-safe-rewrites updated cr.csfd_id.
    --   manual_resolved — maintainer fixed by hand and cleared the queue.
    --   kept_original   — maintainer reviewed and decided cr was right.
    action_taken        TEXT NOT NULL DEFAULT 'pending_review'
        CHECK (action_taken IN
            ('pending_review', 'auto_rewritten', 'manual_resolved', 'kept_original')),
    rewritten_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_csfd_reconcile_review_run_id
    ON csfd_id_reconcile_review (run_id);
CREATE INDEX idx_csfd_reconcile_review_source
    ON csfd_id_reconcile_review (source_table, source_row_id);
-- Partial index — most rows will quickly transition out of
-- pending_review after a run completes, so we only care about the
-- queue itself for the admin dashboard.
CREATE INDEX idx_csfd_reconcile_review_pending
    ON csfd_id_reconcile_review (source_table, source_row_id)
    WHERE action_taken = 'pending_review';
