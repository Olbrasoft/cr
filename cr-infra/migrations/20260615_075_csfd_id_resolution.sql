-- =============================================================================
-- csfd_id resolution run log + manual-review queue (#730 / #732).
--
-- ČSFD exposes no API; the only public path that cross-references
-- IMDb/TMDB ↔ ČSFD is Wikidata SPARQL via P345/P4947/P4983 + P2529.
-- The resolver script (scripts/resolve-csfd-via-wikidata.py) runs weekly
-- as a systemd timer, batches IMDb IDs (~200 per query), and writes
-- `csfd_id` back into films/series/tv_shows.
--
-- Two tables here:
--   * csfd_id_resolution_runs  — one row per resolver invocation. Pattern
--     mirrors `rating_sync_runs` (#591/#696): INSERT at start with
--     status='running', UPDATE at finish with counters + status. Admin
--     tile reads `WHERE status<>'running' ORDER BY started_at DESC LIMIT 1`.
--   * csfd_id_resolution_review — one row per *suspicious* mapping that
--     the resolver refused to write because the Czech Wikidata label
--     disagreed with `cr.{title}` (or because Wikidata returned multiple
--     ?film entities for the same external ID). Maintainer reviews this
--     table manually, then either UPDATE…csfd_id by hand or deletes the
--     review row to clear the queue.
--
-- Volume estimate: weekly run, plus ad-hoc runs after the first big
-- backfill of 22 751 rows → ≤100 rows/year in `runs`. Review queue is
-- one-off (tens to low-hundreds entries) so no archiving needed.
-- =============================================================================

CREATE TABLE csfd_id_resolution_runs (
    id                  SERIAL PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'ok', 'error', 'partial')),
    -- Dry-run rows still get written (they are useful as a diagnostic
    -- "what would have happened" record) but the resolved csfd_id is
    -- never written back to films/series/tv_shows.
    dry_run             BOOLEAN NOT NULL DEFAULT FALSE,
    -- Aggregate counters across films + series + tv_shows. Per-table
    -- breakdown lives in `per_table` JSONB so we don't grow the column
    -- list every time a new source table appears.
    processed           INT NOT NULL DEFAULT 0,
    resolved_via_imdb   INT NOT NULL DEFAULT 0,
    resolved_via_tmdb   INT NOT NULL DEFAULT 0,
    sanity_rejected     INT NOT NULL DEFAULT 0,
    unresolved          INT NOT NULL DEFAULT 0,
    -- Free-form per-table summary, e.g.
    --   {"films": {"processed": 20000, "via_imdb": 14000, …},
    --    "series": {…}, "tv_shows": {…}}.
    per_table           JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message       TEXT
);

CREATE INDEX idx_csfd_resolution_runs_started_at
    ON csfd_id_resolution_runs (started_at DESC);


CREATE TABLE csfd_id_resolution_review (
    id                  SERIAL PRIMARY KEY,
    run_id              INT NOT NULL
        REFERENCES csfd_id_resolution_runs(id) ON DELETE CASCADE,
    source_table        TEXT NOT NULL
        CHECK (source_table IN ('films', 'series', 'tv_shows')),
    source_row_id       INT NOT NULL,
    -- External IDs from the source row, copied at review time so the
    -- entry is self-contained even if the source row is later edited.
    cr_imdb_id          TEXT,
    cr_tmdb_id          INT,
    cr_title            TEXT,
    cr_year             INT,
    -- Wikidata's proposal that we refused.
    wikidata_qid        TEXT,
    proposed_csfd_id    INT,
    wikidata_label_cs   TEXT,
    -- Short tag of why this was rejected: 'label_mismatch',
    -- 'duplicate_wikidata_entity', 'missing_label', etc.
    reason              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_csfd_resolution_review_run_id
    ON csfd_id_resolution_review (run_id);
CREATE INDEX idx_csfd_resolution_review_source
    ON csfd_id_resolution_review (source_table, source_row_id);
