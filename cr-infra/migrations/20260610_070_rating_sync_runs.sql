-- Run history for the nightly IMDb (04:30 UTC) and TMDB (04:45 UTC)
-- rating sync jobs (#591, #696). Each script writes one row per run:
-- INSERT at start with status='running', UPDATE at finish with the
-- final counters + status. Admin dashboard reads the latest row per
-- kind to render the IMDb/TMDB tiles, same pattern as `import_runs` +
-- `backup_runs`.
--
-- Volume: 2 rows/day → 730 rows/year. Even after a decade the table is
-- under 2 MB, so no retention/archiving is needed.

CREATE TABLE rating_sync_runs (
    id                  SERIAL PRIMARY KEY,
    kind                TEXT NOT NULL
        CHECK (kind IN ('imdb', 'tmdb')),
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'ok', 'error', 'partial')),
    -- Per-table refresh counters. INT (not SMALLINT): IMDb daily TSV
    -- can refresh ~30 000 films at once, well past SMALLINT's ceiling.
    films_refreshed     INT NOT NULL DEFAULT 0,
    series_refreshed    INT NOT NULL DEFAULT 0,
    tv_shows_refreshed  INT NOT NULL DEFAULT 0,
    -- TMDB only — rows that returned 404 or had no votes. IMDb sync
    -- doesn't track per-row failures (the TSV either parses cleanly or
    -- the whole run errors).
    failed_count        INT NOT NULL DEFAULT 0,
    error_message       TEXT
);

-- Admin dashboard query: `SELECT … FROM rating_sync_runs WHERE kind=$1
-- ORDER BY started_at DESC LIMIT 1`. Composite index keeps the lookup
-- O(1) regardless of how the table grows.
CREATE INDEX idx_rating_sync_runs_kind_started_at
    ON rating_sync_runs (kind, started_at DESC);
