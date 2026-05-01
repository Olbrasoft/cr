-- Registry of sitemap clusters that the prehraj.to importer could not
-- match to any row in `films` (#657, parent epic #656).
--
-- Today the importer (`scripts/import-prehrajto-uploads.py`) joins
-- sitemap clusters against the films table by (normalized title core,
-- year, 3-min duration bucket) and silently drops everything that does
-- not match. We do not know how large that pile is, which titles keep
-- showing up, or whether previously unmatched clusters later got
-- resolved (e.g. operator added the film manually, or it landed via SK
-- Torrent auto-import). This table is the persistent log so:
--
--   - The follow-up auto-import flow (#652) can consult `last_attempt_at`
--     and skip TMDB lookups for clusters that recently failed, instead
--     of mlátit do TMDB rate-limitu se stejným nematchem každý den.
--   - The /admin/prehrajto/unmatched dashboard surfaces "loudest" rows
--     (high `upload_count`) so the operator can decide what to backfill
--     manually or where the matching heuristic is too strict.
--   - When a cluster eventually does match (films table catches up),
--     the importer marks it `resolved_at` + `resolved_film_id` so we
--     have a feedback loop on what gets fixed how.
--
-- Bucket size matches `cluster_key`: row.duration / (3 * 60). Year is
-- nullable in principle (the importer skips rows without an extractable
-- year, but we keep the column nullable for forward compatibility).

CREATE TABLE IF NOT EXISTS prehrajto_unmatched_clusters (
    id                    SERIAL PRIMARY KEY,
    -- Normalized title core after strip_title()+normalize() — the same
    -- form `cluster_key()` uses on the films-side. Stored as TEXT
    -- because there is no length cap on the input title.
    cluster_key           TEXT        NOT NULL,
    year                  INTEGER,
    duration_bucket       INTEGER,
    -- One representative <video:title> for humans; updated whenever a
    -- new sitemap snapshot bumps `last_seen_at`. Helps the operator
    -- recognize "ah, this is the Czech localized title vs. original".
    sample_title          TEXT        NOT NULL,
    -- One representative prehraj.to URL — clickable link in the admin
    -- table so the operator can verify the upload is real.
    sample_url            TEXT        NOT NULL,
    -- Number of distinct upload_ids in the cluster *as observed by the
    -- most recent importer run* — a per-run snapshot, NOT cumulative. We
    -- replace the value on each UPSERT instead of summing because the
    -- same uploads stay in sitemap day after day; summing would inflate
    -- the count without adding signal. Sortable signal for the admin view.
    upload_count          INTEGER     NOT NULL DEFAULT 1,
    first_seen_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Last time we attempted to *resolve* this cluster — i.e. tried to
    -- match it to something concrete. Today the importer doesn't call
    -- TMDB, so this is set only on INSERT and stays put on subsequent
    -- sightings. #652 (auto-import via TMDB) will UPDATE it whenever
    -- it actually tries a lookup, so its 7-day skip-window arithmetic
    -- works without being reset by every nightly sitemap parse.
    last_attempt_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt_count         INTEGER     NOT NULL DEFAULT 1,
    -- Set on INSERT to a generic "no match" string; #652 will overwrite
    -- with TMDB-specific failure reasons when it tries lookups. The
    -- importer never overwrites an existing value (COALESCE protects it),
    -- so #652's diagnostic text isn't clobbered by a nightly sitemap
    -- parse that didn't actually retry the lookup.
    last_failure_reason   TEXT,
    -- Set when a later run successfully matched this cluster — either
    -- because someone added the film manually, or because #652 resolved
    -- it via TMDB. Once set we leave the row in place (keeps the
    -- per-cluster history) but stop counting it on the dashboard.
    resolved_at           TIMESTAMPTZ,
    resolved_film_id      INTEGER     REFERENCES films(id) ON DELETE SET NULL,

    -- One row per (key, year, duration_bucket). Same triplet as
    -- `cluster_key()` returns on the films side, so resolving = exact
    -- key lookup.
    CONSTRAINT uq_pu_clusters_key UNIQUE (cluster_key, year, duration_bucket)
);

-- Admin dashboard sorts unresolved rows by upload_count DESC. Partial
-- index keeps the dashboard query cheap as resolved rows accumulate.
CREATE INDEX IF NOT EXISTS idx_pu_clusters_unresolved
    ON prehrajto_unmatched_clusters (upload_count DESC)
    WHERE resolved_at IS NULL;

-- #652 will look up rows by (key, year, bucket) plus `last_attempt_at`
-- to decide skip vs. retry. The unique constraint above already covers
-- the lookup; this index speeds up the time-window scan when the
-- importer iterates "all unresolved clusters older than X days".
CREATE INDEX IF NOT EXISTS idx_pu_clusters_attempt
    ON prehrajto_unmatched_clusters (last_attempt_at)
    WHERE resolved_at IS NULL;
