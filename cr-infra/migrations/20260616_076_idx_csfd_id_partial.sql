-- =============================================================================
-- Partial indexes on series.csfd_id and tv_shows.csfd_id (#737 review).
--
-- The csfd resolver (resolve-csfd-via-wikidata.py) performs a per-row
-- NOT EXISTS collision check on the target table before writing
-- `csfd_id`. `films` already had `idx_films_csfd_id` from migration 028,
-- but `series` and `tv_shows` only had the bare nullable column, so the
-- collision check fell back to a Seq Scan on each row. With the resolver
-- now writing thousands of csfd_ids per run on series, that scan turned
-- into a hot loop.
--
-- Partial WHERE-NOT-NULL keeps the index tiny: most series/tv_shows
-- rows have NULL csfd_id (newly imported entries before the next
-- weekly resolver tick), so we don't pay write-amplification for them.
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_series_csfd_id
    ON series (csfd_id) WHERE csfd_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tv_shows_csfd_id
    ON tv_shows (csfd_id) WHERE csfd_id IS NOT NULL;
