-- =============================================================================
-- idx_episodes_series_created — covers the per-series "latest episode" lookup
-- used by `fetch_latest_episode_cards` (cr-web/src/handlers/series.rs) and the
-- count-of-listable-series subqueries on `/serialy-online/`.
--
-- Without it the planner falls back to a Parallel Seq Scan + sort across all
-- ~104k episodes for every page load, costing ~3s/request. After the index,
-- the LATERAL subquery does a single index range read per series — total
-- listing query drops to ~300 ms.
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_episodes_series_created
    ON episodes (series_id, created_at DESC);
