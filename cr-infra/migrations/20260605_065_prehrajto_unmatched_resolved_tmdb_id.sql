-- Persistent state for the LLM TMDB-ID resolver
-- (`scripts/resolve-unmatched-via-llm.py`, PR #668).
--
-- The resolver asks Gemma to extract a canonical title from each
-- unmatched cluster, then resolves to a stable TMDB ID. Three goals
-- for this column:
--
--   1. When the resolved TMDB ID matches an existing `films.tmdb_id`,
--      we already store the join via `resolved_film_id`. But many
--      clusters resolve to a TMDB ID NOT yet in `films` — those are
--      candidates for the #652 auto-import pipeline and need to be
--      remembered between runs (otherwise the daily resolver would
--      keep paying Gemma quota to re-extract the same TMDB ID for
--      the same cluster every day).
--
--   2. If the TMDB ID is later auto-imported into `films`, the
--      resolver's next pass can connect cluster → film via
--      `films.tmdb_id` lookup without a fresh Gemma call.
--
--   3. The /admin/prehrajto/unmatched dashboard can surface "ready
--      to import" rows (resolved_tmdb_id IS NOT NULL AND
--      resolved_film_id IS NULL) so the operator can trigger #652
--      auto-import in batches.
--
-- The resolver also relies on `last_attempt_at` + `last_failure_reason`
-- (already present from migration 064) to skip clusters re-attempted
-- within the last N days — that's how we avoid burning 11k Gemma
-- requests every single day on the same backlog. No schema change is
-- needed for that part; just disciplined writes from the resolver.

ALTER TABLE prehrajto_unmatched_clusters
    ADD COLUMN IF NOT EXISTS resolved_tmdb_id INTEGER;

-- Partial index: dashboard / batch-import queries look for "we know
-- the TMDB ID but the film row doesn't exist yet". Postgres skips
-- the bulk of unresolved rows automatically thanks to the partial
-- predicate.
CREATE INDEX IF NOT EXISTS idx_pu_clusters_tmdb_pending
    ON prehrajto_unmatched_clusters (resolved_tmdb_id)
 WHERE resolved_tmdb_id IS NOT NULL
   AND resolved_film_id IS NULL;
