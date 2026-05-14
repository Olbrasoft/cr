-- =============================================================================
-- idx_films_added_at_id — covers the default `/filmy-online/` listing sort.
--
-- The "podle přidání" (default) sort is `ORDER BY f.added_at DESC NULLS LAST,
-- f.id DESC` and the page also gates on `EXISTS (video_sources is_alive)`.
-- Without this index the planner Parallel-Seq-Scans all ~28 k films + sorts
-- by added_at, costing ~120 ms for the SELECT alone. With the index the
-- planner does an indexed range scan in added_at order and pairs each
-- candidate with the (fast) `idx_vs_film_alive` lookup — total query time
-- drops to ~2 ms.
--
-- The `id DESC` second column is a tiebreaker for films with identical
-- added_at (which is common after a bulk import). Keeping it in the index
-- lets the index ordering match `ORDER BY` exactly so no extra sort is
-- needed.
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_films_added_at_id
    ON films (added_at DESC NULLS LAST, id DESC);
