-- #366 — library recency. Every time the user re-requests an URL that
-- already has a MP4 library row we bump `last_accessed_at = NOW()` so
-- the card slides back to the top of the "Stažená videa" grid. The
-- list endpoint orders by this column instead of `created_at` so the
-- natural "most recently touched" ordering falls out of the query.

ALTER TABLE videos
    ADD COLUMN last_accessed_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX idx_videos_last_accessed_at ON videos (last_accessed_at DESC);
