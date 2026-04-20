-- Lazily-filled "direct CDN vs proxied" flag for each prehraj.to upload.
--
-- TRUE  = last resolve produced a `*.premiumcdn.net` URL (direct, fast
--         HTML5 `<video>` playback — the green "přímý" badge in the UI).
-- FALSE = last resolve produced a non-premiumcdn URL (prehraj.to's own
--         proxy path — the orange "proxy" badge).
-- NULL  = not yet resolved by the server-side stream endpoint, so we do
--         not know. Importer never populates this; the column is
--         opportunistically corrected on the first hit of
--         `/api/movies/stream/{upload_id}`.
--
-- Why nullable + lazy, not an importer field: validating every upload
-- at import time would take a per-upload page scrape (~60 k calls first
-- run), and the direct/proxy state can flip later as prehraj.to moves
-- storage — so pay for the check only when a user actually watches.
--
-- This column supports issue #521 (serve "Další zdroje" from the DB
-- instead of a live scrape + per-result validate).

ALTER TABLE film_prehrajto_uploads
    ADD COLUMN IF NOT EXISTS is_direct BOOLEAN;

-- No index: the column is a per-row descriptor read as part of the
-- "all alive uploads for film X" query. The existing
-- `idx_fpu_film_alive` partial index already covers that access path.
