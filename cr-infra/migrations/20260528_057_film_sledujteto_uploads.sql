-- Per-upload metadata for every sledujteto.cz video linked to a film. One film
-- can have many uploads (observed in pilot: 1-5× the same film) with different
-- language markers, resolutions, and CDN hosts. Import source is a
-- title-first search crawl via the SledujteToCzProxy (issue #545/#597);
-- audio-language is detected separately by whisper on a short sample
-- (scripts/sledujteto-detect-audio.py), because sledujteto title strings
-- frequently miss dubbing hints.
--
-- Serves three consumers (mirrors the prehrajto.cz model in
-- 20260508_048_film_prehrajto_uploads.sql):
--   1) Detail-page "Další zdroje" — reads from this table.
--   2) /api/movies/stream/sledujteto/<file_id> player endpoint — picks the
--      primary upload and has a deterministic fallback when an upload on
--      sledujteto.cz disappears.
--   3) Audio filter on /filmy-a-serialy — aggregated booleans on `films`
--      (below) are filled as UNION over alive uploads.
--
-- Key differences from prehrajto.cz:
--   a) `file_id` is INT (sledujteto's internal `files.id`), not a hex token.
--   b) `cdn` tracks www / data{N} / unknown — critical for filtering
--      playable copies (datacenter ASNs can stream from `www.sledujteto.cz`
--      but are blocked from `data{N}.sledujteto.cz`, see issue #549).

CREATE TABLE IF NOT EXISTS film_sledujteto_uploads (
    film_id          INTEGER     NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    -- Sledujteto internal `files.id` (stable DB key, int). The short URL
    -- slug visible in the browser (`/file/<slug_id>/<name>.html`) is a
    -- separate value; we keep `file_id` here because it's what the
    -- `POST /services/add-file-link` endpoint consumes to mint a playback
    -- URL, and because it's monotonically stable while slugs can be
    -- rewritten by the uploader.
    file_id          INTEGER     NOT NULL,
    -- Raw title from the uploader — used as the input for language
    -- classification below (scripts/sledujteto-detect-audio.py combines
    -- this with a whisper sample of the actual audio track).
    title            TEXT        NOT NULL,
    duration_sec     INTEGER,
    -- Detected language class. Sledujteto uploads are less disciplined
    -- than prehraj.to in their naming conventions, so this is a merge of
    -- title-derived hints and whisper audio-detection results.
    lang_class       TEXT        NOT NULL DEFAULT 'UNKNOWN',
    -- Resolution hint parsed from the upload metadata (`1920x1080`,
    -- `1280x720`, `720p`, …). TEXT + no constraint because the shape
    -- varies by uploader.
    resolution_hint  TEXT,
    -- Raw filesize in bytes. Useful for ranking (higher bitrate ≈ better
    -- quality, tie-breaker against resolution) and for UI display.
    filesize_bytes   BIGINT,
    -- CDN host family for this upload's `video_url`:
    --   'www'      → www.sledujteto.cz — playable from any ASN (Hetzner, Oracle)
    --   'dataN'    → data{N}.sledujteto.cz — blocked from datacenter ASNs
    --   'unknown'  → resolve hasn't run yet or returned an unparseable URL
    -- The import pipeline sets this per upload via the Hash.ashx proxy
    -- endpoint (which itself parses `video_url` from the add-file-link
    -- response). See issue #549 for the data{N} routing strategy.
    cdn              TEXT        NOT NULL DEFAULT 'unknown',
    -- FALSE means "search crawl no longer finds it" or "add-file-link
    -- returned an error / missing video_url". The primary-upload picker
    -- skips these; the reconciliation job sweeps them to cross-check
    -- against upstream before permanent delete.
    is_alive         BOOLEAN     NOT NULL DEFAULT TRUE,
    last_seen        TIMESTAMPTZ,
    last_checked     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (film_id, file_id),
    CONSTRAINT film_sledujteto_uploads_lang_check CHECK (
        lang_class IN ('CZ_DUB', 'CZ_NATIVE', 'CZ_SUB',
                       'SK_DUB', 'SK_SUB', 'EN', 'UNKNOWN')
    )
);

-- Main read pattern: "all alive uploads for a given film, sorted" (Další
-- zdroje listing + primary selection). Partial index on `film_id` only for
-- rows where is_alive = TRUE; is_alive itself is not a key column.
CREATE INDEX IF NOT EXISTS idx_fsu_film_alive
    ON film_sledujteto_uploads (film_id)
    WHERE is_alive;

-- Secondary: reconciliation job hunts uploads that haven't been seen in a
-- while (WHERE last_seen < NOW() - INTERVAL '30 days'). Index helps this
-- periodic full-table scan stay fast as the table grows.
CREATE INDEX IF NOT EXISTS idx_fsu_last_seen
    ON film_sledujteto_uploads (last_seen)
    WHERE is_alive;

-- Rollup flags and preferred upload per film. Unlike prehraj.to (where CZ
-- flags were already present from migration #032), sledujteto introduces
-- the whole column family fresh — both CZ and SK flavors.
ALTER TABLE films
    ADD COLUMN IF NOT EXISTS sledujteto_primary_file_id INTEGER,
    ADD COLUMN IF NOT EXISTS sledujteto_has_dub     BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS sledujteto_has_subs    BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS sledujteto_has_sk_dub  BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS sledujteto_has_sk_subs BOOLEAN NOT NULL DEFAULT FALSE;
