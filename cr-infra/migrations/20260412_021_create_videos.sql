-- Hosted video library: every video downloaded via /stahnout-video/ that
-- has been uploaded to our Streamtape account, with the matching thumbnail
-- stored on Cloudflare R2. Used by the library grid + inline player on the
-- /stahnout-video/ page.
--
-- Dedup key: (source_url, quality). Same source URL can exist multiple
-- times in different qualities (e.g. 1080p and the WhatsApp variant) but
-- never twice in the same quality.

CREATE TABLE videos (
    id                 SERIAL      PRIMARY KEY,
    source_url         TEXT        NOT NULL,
    title              TEXT        NOT NULL,
    description        TEXT,
    duration_sec       INTEGER,
    source_extractor   TEXT,                                -- yt-dlp extractor name (youtube, novinky, ...)
    quality            TEXT        NOT NULL,                -- "1080p" | "720p" | ... | "whatsapp"
    format_ext         TEXT        NOT NULL,                -- "mp4" | "webm" | ...

    -- Streamtape (file hosting)
    streamtape_file_id TEXT        NOT NULL UNIQUE,         -- stable 14-15 char id
    streamtape_url     TEXT        NOT NULL,                -- https://streamtape.com/v/{id}/{name}
    file_size_bytes    BIGINT      NOT NULL,

    -- Thumbnail on Cloudflare R2 (cr-images bucket, prefix videos/thumbs/)
    thumbnail_r2_key   TEXT,
    thumbnail_url      TEXT,

    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT videos_source_quality_unique UNIQUE (source_url, quality)
);

CREATE INDEX idx_videos_created_at ON videos (created_at DESC);
