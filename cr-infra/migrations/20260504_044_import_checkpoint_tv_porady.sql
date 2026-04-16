-- #482 — separate checkpoint for SK Torrent `/videos/tv-porady/` section.
--
-- The generic `/videos` listing and the TV pořady sub-section each have
-- their own newest-first ordering and their own highest-processed
-- sktorrent_video_id. Tracking them independently means we don't
-- re-scan tv-porady items every day just because they sit below the
-- generic checkpoint (or vice-versa). `0` means "never scanned".

ALTER TABLE import_checkpoint
    ADD COLUMN IF NOT EXISTS last_sktorrent_video_id_tv_porady INTEGER NOT NULL DEFAULT 0;
