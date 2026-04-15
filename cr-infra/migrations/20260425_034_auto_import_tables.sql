-- Auto-import pipeline tables (Issue #413, sub-issue #414)
--
-- Four tables supporting daily scanner of SK Torrent:
--   import_runs         — history of each scan run (counts + status)
--   import_items        — per-video detail (action, target, failure info, raw_log)
--   import_skipped_videos — blacklist of IMDB-unresolvable videos (idempotency)
--   import_checkpoint   — singleton row with highest processed sktorrent_video_id
--
-- Checkpoint is seeded to current max sktorrent_video_id across films + episodes
-- so the first run only picks up videos uploaded AFTER the initial deploy.

CREATE TABLE IF NOT EXISTS import_runs (
    id                  SERIAL PRIMARY KEY,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'ok', 'error', 'partial')),
    trigger             TEXT NOT NULL DEFAULT 'cron'
        CHECK (trigger IN ('cron', 'manual')),
    scanned_pages       SMALLINT NOT NULL DEFAULT 0,
    scanned_videos      SMALLINT NOT NULL DEFAULT 0,
    checkpoint_before   INT,
    checkpoint_after    INT,
    added_films         SMALLINT NOT NULL DEFAULT 0,
    added_series        SMALLINT NOT NULL DEFAULT 0,
    added_episodes      SMALLINT NOT NULL DEFAULT 0,
    updated_films       SMALLINT NOT NULL DEFAULT 0,
    updated_episodes    SMALLINT NOT NULL DEFAULT 0,
    failed_count        SMALLINT NOT NULL DEFAULT 0,
    skipped_count       SMALLINT NOT NULL DEFAULT 0,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_import_runs_started_at
    ON import_runs (started_at DESC);

CREATE TABLE IF NOT EXISTS import_items (
    id                  SERIAL PRIMARY KEY,
    run_id              INT NOT NULL REFERENCES import_runs(id) ON DELETE CASCADE,
    sktorrent_video_id  INT NOT NULL,
    sktorrent_url       TEXT NOT NULL,
    sktorrent_title     TEXT NOT NULL,
    detected_type       TEXT
        CHECK (detected_type IS NULL OR detected_type IN ('film', 'series', 'unknown')),
    imdb_id             VARCHAR(20),
    tmdb_id             INT,
    season              SMALLINT,
    episode             SMALLINT,
    action              TEXT NOT NULL
        CHECK (action IN (
            'added_film', 'added_series', 'added_episode',
            'updated_film', 'updated_episode',
            'skipped', 'failed'
        )),
    target_film_id      INT,
    target_series_id    INT,
    target_episode_id   INT,
    failure_step        TEXT,
    failure_message     TEXT,
    raw_log             JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_import_items_run
    ON import_items (run_id);
CREATE INDEX IF NOT EXISTS idx_import_items_sktid
    ON import_items (sktorrent_video_id);
CREATE INDEX IF NOT EXISTS idx_import_items_action
    ON import_items (action);

CREATE TABLE IF NOT EXISTS import_skipped_videos (
    sktorrent_video_id  INT PRIMARY KEY,
    reason              TEXT NOT NULL,
    last_tried_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    try_count           SMALLINT NOT NULL DEFAULT 1
);

-- Singleton (id = 1, enforced by CHECK) to store the highest processed
-- sktorrent_video_id. Seeded from current films+episodes max so the first
-- scanner run only picks up genuinely new uploads.
CREATE TABLE IF NOT EXISTS import_checkpoint (
    id                      SMALLINT PRIMARY KEY DEFAULT 1
        CHECK (id = 1),
    last_sktorrent_video_id INT NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO import_checkpoint (id, last_sktorrent_video_id)
SELECT 1, COALESCE(
    GREATEST(
        (SELECT max(sktorrent_video_id) FROM films),
        (SELECT max(sktorrent_video_id) FROM episodes)
    ),
    0
)
ON CONFLICT (id) DO NOTHING;
