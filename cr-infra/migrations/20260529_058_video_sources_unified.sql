-- Unified polymorphic video source schema (issue #607 / sub-issue #608).
--
-- Replaces the per-provider tables (`film_prehrajto_uploads`,
-- `film_sledujteto_uploads`) and the ~15 per-provider denormalized columns on
-- `films` / `episodes` / `tv_episodes` with a single
-- `video_providers` + `video_sources` + `video_source_subtitles` stack.
--
-- This migration is PHASE 1 of a multi-phase refactor: it only LANDS the new
-- structure. Backfill, dual-write, reader switch, UX, and the legacy drop are
-- separate migrations / PRs (#609–#614). Legacy columns + tables continue to
-- exist and to be the source of truth until the reader switch ships.
--
-- Design notes (distilled from the issue + the RDBMS review comment on #608):
--
--   1. `video_sources` is polymorphic over (`film_id`, `episode_id`,
--      `tv_episode_id`). Exactly one of the three must be NOT NULL; enforced
--      via `num_nonnulls() = 1`. This lets us reuse the same table — and
--      therefore the same handler code — for films, series episodes, and TV
--      episodes.
--
--   2. `is_primary` replaces the scalar "primary pointer" columns
--      (`films.prehrajto_primary_upload_id` etc.). Without a constraint the
--      new schema would accept multiple primaries per (owner, provider), so
--      we back it with three partial unique indexes — one per parent column
--      (the standard Postgres way to index a nullable-key condition).
--
--   3. Rollup arrays `films.audio_langs` / `subtitle_langs` are maintained by
--      a trigger that recomputes them atomically in a single SQL statement
--      (not SELECT-then-UPDATE), so two parallel importers can't lost-update
--      each other under READ COMMITTED. The trigger grabs the row lock on
--      `films` via the UPDATE itself.
--
--   4. `lang_class` keeps the existing domain enum
--      (`CZ_DUB|CZ_NATIVE|CZ_SUB|SK_DUB|SK_SUB|EN|UNKNOWN`) as a CHECK
--      constraint, mirroring `film_prehrajto_uploads` / `film_sledujteto_uploads`
--      from migrations 048 and 057. A second CHECK ensures `lang_class` and
--      `audio_lang` can't drift into inconsistent states (e.g. `CZ_DUB` with
--      `audio_lang='en'`).
--
--   5. `cdn`, `duration_sec`, `resolution_hint`, `filesize_bytes`,
--      `view_count` stay as first-class columns (not JSONB) because they
--      participate in ORDER BY / WHERE in the listing and detail-page
--      queries. `metadata JSONB` is reserved for provider-specific bits with
--      no query pressure (e.g. sktorrent `qualities` string, `added_days_ago`).

-- =============================================================================
-- video_providers — lookup table for the three source systems.
-- Seeded with the three providers we support today. Adding a fourth is a
-- data change, not a schema change.
-- =============================================================================

CREATE TABLE IF NOT EXISTS video_providers (
    id             SMALLSERIAL PRIMARY KEY,
    slug           VARCHAR(32) NOT NULL UNIQUE,
    host           VARCHAR(64) NOT NULL,
    display_name   VARCHAR(64) NOT NULL,
    -- Lower number = shown first on the detail-page tab row. Replaces the
    -- hard-coded `sktorrent → prehrajto → sledujteto` ordering in the
    -- Askama templates.
    sort_priority  SMALLINT    NOT NULL DEFAULT 100,
    is_active      BOOLEAN     NOT NULL DEFAULT true,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO video_providers (slug, host, display_name, sort_priority)
VALUES
    ('sktorrent',  'online.sktorrent.eu', 'SK Torrent',    10),
    ('prehrajto',  'prehraj.to',          'Prehraj.to',    20),
    ('sledujteto', 'sledujteto.cz',       'Sledujteto.cz', 30)
ON CONFLICT (slug) DO NOTHING;

-- =============================================================================
-- video_sources — one row per (provider, parent entity, external id).
-- Parent is polymorphic: exactly one of film_id / episode_id / tv_episode_id.
-- =============================================================================

CREATE TABLE IF NOT EXISTS video_sources (
    id               SERIAL      PRIMARY KEY,
    provider_id      SMALLINT    NOT NULL REFERENCES video_providers(id) ON DELETE RESTRICT,

    -- Polymorphic parent: exactly one non-null, enforced by CHECK below.
    film_id          INTEGER     REFERENCES films(id)           ON DELETE CASCADE,
    episode_id       INTEGER     REFERENCES episodes(id) ON DELETE CASCADE,
    tv_episode_id    INTEGER     REFERENCES tv_episodes(id)     ON DELETE CASCADE,

    -- Provider-native id. sktorrent video_id is INT, prehrajto upload_id is
    -- 13/16-hex, sledujteto file_id is INT — all serialize into the same
    -- VARCHAR without loss. UNIQUE per provider below.
    external_id      VARCHAR(128) NOT NULL,

    title            TEXT,
    duration_sec     INTEGER,
    resolution_hint  VARCHAR(32),
    filesize_bytes   BIGINT,
    view_count       INTEGER,

    -- Language classification (preserved from legacy tables for transition;
    -- eventually derivable from audio_lang + subtitle rows).
    lang_class       VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN',

    -- Detected audio language (ISO 639-1/639-2 short code).
    audio_lang       VARCHAR(8),
    audio_confidence REAL,
    -- Which detector set audio_lang: whisper sample, title-regex, upstream
    -- metadata, or unknown.
    audio_detected_by VARCHAR(16),

    -- CDN/host family for the provider-specific playback URL.
    -- sledujteto: 'www' | 'data1'..'data9' | 'unknown'
    -- sktorrent:  server number as text ('22' etc.)
    -- prehrajto:  NULL (URLs are single-host + tokenized)
    cdn              VARCHAR(32),

    is_primary       BOOLEAN     NOT NULL DEFAULT false,
    is_alive         BOOLEAN     NOT NULL DEFAULT true,
    last_seen        TIMESTAMPTZ,
    last_checked     TIMESTAMPTZ,

    -- Schema-less provider-specific bag (sktorrent qualities, added_days_ago,
    -- legacy upload URLs, ...). No indexes over it — avoid putting anything
    -- here that needs fast filtering.
    metadata         JSONB,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Exactly one parent. Rejects rows where zero or multiple parent FKs are
    -- set — enforces the polymorphic contract at the DB level.
    CONSTRAINT video_sources_one_parent_check CHECK (
        num_nonnulls(film_id, episode_id, tv_episode_id) = 1
    ),

    -- Inherit the lang_class enum from legacy tables (migrations 048 + 057).
    CONSTRAINT video_sources_lang_class_check CHECK (
        lang_class IN ('CZ_DUB', 'CZ_NATIVE', 'CZ_SUB',
                       'SK_DUB', 'SK_SUB', 'EN', 'UNKNOWN')
    ),

    -- Prevent lang_class / audio_lang drift. For subtitle-only classes, we
    -- don't enforce the audio_lang value (original audio varies; it may be
    -- NULL or 'en' or 'de' etc.), just that it's not the same lang as the
    -- subtitles (otherwise the row should be DUB/NATIVE, not SUB).
    CONSTRAINT video_sources_lang_class_audio_consistency_check CHECK (
        (lang_class IN ('CZ_DUB','CZ_NATIVE') AND audio_lang = 'cs') OR
        (lang_class = 'CZ_SUB' AND (audio_lang IS NULL OR audio_lang <> 'cs')) OR
        (lang_class = 'SK_DUB' AND audio_lang = 'sk') OR
        (lang_class = 'SK_SUB' AND (audio_lang IS NULL OR audio_lang <> 'sk')) OR
        (lang_class = 'EN' AND audio_lang = 'en') OR
        (lang_class = 'UNKNOWN')
    ),

    CONSTRAINT video_sources_audio_lang_format_check CHECK (
        audio_lang IS NULL OR audio_lang ~ '^[a-z]{2,3}$'
    ),

    CONSTRAINT video_sources_audio_detected_by_check CHECK (
        audio_detected_by IS NULL
        OR audio_detected_by IN ('whisper', 'title_regex', 'upstream', 'unknown')
    ),

    CONSTRAINT video_sources_audio_confidence_range_check CHECK (
        audio_confidence IS NULL
        OR (audio_confidence >= 0.0 AND audio_confidence <= 1.0)
    )
);

-- Same external id across providers is fine (sktorrent 12345 ≠ sledujteto 12345);
-- within a provider the id must be globally unique.
CREATE UNIQUE INDEX IF NOT EXISTS uq_video_sources_provider_external
    ON video_sources (provider_id, external_id);

-- Main read paths — "all alive sources for this parent, per provider".
CREATE INDEX IF NOT EXISTS idx_vs_film_alive
    ON video_sources (film_id) WHERE is_alive AND film_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_vs_episode_alive
    ON video_sources (episode_id) WHERE is_alive AND episode_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_vs_tv_episode_alive
    ON video_sources (tv_episode_id) WHERE is_alive AND tv_episode_id IS NOT NULL;

-- Reconciliation sweep ("sources unseen for 30 days") — supports the periodic
-- is_alive=false flagging job.
CREATE INDEX IF NOT EXISTS idx_vs_last_seen_alive
    ON video_sources (last_seen) WHERE is_alive;

-- Primary-pointer integrity. Each partial unique index enforces "at most one
-- is_primary per (provider, parent)". Three indexes — one per parent column —
-- is the clean Postgres pattern when the partial condition involves a
-- non-null check on a nullable column.
CREATE UNIQUE INDEX IF NOT EXISTS uq_vs_primary_film
    ON video_sources (provider_id, film_id)
    WHERE is_primary AND film_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_vs_primary_episode
    ON video_sources (provider_id, episode_id)
    WHERE is_primary AND episode_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_vs_primary_tv_episode
    ON video_sources (provider_id, tv_episode_id)
    WHERE is_primary AND tv_episode_id IS NOT NULL;

-- =============================================================================
-- video_source_subtitles — 1:N child of video_sources.
-- Persisted even when URL is NULL (sledujteto subtitles are resolved at
-- play-time), so filters + badges work from the DB without live resolve.
-- =============================================================================

CREATE TABLE IF NOT EXISTS video_source_subtitles (
    id          SERIAL      PRIMARY KEY,
    source_id   INTEGER     NOT NULL REFERENCES video_sources(id) ON DELETE CASCADE,
    lang        VARCHAR(8)  NOT NULL,
    label       VARCHAR(64),
    -- 'srt' | 'vtt' | 'ass' | 'ssa' | NULL (unknown until resolved).
    format      VARCHAR(8),
    -- NULL when we know the track exists but haven't resolved the URL yet
    -- (sledujteto: playback endpoint returns URLs; crawler only records
    -- existence). Resolved URLs are re-fetched live from a proxy endpoint.
    url         TEXT,
    is_default  BOOLEAN     NOT NULL DEFAULT false,
    is_forced   BOOLEAN     NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT video_source_subtitles_lang_format_check CHECK (
        lang ~ '^[a-z]{2,3}$'
    )
);

-- Include `format` in the uniqueness key: a single upload can legitimately
-- carry .srt + .ass for the same (lang, is_forced) tuple, and we want both
-- rows persisted. Uses COALESCE because format can be NULL during the
-- window between subtitle discovery and the first URL resolve.
CREATE UNIQUE INDEX IF NOT EXISTS uq_vss_per_source_lang_format
    ON video_source_subtitles (source_id, lang, is_forced, COALESCE(format, ''));

CREATE INDEX IF NOT EXISTS idx_vss_source_id
    ON video_source_subtitles (source_id);

-- =============================================================================
-- Rollup arrays on parent tables + GIN indexes for the audio/subs filter.
--
-- These are maintained by the trigger below. They're a denormalization for
-- fast listing filters: `WHERE 'cs' = ANY(audio_langs)` lands a GIN index
-- lookup instead of a correlated subquery against video_sources.
-- =============================================================================

ALTER TABLE films
    ADD COLUMN IF NOT EXISTS audio_langs    TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    ADD COLUMN IF NOT EXISTS subtitle_langs TEXT[] NOT NULL DEFAULT '{}'::TEXT[];

ALTER TABLE episodes
    ADD COLUMN IF NOT EXISTS audio_langs    TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    ADD COLUMN IF NOT EXISTS subtitle_langs TEXT[] NOT NULL DEFAULT '{}'::TEXT[];

ALTER TABLE tv_episodes
    ADD COLUMN IF NOT EXISTS audio_langs    TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    ADD COLUMN IF NOT EXISTS subtitle_langs TEXT[] NOT NULL DEFAULT '{}'::TEXT[];

CREATE INDEX IF NOT EXISTS idx_films_audio_langs_gin
    ON films USING GIN (audio_langs);
CREATE INDEX IF NOT EXISTS idx_films_subtitle_langs_gin
    ON films USING GIN (subtitle_langs);

CREATE INDEX IF NOT EXISTS idx_episodes_audio_langs_gin
    ON episodes USING GIN (audio_langs);
CREATE INDEX IF NOT EXISTS idx_episodes_subtitle_langs_gin
    ON episodes USING GIN (subtitle_langs);

CREATE INDEX IF NOT EXISTS idx_tv_episodes_audio_langs_gin
    ON tv_episodes USING GIN (audio_langs);
CREATE INDEX IF NOT EXISTS idx_tv_episodes_subtitle_langs_gin
    ON tv_episodes USING GIN (subtitle_langs);

-- =============================================================================
-- Rollup trigger — keeps audio_langs / subtitle_langs in sync on parent rows.
--
-- Implementation note (race condition):
--   The recomputation is a single atomic UPDATE. A naive "SELECT array_agg →
--   UPDATE" sequence under READ COMMITTED allows two concurrent inserts into
--   video_sources for the same film to each compute a "before-the-other"
--   snapshot and clobber each other's contribution. A one-statement UPDATE
--   grabs the row lock on the parent row via its own UPDATE — the second
--   transaction then waits for the first to commit before re-evaluating the
--   subquery, so the final state reflects both inserts.
-- =============================================================================

CREATE OR REPLACE FUNCTION recompute_video_rollups_for_parent(
    p_film_id       INTEGER,
    p_episode_id    INTEGER,
    p_tv_episode_id INTEGER
) RETURNS VOID AS $$
BEGIN
    -- Atomic single-UPDATE per parent column. Whichever `p_*_id` argument
    -- is non-NULL, we update the corresponding table and set its
    -- `audio_langs` / `subtitle_langs` from a subquery over alive sources.
    IF p_film_id IS NOT NULL THEN
        UPDATE films f
        SET audio_langs = COALESCE(
                (SELECT array_agg(DISTINCT vs.audio_lang ORDER BY vs.audio_lang)
                 FROM video_sources vs
                 WHERE vs.film_id = p_film_id
                   AND vs.is_alive
                   AND vs.audio_lang IS NOT NULL),
                '{}'::TEXT[]),
            subtitle_langs = COALESCE(
                (SELECT array_agg(DISTINCT vss.lang ORDER BY vss.lang)
                 FROM video_sources vs
                 JOIN video_source_subtitles vss ON vss.source_id = vs.id
                 WHERE vs.film_id = p_film_id AND vs.is_alive),
                '{}'::TEXT[])
        WHERE f.id = p_film_id;
    END IF;

    IF p_episode_id IS NOT NULL THEN
        UPDATE episodes e
        SET audio_langs = COALESCE(
                (SELECT array_agg(DISTINCT vs.audio_lang ORDER BY vs.audio_lang)
                 FROM video_sources vs
                 WHERE vs.episode_id = p_episode_id
                   AND vs.is_alive
                   AND vs.audio_lang IS NOT NULL),
                '{}'::TEXT[]),
            subtitle_langs = COALESCE(
                (SELECT array_agg(DISTINCT vss.lang ORDER BY vss.lang)
                 FROM video_sources vs
                 JOIN video_source_subtitles vss ON vss.source_id = vs.id
                 WHERE vs.episode_id = p_episode_id AND vs.is_alive),
                '{}'::TEXT[])
        WHERE e.id = p_episode_id;
    END IF;

    IF p_tv_episode_id IS NOT NULL THEN
        UPDATE tv_episodes t
        SET audio_langs = COALESCE(
                (SELECT array_agg(DISTINCT vs.audio_lang ORDER BY vs.audio_lang)
                 FROM video_sources vs
                 WHERE vs.tv_episode_id = p_tv_episode_id
                   AND vs.is_alive
                   AND vs.audio_lang IS NOT NULL),
                '{}'::TEXT[]),
            subtitle_langs = COALESCE(
                (SELECT array_agg(DISTINCT vss.lang ORDER BY vss.lang)
                 FROM video_sources vs
                 JOIN video_source_subtitles vss ON vss.source_id = vs.id
                 WHERE vs.tv_episode_id = p_tv_episode_id AND vs.is_alive),
                '{}'::TEXT[])
        WHERE t.id = p_tv_episode_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Trigger on video_sources — fires after INSERT / UPDATE / DELETE.
-- For UPDATE we have to recompute for BOTH the old and new parent in case
-- a source is re-pointed (rare, but possible during backfill re-runs).
CREATE OR REPLACE FUNCTION trg_video_sources_rollup() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM recompute_video_rollups_for_parent(
            NEW.film_id, NEW.episode_id, NEW.tv_episode_id);
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM recompute_video_rollups_for_parent(
            OLD.film_id, OLD.episode_id, OLD.tv_episode_id);
    ELSE  -- UPDATE
        PERFORM recompute_video_rollups_for_parent(
            NEW.film_id, NEW.episode_id, NEW.tv_episode_id);
        -- If parent moved, also recompute OLD parent so it drops the lang.
        IF OLD.film_id IS DISTINCT FROM NEW.film_id
           OR OLD.episode_id IS DISTINCT FROM NEW.episode_id
           OR OLD.tv_episode_id IS DISTINCT FROM NEW.tv_episode_id THEN
            PERFORM recompute_video_rollups_for_parent(
                OLD.film_id, OLD.episode_id, OLD.tv_episode_id);
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_video_sources_rollup_aiud
    AFTER INSERT OR UPDATE OR DELETE ON video_sources
    FOR EACH ROW EXECUTE FUNCTION trg_video_sources_rollup();

-- Trigger on video_source_subtitles — subtitle changes don't change audio_lang
-- but do change subtitle_langs. We look up the owning source's parent ids
-- and recompute only for that parent.
CREATE OR REPLACE FUNCTION trg_video_source_subtitles_rollup() RETURNS TRIGGER AS $$
DECLARE
    v_film       INTEGER;
    v_episode    INTEGER;
    v_tv_episode INTEGER;
    v_source_id  INTEGER;
BEGIN
    -- Pick source_id from NEW or OLD depending on op.
    IF TG_OP = 'DELETE' THEN
        v_source_id := OLD.source_id;
    ELSE
        v_source_id := NEW.source_id;
    END IF;

    SELECT film_id, episode_id, tv_episode_id
    INTO   v_film, v_episode, v_tv_episode
    FROM   video_sources
    WHERE  id = v_source_id;

    IF FOUND THEN
        PERFORM recompute_video_rollups_for_parent(v_film, v_episode, v_tv_episode);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_video_source_subtitles_rollup_aiud
    AFTER INSERT OR UPDATE OR DELETE ON video_source_subtitles
    FOR EACH ROW EXECUTE FUNCTION trg_video_source_subtitles_rollup();

-- TRUNCATE triggers. Row-level triggers don't fire on TRUNCATE, which would
-- leave `audio_langs` / `subtitle_langs` rollup arrays stale on parent rows.
-- Production doesn't use TRUNCATE (the dual-write + reader switch pipelines
-- all use INSERT/UPDATE/DELETE), but a dev cleanup path or an accidental
-- TRUNCATE in a migration would silently desync the rollups without this.
CREATE OR REPLACE FUNCTION trg_video_sources_truncate() RETURNS TRIGGER AS $$
BEGIN
    UPDATE films       SET audio_langs = '{}'::TEXT[], subtitle_langs = '{}'::TEXT[]
                       WHERE cardinality(audio_langs) > 0 OR cardinality(subtitle_langs) > 0;
    UPDATE episodes    SET audio_langs = '{}'::TEXT[], subtitle_langs = '{}'::TEXT[]
                       WHERE cardinality(audio_langs) > 0 OR cardinality(subtitle_langs) > 0;
    UPDATE tv_episodes SET audio_langs = '{}'::TEXT[], subtitle_langs = '{}'::TEXT[]
                       WHERE cardinality(audio_langs) > 0 OR cardinality(subtitle_langs) > 0;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_video_sources_truncate_s
    AFTER TRUNCATE ON video_sources
    FOR EACH STATEMENT EXECUTE FUNCTION trg_video_sources_truncate();
