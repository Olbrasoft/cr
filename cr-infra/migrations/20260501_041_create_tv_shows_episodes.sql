-- TV pořady (reality, talk shows, cooking, soaps, etc.) as a separate
-- catalog from scripted series. TV pořady must NEVER appear in the
-- /serialy-online/ listing — they live at /tv-porady/ with their own
-- URL space, their own homepage tile, their own import pipeline.
--
-- Tables mirror the current schema of series/episodes so that existing
-- handlers and image pipelines (covers, stills) can be adapted 1:1.
-- When migrations 029..040 are applied, `series` has the superset of
-- columns included below; `tv_shows` is kept in lockstep so moving
-- data between them (scripts/move-tv-porady-to-new-tables.py, #463)
-- is a straight column-for-column copy.

CREATE TABLE tv_shows (
    id                    SERIAL PRIMARY KEY,
    title                 VARCHAR(255) NOT NULL,
    original_title        VARCHAR(255),
    slug                  VARCHAR(255) NOT NULL,
    first_air_year        SMALLINT,
    last_air_year         SMALLINT,
    description           TEXT,
    generated_description TEXT,
    tmdb_overview_en      TEXT,
    imdb_id               VARCHAR(20),
    tmdb_id               INTEGER,
    csfd_id               INTEGER,
    imdb_rating           REAL,
    csfd_rating           SMALLINT,
    season_count          SMALLINT,
    episode_count         SMALLINT,
    cover_filename        VARCHAR(255),
    has_dub               BOOLEAN DEFAULT FALSE,
    has_subtitles         BOOLEAN DEFAULT FALSE,
    old_slug              VARCHAR,
    added_at              TIMESTAMP WITH TIME ZONE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT tv_shows_slug_key UNIQUE (slug)
);

CREATE INDEX idx_tv_shows_imdb_id     ON tv_shows (imdb_id)     WHERE imdb_id IS NOT NULL;
CREATE INDEX idx_tv_shows_tmdb_id     ON tv_shows (tmdb_id)     WHERE tmdb_id IS NOT NULL;
CREATE INDEX idx_tv_shows_added_at    ON tv_shows (added_at DESC NULLS LAST);
CREATE INDEX idx_tv_shows_imdb_rating ON tv_shows (imdb_rating DESC NULLS LAST);

CREATE TABLE tv_episodes (
    id                    SERIAL PRIMARY KEY,
    tv_show_id            INTEGER NOT NULL REFERENCES tv_shows(id) ON DELETE CASCADE,
    season                SMALLINT NOT NULL,
    episode               SMALLINT NOT NULL,
    title                 VARCHAR(500),
    slug                  VARCHAR,
    episode_name          TEXT,
    overview              TEXT,
    overview_en           TEXT,
    generated_description TEXT,
    air_date              DATE,
    runtime               SMALLINT,
    still_filename        VARCHAR(255),
    vote_average          REAL,
    sktorrent_video_id    INTEGER,
    sktorrent_cdn         SMALLINT,
    sktorrent_qualities   VARCHAR(50),
    sktorrent_added_at    TIMESTAMP WITH TIME ZONE,
    prehrajto_url         VARCHAR(500),
    prehrajto_has_dub     BOOLEAN NOT NULL DEFAULT FALSE,
    prehrajto_has_subs    BOOLEAN NOT NULL DEFAULT FALSE,
    has_dub               BOOLEAN DEFAULT FALSE,
    has_subtitles         BOOLEAN DEFAULT FALSE,
    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT tv_episodes_unique UNIQUE (tv_show_id, season, episode, sktorrent_video_id),
    CONSTRAINT tv_episodes_slug_unique UNIQUE (tv_show_id, slug)
);

CREATE INDEX idx_tv_episodes_show          ON tv_episodes (tv_show_id, season, episode);
CREATE INDEX idx_tv_episodes_sktorrent     ON tv_episodes (sktorrent_video_id) WHERE sktorrent_video_id IS NOT NULL;
CREATE INDEX idx_tv_episodes_prehrajto_url ON tv_episodes (prehrajto_url)      WHERE prehrajto_url      IS NOT NULL;
CREATE INDEX idx_tv_episodes_slug          ON tv_episodes (slug)                WHERE slug               IS NOT NULL;

-- Genres junction (same shape as series_genres / film_genres).
CREATE TABLE tv_show_genres (
    tv_show_id INTEGER NOT NULL REFERENCES tv_shows(id) ON DELETE CASCADE,
    genre_id   INTEGER NOT NULL REFERENCES genres(id)   ON DELETE CASCADE,
    PRIMARY KEY (tv_show_id, genre_id)
);

-- Cross-slug uniqueness: a tv_shows slug must not collide with films,
-- series or genres (and vice versa). Films and genres already have
-- triggers checking each other plus series; extend the series trigger
-- to also reject tv_shows collisions, and add a symmetric trigger on
-- tv_shows.
CREATE OR REPLACE FUNCTION check_series_slug_not_film_or_genre() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM films     WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a film', NEW.slug;
    END IF;
    IF EXISTS (SELECT 1 FROM genres    WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a genre', NEW.slug;
    END IF;
    IF EXISTS (SELECT 1 FROM tv_shows  WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a tv_show', NEW.slug;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION check_tv_show_slug_not_film_series_or_genre() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM films  WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a film', NEW.slug;
    END IF;
    IF EXISTS (SELECT 1 FROM genres WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a genre', NEW.slug;
    END IF;
    IF EXISTS (SELECT 1 FROM series WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a series', NEW.slug;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tv_show_slug_not_film_series_or_genre
    BEFORE INSERT OR UPDATE ON tv_shows
    FOR EACH ROW EXECUTE FUNCTION check_tv_show_slug_not_film_series_or_genre();
