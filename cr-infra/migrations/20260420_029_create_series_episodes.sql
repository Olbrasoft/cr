-- Series catalog: TV shows with multiple seasons and episodes.
-- Mirrors the films structure but with episodes as a separate table.

CREATE TABLE series (
    id                  SERIAL PRIMARY KEY,
    title               VARCHAR(255) NOT NULL,
    original_title      VARCHAR(255),
    slug                VARCHAR(255) NOT NULL,
    first_air_year      SMALLINT,
    last_air_year       SMALLINT,
    description         TEXT,
    generated_description TEXT,
    imdb_id             VARCHAR(20),
    tmdb_id             INTEGER,
    csfd_id             INTEGER,
    imdb_rating         REAL,
    csfd_rating         SMALLINT,
    season_count        SMALLINT,
    episode_count       SMALLINT,
    cover_filename      VARCHAR(255),
    has_dub             BOOLEAN DEFAULT FALSE,
    has_subtitles       BOOLEAN DEFAULT FALSE,
    added_at            TIMESTAMP WITH TIME ZONE,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT series_slug_key UNIQUE (slug)
);

CREATE INDEX idx_series_imdb_id ON series (imdb_id) WHERE imdb_id IS NOT NULL;
CREATE INDEX idx_series_tmdb_id ON series (tmdb_id) WHERE tmdb_id IS NOT NULL;
CREATE INDEX idx_series_added_at ON series (added_at DESC NULLS LAST);
CREATE INDEX idx_series_imdb_rating ON series (imdb_rating DESC NULLS LAST);

-- Episodes: one row per episode
CREATE TABLE episodes (
    id                  SERIAL PRIMARY KEY,
    series_id           INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    season              SMALLINT NOT NULL,
    episode             SMALLINT NOT NULL,
    title               VARCHAR(500),
    air_date            DATE,
    sktorrent_video_id  INTEGER,
    sktorrent_cdn       SMALLINT,
    sktorrent_qualities VARCHAR(50),
    has_dub             BOOLEAN DEFAULT FALSE,
    has_subtitles       BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT episodes_unique UNIQUE (series_id, season, episode, sktorrent_video_id)
);

CREATE INDEX idx_episodes_series ON episodes (series_id, season, episode);
CREATE INDEX idx_episodes_sktorrent ON episodes (sktorrent_video_id) WHERE sktorrent_video_id IS NOT NULL;

-- Series → genres junction (same as film_genres)
CREATE TABLE series_genres (
    series_id  INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    genre_id   INTEGER NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (series_id, genre_id)
);

-- Cross-slug uniqueness: series slug must not collide with films/genres
CREATE OR REPLACE FUNCTION check_slug_not_series() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM series WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a series', NEW.slug;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION check_series_slug_not_film_or_genre() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM films WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a film', NEW.slug;
    END IF;
    IF EXISTS (SELECT 1 FROM genres WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'slug "%" already used by a genre', NEW.slug;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_series_slug_not_film_or_genre
    BEFORE INSERT OR UPDATE ON series
    FOR EACH ROW EXECUTE FUNCTION check_series_slug_not_film_or_genre();

-- Add TV-specific genres
INSERT INTO genres (slug, name_en, name_cs, tmdb_genre_id) VALUES
    ('zpravodajstvi', 'News', 'Zpravodajství', 10763),
    ('reality',       'Reality', 'Reality show', 10764),
    ('telenovela',    'Soap', 'Telenovela', 10766),
    ('talk-show',     'Talk', 'Talk show', 10767)
ON CONFLICT (slug) DO NOTHING;
