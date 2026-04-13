-- Film listing: films, genres, and the junction table film_genres.
-- Slugs are unique across BOTH films and genres (shared URL namespace under /filmy-online/).

CREATE TABLE genres (
    id       SERIAL      PRIMARY KEY,
    slug     VARCHAR(50) NOT NULL UNIQUE,   -- English, URL-safe: "horror", "action"
    name_en  VARCHAR(50) NOT NULL,          -- "Horror", "Action"
    name_cs  VARCHAR(50) NOT NULL           -- "Horor", "Akční"
);

-- 19 standard genres based on TMDB genre IDs, Czech slugs for SEO
INSERT INTO genres (slug, name_en, name_cs) VALUES
    ('akcni',         'Action',        'Akční'),
    ('dobrodruzny',   'Adventure',     'Dobrodružný'),
    ('animovany',     'Animation',     'Animovaný'),
    ('komedie',       'Comedy',        'Komedie'),
    ('krimi',         'Crime',         'Krimi'),
    ('dokumentarni',  'Documentary',   'Dokumentární'),
    ('drama',         'Drama',         'Drama'),
    ('rodinny',       'Family',        'Rodinný'),
    ('fantasy',       'Fantasy',       'Fantasy'),
    ('historicky',    'History',       'Historický'),
    ('horor',         'Horror',        'Horor'),
    ('hudebni',       'Music',         'Hudební'),
    ('mysteriozni',   'Mystery',       'Mysteriózní'),
    ('romanticky',    'Romance',       'Romantický'),
    ('sci-fi',        'Science Fiction','Sci-Fi'),
    ('thriller',      'Thriller',      'Thriller'),
    ('valecny',       'War',           'Válečný'),
    ('western',       'Western',       'Western'),
    ('tv-film',       'TV Movie',      'TV film');

CREATE TABLE films (
    id                SERIAL       PRIMARY KEY,
    title             VARCHAR(255) NOT NULL,           -- Czech title
    original_title    VARCHAR(255),                    -- Original (English) title
    slug              VARCHAR(255) NOT NULL UNIQUE,    -- URL-safe, shared namespace with genres
    year              SMALLINT,
    description       TEXT,                            -- Generated unique description
    imdb_id           VARCHAR(20),                     -- tt1234567
    tmdb_id           INTEGER,
    csfd_id           INTEGER,
    imdb_rating       REAL,
    csfd_rating       SMALLINT,                        -- 0-100
    runtime_min       SMALLINT,
    cover_filename    VARCHAR(255),                    -- WebP filename in covers dir
    lang              VARCHAR(20),                     -- CZ, SK, EN, CZ/EN
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE film_genres (
    film_id   INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    genre_id  INTEGER NOT NULL REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (film_id, genre_id)
);

-- Indexes for common queries
CREATE INDEX idx_films_year         ON films (year DESC NULLS LAST);
CREATE INDEX idx_films_imdb_id      ON films (imdb_id) WHERE imdb_id IS NOT NULL;
CREATE INDEX idx_films_tmdb_id      ON films (tmdb_id) WHERE tmdb_id IS NOT NULL;
CREATE INDEX idx_films_csfd_id      ON films (csfd_id) WHERE csfd_id IS NOT NULL;
CREATE INDEX idx_films_imdb_rating  ON films (imdb_rating DESC NULLS LAST);
CREATE INDEX idx_film_genres_genre  ON film_genres (genre_id);

-- Cross-table slug uniqueness: films and genres share /filmy-online/ URL namespace.
-- Two triggers ensure no slug collision in either direction.

CREATE OR REPLACE FUNCTION check_slug_not_genre() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM genres WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'Film slug "%" collides with existing genre slug', NEW.slug;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_films_slug_not_genre
    BEFORE INSERT OR UPDATE ON films
    FOR EACH ROW EXECUTE FUNCTION check_slug_not_genre();

CREATE OR REPLACE FUNCTION check_slug_not_film() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM films WHERE slug = NEW.slug) THEN
        RAISE EXCEPTION 'Genre slug "%" collides with existing film slug', NEW.slug;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_genres_slug_not_film
    BEFORE INSERT OR UPDATE ON genres
    FOR EACH ROW EXECUTE FUNCTION check_slug_not_film();
