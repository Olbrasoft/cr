-- Episode metadata + normalized people/credits tables.
-- Episode-level data (name, overview, air_date, runtime, still, rating)
-- comes from TMDB /tv/{id}/season/{n}. Cast/crew comes from
-- /tv/{id}/credits (series-level) and /tv/{id}/season/{n}/episode/{m}/credits
-- (episode-level). One person can hold multiple roles, so we keep a single
-- `people` master table referenced from role-specific join tables.

ALTER TABLE episodes
    ADD COLUMN IF NOT EXISTS episode_name   TEXT,
    ADD COLUMN IF NOT EXISTS overview       TEXT,
    ADD COLUMN IF NOT EXISTS air_date       DATE,
    ADD COLUMN IF NOT EXISTS runtime        SMALLINT,
    ADD COLUMN IF NOT EXISTS still_filename VARCHAR(255),
    ADD COLUMN IF NOT EXISTS vote_average   REAL;

-- Master table of people (actors, directors, writers — same person can be all).
CREATE TABLE IF NOT EXISTS people (
    id               SERIAL PRIMARY KEY,
    tmdb_id          INTEGER UNIQUE,
    name             VARCHAR(255) NOT NULL,
    profile_filename VARCHAR(255),  -- WebP file in data/series/people/
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_people_name ON people (name);

-- Series-level cast (actors playing characters across the show).
CREATE TABLE IF NOT EXISTS series_actors (
    series_id      INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    person_id      INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    character_name VARCHAR(255),
    order_index    SMALLINT NOT NULL DEFAULT 0,
    PRIMARY KEY (series_id, person_id)
);
CREATE INDEX IF NOT EXISTS idx_series_actors_person ON series_actors (person_id);

-- Series-level directors / creators (showrunners).
CREATE TABLE IF NOT EXISTS series_directors (
    series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    person_id INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    PRIMARY KEY (series_id, person_id)
);
CREATE INDEX IF NOT EXISTS idx_series_directors_person ON series_directors (person_id);

-- Per-episode director(s).
CREATE TABLE IF NOT EXISTS episode_directors (
    episode_id INTEGER NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    person_id  INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    PRIMARY KEY (episode_id, person_id)
);
CREATE INDEX IF NOT EXISTS idx_episode_directors_person ON episode_directors (person_id);

-- Per-episode writer(s).
CREATE TABLE IF NOT EXISTS episode_writers (
    episode_id INTEGER NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    person_id  INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    PRIMARY KEY (episode_id, person_id)
);
CREATE INDEX IF NOT EXISTS idx_episode_writers_person ON episode_writers (person_id);
