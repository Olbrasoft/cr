-- =============================================================================
-- film_actors + film_directors — mirror of series_actors / series_directors
-- so the `/filmy-online/{slug}/` detail page can render the same Tvůrci/Herci
-- cards series got in #720.
--
-- Both tables share the `(film_id, person_id)` PK with `ON DELETE CASCADE`
-- — removing a film cleans up its credits; removing a person from the
-- `people` table (rare, manual cleanup only) cascades to their roles.
--
-- `character_name VARCHAR(255)` follows the series schema. The series
-- backfill hit exactly one TMDB row with a longer name; if the same edge
-- case bites films we'll widen the column then. `order_index SMALLINT`
-- mirrors TMDB's `order` (billing rank) and is what the detail page
-- sorts by.
-- =============================================================================

CREATE TABLE IF NOT EXISTS film_actors (
    film_id        INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    person_id      INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    character_name VARCHAR(255),
    order_index    SMALLINT NOT NULL DEFAULT 0,
    PRIMARY KEY (film_id, person_id)
);

CREATE INDEX IF NOT EXISTS idx_film_actors_person
    ON film_actors (person_id);

CREATE TABLE IF NOT EXISTS film_directors (
    film_id   INTEGER NOT NULL REFERENCES films(id) ON DELETE CASCADE,
    person_id INTEGER NOT NULL REFERENCES people(id) ON DELETE CASCADE,
    PRIMARY KEY (film_id, person_id)
);

CREATE INDEX IF NOT EXISTS idx_film_directors_person
    ON film_directors (person_id);
