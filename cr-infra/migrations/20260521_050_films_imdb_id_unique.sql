-- Upgrade idx_films_imdb_id from a non-unique partial index to a UNIQUE one.
-- Enables INSERT ... ON CONFLICT (imdb_id) DO NOTHING semantics for the
-- new-film bulk importer in scripts/import-prehrajto-new-films.py (issue #524).
--
-- Safety: verified on stage that no duplicate non-null imdb_id values exist
-- today (films.imdb_id is effectively functional-unique already, just never
-- constrained). Non-null imdb_id entries in films table are populated
-- exclusively by the auto-import and prehraj.to importers, both of which
-- already check for existing rows before inserting.
--
-- NULL imdb_id rows (historical, brand-new releases with no IMDB yet) are
-- allowed multiple times because the index is partial.
--
-- Create the new unique index BEFORE dropping the old non-unique one, so
-- that if CREATE fails (unexpected duplicate) the table still has an
-- imdb_id index and query plans don't regress.

CREATE UNIQUE INDEX IF NOT EXISTS idx_films_imdb_id_unique
    ON films (imdb_id)
    WHERE imdb_id IS NOT NULL;

DROP INDEX IF EXISTS idx_films_imdb_id;
