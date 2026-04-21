-- Stores the TMDB `poster_path` for each series, e.g. `/mqlgZiQHT3B5J8Q4mkgaCkt47uJ.jpg`.
-- Needed so the large-cover route can build the live TMDB URL
-- (`https://image.tmdb.org/t/p/w780{poster_path}`) without having to call the
-- TMDB API on every detail page render. Backfilled once by
-- `scripts/backfill-tmdb-poster-paths.py --table series` and kept fresh by the enricher.
ALTER TABLE series ADD COLUMN tmdb_poster_path VARCHAR(64);
