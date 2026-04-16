-- Drop all cross-table slug uniqueness triggers.
--
-- Slugs must only be unique WITHIN a category table. URL prefixes
-- (/filmy-online/, /serialy-online/, /tv-porady/) disambiguate the
-- entities — a film "Táta", a series "Táta" and a TV pořad "Táta" can
-- coexist and each is reachable at its own route.
--
-- Previously migrations 028, 029 and 041 enforced a global namespace
-- across films/genres/series/tv_shows, which was overly strict and
-- blocked legitimate name collisions (e.g. the film "Na nože" / Knives
-- Out 2019 vs. the Slovak reality cooking show "Na nože").

DROP TRIGGER IF EXISTS trg_films_slug_not_genre        ON films;
DROP TRIGGER IF EXISTS trg_genres_slug_not_film        ON genres;
DROP TRIGGER IF EXISTS trg_series_slug_not_film_or_genre      ON series;
DROP TRIGGER IF EXISTS trg_tv_show_slug_not_film_series_or_genre ON tv_shows;

DROP FUNCTION IF EXISTS check_slug_not_genre();
DROP FUNCTION IF EXISTS check_slug_not_film();
DROP FUNCTION IF EXISTS check_slug_not_series();
DROP FUNCTION IF EXISTS check_series_slug_not_film_or_genre();
DROP FUNCTION IF EXISTS check_tv_show_slug_not_film_series_or_genre();
