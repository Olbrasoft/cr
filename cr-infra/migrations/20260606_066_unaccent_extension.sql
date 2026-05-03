-- Enable the `unaccent` extension so search queries on /filmy-online/
-- and /serialy-online/ can match titles regardless of diacritics
-- ("laska nebeska" finds "Láska nebeská"). Used by the films/series
-- list and autocomplete handlers, where ILIKE is wrapped in
-- `unaccent(...)` on both the column and the bound pattern.
--
-- Diacritic-exact ILIKE matches stay first in result ordering — only
-- when the exact pattern returns nothing do unaccented matches fill in.
-- See cr-web/src/handlers/films.rs and cr-web/src/handlers/series.rs.
CREATE EXTENSION IF NOT EXISTS unaccent;
