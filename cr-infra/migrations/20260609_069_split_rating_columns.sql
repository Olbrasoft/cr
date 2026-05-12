-- Split the misnamed `imdb_rating` column into proper TMDB / IMDb columns.
--
-- Background: parent issue #588 documents that the `imdb_rating` column in
-- both `films` and `series` actually holds TMDB's `vote_average` — never a
-- real IMDb rating. The original auto-import (enricher.py) seeds it from
-- TMDB and #584/#586 papered over the mismatch by relabelling the UI badge
-- to "TMDB X". This migration finally splits the column so the names match
-- the data: TMDB's vote_average moves to a new `tmdb_rating` column, and
-- the cleared `imdb_rating` column is reserved for the real IMDb rating
-- that sub-issue #690 will populate from the IMDb datasets TSV.
--
-- After this migration:
--   - films.tmdb_rating / series.tmdb_rating hold what `imdb_rating` did before
--   - films.imdb_rating / series.imdb_rating are NULL — populated later by
--     scripts/sync-imdb-ratings.py from https://datasets.imdbws.com/title.ratings.tsv.gz
--   - both sources carry their own `*_synced_at` timestamp so the daily
--     freshness audit can tell when each was last refreshed
--   - tmdb_vote_count is a new field (TMDB's `vote_count`) so we can later
--     rank quality of TMDB ratings; backfill happens in a follow-up sync.
--
-- Idempotency (Copilot review on PR #691):
--   * Copy is gated on `tmdb_rating IS NULL` — never clobber an already-
--     populated tmdb_rating with the now-empty imdb_rating on a re-run.
--   * Clear is gated on `imdb_rating_synced_at IS NULL` — that timestamp
--     is only ever set by scripts/sync-imdb-ratings.py (#690), so before
--     #690 lands every row matches, and after #690 runs the real IMDb
--     ratings are skipped instead of wiped.
--   * tmdb_rating_synced_at is stamped during the copy so freshness audits
--     can tell when each TMDB number was last touched.

-- films -----------------------------------------------------------------
ALTER TABLE films
    ADD COLUMN IF NOT EXISTS tmdb_rating            REAL,
    ADD COLUMN IF NOT EXISTS tmdb_vote_count        INTEGER,
    ADD COLUMN IF NOT EXISTS tmdb_rating_synced_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS imdb_votes             INTEGER,
    ADD COLUMN IF NOT EXISTS imdb_rating_synced_at  TIMESTAMPTZ;

UPDATE films
   SET tmdb_rating = imdb_rating,
       tmdb_rating_synced_at = now()
 WHERE imdb_rating IS NOT NULL
   AND tmdb_rating IS NULL;

UPDATE films
   SET imdb_rating = NULL
 WHERE imdb_rating IS NOT NULL
   AND imdb_rating_synced_at IS NULL;

-- series ----------------------------------------------------------------
ALTER TABLE series
    ADD COLUMN IF NOT EXISTS tmdb_rating            REAL,
    ADD COLUMN IF NOT EXISTS tmdb_vote_count        INTEGER,
    ADD COLUMN IF NOT EXISTS tmdb_rating_synced_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS imdb_votes             INTEGER,
    ADD COLUMN IF NOT EXISTS imdb_rating_synced_at  TIMESTAMPTZ;

UPDATE series
   SET tmdb_rating = imdb_rating,
       tmdb_rating_synced_at = now()
 WHERE imdb_rating IS NOT NULL
   AND tmdb_rating IS NULL;

UPDATE series
   SET imdb_rating = NULL
 WHERE imdb_rating IS NOT NULL
   AND imdb_rating_synced_at IS NULL;

-- tv_shows --------------------------------------------------------------
-- TV pořady (`tv_shows`) share the same rating model as `series`; the
-- handlers in tv_porady.rs render the badge from the same column names,
-- so the split has to mirror across both tables.
ALTER TABLE tv_shows
    ADD COLUMN IF NOT EXISTS tmdb_rating            REAL,
    ADD COLUMN IF NOT EXISTS tmdb_vote_count        INTEGER,
    ADD COLUMN IF NOT EXISTS tmdb_rating_synced_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS imdb_votes             INTEGER,
    ADD COLUMN IF NOT EXISTS imdb_rating_synced_at  TIMESTAMPTZ;

UPDATE tv_shows
   SET tmdb_rating = imdb_rating,
       tmdb_rating_synced_at = now()
 WHERE imdb_rating IS NOT NULL
   AND tmdb_rating IS NULL;

UPDATE tv_shows
   SET imdb_rating = NULL
 WHERE imdb_rating IS NOT NULL
   AND imdb_rating_synced_at IS NULL;

-- Indexes ---------------------------------------------------------------
-- Sorting by rating ("top rated" lists) now uses tmdb_rating, since that
-- is where the existing data lives. The pre-existing imdb_rating indexes
-- stay in place — they will be useful again once #690 populates the
-- column with real IMDb ratings — but they currently sort all NULLs.
CREATE INDEX IF NOT EXISTS idx_films_tmdb_rating
    ON films (tmdb_rating DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_series_tmdb_rating
    ON series (tmdb_rating DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_tv_shows_tmdb_rating
    ON tv_shows (tmdb_rating DESC NULLS LAST);
