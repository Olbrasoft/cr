-- Enforce one film row per TMDB id and clean up the 8 historical duplicates
-- that the SK Torrent auto-import produced.
--
-- Background: `upsert_film` (auto-import) used to look the existing film up
-- by `imdb_id` only. Films imported from earlier sources (e.g. legacy
-- prehrajto-only flow) had `imdb_id = NULL` even though they had `tmdb_id`
-- set, so the SKT pass treated them as new and inserted a second row. The
-- newer prehrajto auto-match then picked the older row (where the uploads
-- already lived) and skipped the new one as `no_acceptable`, leaving the
-- film visible twice in the catalogue with the sources split between the
-- two rows. See production audit: 8 pairs sharing tmdb_id, in every pair
-- the older row carries the prehrajto sources and the newer row carries
-- the SKT video — exact pattern this migration consolidates.
--
-- Plan:
--   1) Snapshot the duplicates (and the values we need to backfill onto
--      the canonical row) into a temp table. Lowest id per tmdb_id wins.
--   2) Re-point every FK on the duplicate rows to the canonical row.
--   3) Delete the duplicate film rows. Doing this BEFORE the backfill is
--      important: there is already a UNIQUE index on `films.imdb_id`, and
--      the duplicate row carries the imdb_id we want to copy onto the
--      canonical — so the canonical UPDATE would otherwise hit a unique
--      violation while both rows still exist with the same imdb_id.
--   4) Backfill canonical from the snapshot (only fills NULL fields, so
--      we never overwrite curated values).
--   5) Add the partial UNIQUE index on `films(tmdb_id) WHERE tmdb_id IS
--      NOT NULL` so future INSERT paths cannot regress.
--
-- The merge logic is generic — it works on any duplicates, not just the
-- 8 known cases — so re-running on a fresh DB is a no-op.

-- 1) Snapshot duplicates + the per-row values needed for backfill.
CREATE TEMPORARY TABLE _films_merge_map AS
SELECT
    f.id AS dup_id,
    c.keep_id,
    f.imdb_id,
    f.sktorrent_video_id,
    f.sktorrent_cdn,
    f.sktorrent_qualities,
    f.sktorrent_added_at,
    f.has_dub,
    f.has_subtitles,
    f.imdb_rating,
    f.csfd_rating,
    f.tmdb_poster_path
FROM films f
JOIN (
    SELECT tmdb_id, MIN(id) AS keep_id
    FROM films
    WHERE tmdb_id IS NOT NULL
    GROUP BY tmdb_id
    HAVING COUNT(*) > 1
) c USING (tmdb_id)
WHERE f.id <> c.keep_id;

-- 2) Re-point FK references from duplicate rows to the canonical row.
--    `video_sources`, `film_prehrajto_uploads`, `film_sledujteto_uploads`
--    have unique constraints on (provider_id, external_id) / upload_id /
--    file_id respectively, so a given upload can only live on one film at
--    a time across the whole table — no collisions are possible when
--    moving rows between two films that share a tmdb_id.
UPDATE video_sources vs
   SET film_id = m.keep_id
  FROM _films_merge_map m
 WHERE vs.film_id = m.dup_id;

UPDATE film_prehrajto_uploads fpu
   SET film_id = m.keep_id
  FROM _films_merge_map m
 WHERE fpu.film_id = m.dup_id;

UPDATE film_sledujteto_uploads fsu
   SET film_id = m.keep_id
  FROM _films_merge_map m
 WHERE fsu.film_id = m.dup_id;

-- `film_sources` is a legacy table that exists in production (~70k rows
-- referencing `films`) but was never created by an in-repo migration —
-- another piece of pre-068 schema drift, same family as the films
-- sktorrent columns 067a backfills. Guard the UPDATE with `to_regclass`
-- so a fresh CI database (no `film_sources` table) treats it as a no-op
-- while prod still re-points the rows.
DO $$
BEGIN
    IF to_regclass('public.film_sources') IS NOT NULL THEN
        UPDATE film_sources fs
           SET film_id = m.keep_id
          FROM _films_merge_map m
         WHERE fs.film_id = m.dup_id;
    END IF;
END $$;

UPDATE prehrajto_unmatched_clusters puc
   SET resolved_film_id = m.keep_id
  FROM _films_merge_map m
 WHERE puc.resolved_film_id = m.dup_id;

-- film_genres has composite PK (film_id, genre_id) so the duplicate row may
-- already share a (film_id, genre_id) row with the canonical (auto-import
-- inherits the same TMDB genres on insert). Insert the missing ones, then
-- delete the originals.
INSERT INTO film_genres (film_id, genre_id)
SELECT m.keep_id, fg.genre_id
  FROM film_genres fg
  JOIN _films_merge_map m ON m.dup_id = fg.film_id
ON CONFLICT (film_id, genre_id) DO NOTHING;

DELETE FROM film_genres fg
 USING _films_merge_map m
 WHERE fg.film_id = m.dup_id;

-- 3) Drop the duplicate film rows now that nothing references them. This
--    must precede the canonical backfill so the UNIQUE(imdb_id) index
--    isn't violated mid-transaction (the dup carries the imdb_id we're
--    about to copy onto the canonical).
DELETE FROM films f
 USING _films_merge_map m
 WHERE f.id = m.dup_id;

-- 4) Backfill the canonical row from the snapshot. COALESCE so we only
--    fill columns the canonical lacks — a manually-curated rating or a
--    different SKT id on the canonical wins over the duplicate's value.
UPDATE films keep
   SET imdb_id              = COALESCE(keep.imdb_id, m.imdb_id),
       sktorrent_video_id   = COALESCE(keep.sktorrent_video_id, m.sktorrent_video_id),
       sktorrent_cdn        = COALESCE(keep.sktorrent_cdn, m.sktorrent_cdn),
       sktorrent_qualities  = COALESCE(keep.sktorrent_qualities, m.sktorrent_qualities),
       sktorrent_added_at   = COALESCE(keep.sktorrent_added_at, m.sktorrent_added_at),
       has_dub              = keep.has_dub OR m.has_dub,
       has_subtitles        = keep.has_subtitles OR m.has_subtitles,
       imdb_rating          = COALESCE(keep.imdb_rating, m.imdb_rating),
       csfd_rating          = COALESCE(keep.csfd_rating, m.csfd_rating),
       tmdb_poster_path     = COALESCE(keep.tmdb_poster_path, m.tmdb_poster_path)
  FROM _films_merge_map m
 WHERE keep.id = m.keep_id;

DROP TABLE _films_merge_map;

-- 5) Future-proof: prevent a second row per TMDB id. Partial because some
--    legacy / manual films legitimately lack a tmdb_id and we don't want
--    to merge those (no shared identifier). Same upgrade pattern as
--    migration 050 used for `imdb_id`: create the new UNIQUE index first,
--    then drop the old non-unique one (`idx_films_tmdb_id` from migration
--    028) so query plans don't regress if the CREATE blew up.
CREATE UNIQUE INDEX IF NOT EXISTS films_tmdb_id_unique
    ON films (tmdb_id)
    WHERE tmdb_id IS NOT NULL;

DROP INDEX IF EXISTS idx_films_tmdb_id;
