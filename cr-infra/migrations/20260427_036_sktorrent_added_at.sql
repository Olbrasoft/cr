-- Track when each title got its SK Torrent playback link so /serialy-online/
-- and /filmy-online/ can surface newly-linked existing rows in "Novinky"
-- alongside freshly added ones. Before this, a long-running Banda episode
-- that just received a SKT source never re-appeared on the landing page
-- because `created_at` was months old.
--
-- Backfill: populate from import_items.created_at where we have an
-- auto-import trail; remaining rows are left NULL so they fall back to
-- `created_at` via COALESCE in the query layer.

ALTER TABLE films
    ADD COLUMN IF NOT EXISTS sktorrent_added_at TIMESTAMPTZ;

ALTER TABLE episodes
    ADD COLUMN IF NOT EXISTS sktorrent_added_at TIMESTAMPTZ;

-- Backfill from the auto-import trail. MAX() per target because a given
-- film/episode can be touched by multiple runs (we only care about the
-- most recent one).
UPDATE films f SET sktorrent_added_at = sub.latest
FROM (
    SELECT target_film_id, MAX(created_at) AS latest
    FROM import_items
    WHERE target_film_id IS NOT NULL
    GROUP BY target_film_id
) sub
WHERE f.id = sub.target_film_id AND f.sktorrent_added_at IS NULL;

UPDATE episodes e SET sktorrent_added_at = sub.latest
FROM (
    SELECT target_episode_id, MAX(created_at) AS latest
    FROM import_items
    WHERE target_episode_id IS NOT NULL
    GROUP BY target_episode_id
) sub
WHERE e.id = sub.target_episode_id AND e.sktorrent_added_at IS NULL;
