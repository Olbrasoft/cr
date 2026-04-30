-- prehrajto_search_hints — stable resolve-at-play-time hints for prehraj.to (issue #632, parent #631).
--
-- Replaces persisted `video_sources(provider_id=prehrajto)` rows with stable
-- search queries. prehraj.to rotates `external_id` on every re-upload, so any
-- cached ID goes stale within days/weeks (today's evidence: film "Spasitel"
-- has 18 cached `video_sources` rows, all 404 "Soubor nenalezen", same files
-- live under fresh IDs). Mirroring the working sktorrent_cdn pattern, the
-- resolver will re-search prehraj.to on every play attempt and pick the best
-- live candidate by variant.
--
-- Design notes:
--
--   1. `(film_id, episode_id, tv_episode_id)` is XOR — exactly one is NOT
--      NULL. Matches the polymorphism `video_sources` (058) uses for the
--      same three parents (films, series episodes, tv_porady episodes).
--      tv_porady reads prehraj.to via `video_sources.tv_episode_id` today
--      (`cr-web/src/handlers/tv_porady.rs`), so when the resolver flips,
--      tv_porady needs hints too. Cheaper to add the column now than to
--      do a second migration.
--
--   2. `variant` is a small closed enum — CZ_DUB / CZ_SUB / RES_2160P /
--      RES_1080P. Encoded as VARCHAR + CHECK to match `video_sources.lang_class`
--      style. Variants without a hint don't render a play button on the UI;
--      no "Zdroj nedostupný" placeholder needed.
--
--   3. `last_resolved_id` is *informational only* — never authoritative. The
--      resolver MAY use it as a hot-cache key but MUST be prepared for it to
--      404 and re-search. Storing it lets ops eyeball recent activity
--      (`last_resolved_at`) without reading log files.
--
--   4. `title_filter_regex` is optional. Most variants are well-served by
--      hard-coded regexes in the resolver (CZ_DUB matches "cz dab|česk|cestin"
--      etc.), but per-film overrides exist for edge cases (e.g., a Czech
--      title that collides with another film of the same name from a different
--      year — "Spasitel" 1981 vs 2026).
--
--   5. `search_query` is `TEXT` rather than `VARCHAR(255)` because the episode
--      backfill builds `series.title || ' sNNeNN'` and `series.title` is
--      itself `VARCHAR(255)` — the concatenation can exceed 255 bytes for
--      long series titles. PostgreSQL `TEXT` has no length penalty vs.
--      `VARCHAR` and removes the failure mode entirely.
--
--   6. Uniqueness is enforced via three PARTIAL unique indexes (one per
--      parent column), matching the partial-index pattern from
--      `video_sources` (058). These also serve point lookups by parent —
--      a separate non-unique index on `film_id` etc. would be redundant
--      because the unique index already has the parent column as its
--      leading key.
--
--   7. Backfill at the end of this migration: for each film/episode/tv_episode
--      with any prehrajto row in `video_sources`, insert default hints
--      `[CZ_DUB, CZ_SUB]`. `search_query` = `title` (films) or
--      `series.title + " sNNeNN"` (episodes) or `tv_show.title + " sNNeNN"`
--      (tv_episodes). Year is intentionally omitted from the search query —
--      prehraj.to filename matching is messy enough without it, and our
--      `title_filter_regex` covers disambiguation when needed.

CREATE TABLE IF NOT EXISTS prehrajto_search_hints (
    id                  SERIAL      PRIMARY KEY,
    film_id             INTEGER     REFERENCES films(id)        ON DELETE CASCADE,
    episode_id          INTEGER     REFERENCES episodes(id)     ON DELETE CASCADE,
    tv_episode_id       INTEGER     REFERENCES tv_episodes(id)  ON DELETE CASCADE,
    search_query        TEXT        NOT NULL,
    variant             VARCHAR(16) NOT NULL,
    title_filter_regex  TEXT,
    last_resolved_id    VARCHAR(64),
    last_resolved_at    TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT prsh_owner_xor CHECK (
        (film_id IS NOT NULL)::int
      + (episode_id IS NOT NULL)::int
      + (tv_episode_id IS NOT NULL)::int = 1
    ),
    CONSTRAINT prsh_variant_known CHECK (
        variant IN ('CZ_DUB', 'CZ_SUB', 'RES_2160P', 'RES_1080P')
    ),
    CONSTRAINT prsh_search_query_nonempty CHECK (length(trim(search_query)) > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS prsh_film_variant_uniq
    ON prehrajto_search_hints(film_id, variant)
    WHERE film_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS prsh_episode_variant_uniq
    ON prehrajto_search_hints(episode_id, variant)
    WHERE episode_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS prsh_tv_episode_variant_uniq
    ON prehrajto_search_hints(tv_episode_id, variant)
    WHERE tv_episode_id IS NOT NULL;

-- Touch updated_at on UPDATE (mirrors video_sources trigger from 058/059).
CREATE OR REPLACE FUNCTION prehrajto_search_hints_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS prsh_touch_updated_at ON prehrajto_search_hints;
CREATE TRIGGER prsh_touch_updated_at
    BEFORE UPDATE ON prehrajto_search_hints
    FOR EACH ROW
    EXECUTE FUNCTION prehrajto_search_hints_touch_updated_at();

-- =============================================================================
-- Backfill from existing video_sources(provider_id=prehrajto).
--
-- For each film/episode/tv_episode with any prehrajto row, insert two hints
-- (CZ_DUB and CZ_SUB). The ON CONFLICT DO NOTHING is defensive — re-running
-- this migration must be idempotent.
--
-- Episodes and tv_episodes include a season/episode tag in `search_query`
-- (e.g., "Jistina s01e03"). Including it gives prehraj.to's search a much
-- better chance of returning episode-specific matches over season-pack
-- uploads. The resolver may strip the tag in fallback if no episode-specific
-- match is found.
-- =============================================================================

INSERT INTO prehrajto_search_hints (film_id, search_query, variant)
SELECT DISTINCT f.id, f.title, v.variant
FROM films f
JOIN video_sources vs ON vs.film_id = f.id
JOIN video_providers vp ON vp.id = vs.provider_id AND vp.slug = 'prehrajto'
CROSS JOIN (VALUES ('CZ_DUB'), ('CZ_SUB')) AS v(variant)
WHERE length(trim(f.title)) > 0
ON CONFLICT (film_id, variant) WHERE film_id IS NOT NULL DO NOTHING;

INSERT INTO prehrajto_search_hints (episode_id, search_query, variant)
SELECT DISTINCT
    e.id,
    s.title || ' s' || lpad(e.season::text, 2, '0') || 'e' || lpad(e.episode::text, 2, '0'),
    v.variant
FROM episodes e
JOIN series s ON s.id = e.series_id
JOIN video_sources vs ON vs.episode_id = e.id
JOIN video_providers vp ON vp.id = vs.provider_id AND vp.slug = 'prehrajto'
CROSS JOIN (VALUES ('CZ_DUB'), ('CZ_SUB')) AS v(variant)
WHERE length(trim(s.title)) > 0
ON CONFLICT (episode_id, variant) WHERE episode_id IS NOT NULL DO NOTHING;

INSERT INTO prehrajto_search_hints (tv_episode_id, search_query, variant)
SELECT DISTINCT
    te.id,
    ts.title || ' s' || lpad(te.season::text, 2, '0') || 'e' || lpad(te.episode::text, 2, '0'),
    v.variant
FROM tv_episodes te
JOIN tv_shows ts ON ts.id = te.tv_show_id
JOIN video_sources vs ON vs.tv_episode_id = te.id
JOIN video_providers vp ON vp.id = vs.provider_id AND vp.slug = 'prehrajto'
CROSS JOIN (VALUES ('CZ_DUB'), ('CZ_SUB')) AS v(variant)
WHERE length(trim(ts.title)) > 0
ON CONFLICT (tv_episode_id, variant) WHERE tv_episode_id IS NOT NULL DO NOTHING;
