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
--   1. `(film_id, episode_id)` is XOR — one row points to either a film or a
--      series episode, never both, never neither. Matches the polymorphism
--      already established by `video_sources` (058) but is binary here
--      because tv_episodes don't get prehrajto sources today.
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
--   5. Uniqueness is enforced via two PARTIAL unique indexes (one per parent
--      column), matching the partial-index pattern from `video_sources`
--      (058) — keeps the constraint clean across the XOR without needing
--      `UNIQUE NULLS NOT DISTINCT` (PG 15+ only).
--
--   6. Backfill at the end of this migration: for each film that has any
--      prehrajto row in `video_sources`, derive default hints
--      `[CZ_DUB, CZ_SUB]` (a film either has Czech audio or Czech subs in
--      our domain — both variants get a hint, the resolver decides at
--      request time which one returns matches). Same for episodes.
--      `search_query` = `title` (films) or `series.title + " s<NN>e<NN>"`
--      (episodes). Year is intentionally omitted from the search query —
--      prehraj.to filename matching is messy enough without it, and our
--      title_filter_regex covers disambiguation when needed.

CREATE TABLE IF NOT EXISTS prehrajto_search_hints (
    id                  SERIAL      PRIMARY KEY,
    film_id             INTEGER     REFERENCES films(id)    ON DELETE CASCADE,
    episode_id          INTEGER     REFERENCES episodes(id) ON DELETE CASCADE,
    search_query        VARCHAR(255) NOT NULL,
    variant             VARCHAR(16)  NOT NULL,
    title_filter_regex  TEXT,
    last_resolved_id    VARCHAR(64),
    last_resolved_at    TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT prsh_owner_xor CHECK (
        (film_id IS NOT NULL)::int + (episode_id IS NOT NULL)::int = 1
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

CREATE INDEX IF NOT EXISTS prsh_film_lookup
    ON prehrajto_search_hints(film_id)
    WHERE film_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS prsh_episode_lookup
    ON prehrajto_search_hints(episode_id)
    WHERE episode_id IS NOT NULL;

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
-- For each film with any prehrajto row, insert two hints (CZ_DUB and CZ_SUB)
-- with the film's title as search_query. The ON CONFLICT DO NOTHING is
-- defensive — re-running this migration must be idempotent.
--
-- Episodes follow the same pattern, with search_query built from the parent
-- series title plus a season/episode tag (e.g., "Jistina s01e03"). Stripping
-- the tag in the resolver is acceptable; including it in the query gives
-- prehraj.to's search a much better chance of returning episode-specific
-- matches over season-pack uploads.
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
