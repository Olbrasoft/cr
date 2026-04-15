-- Separate table for online.sktorrent.eu video catalog
-- NOT mixed with bombuj.si movies - pairing via IMDB/TMDB happens later

CREATE TABLE sktorrent_videos (
    id                 SERIAL      PRIMARY KEY,
    video_id           INTEGER     NOT NULL UNIQUE,  -- sktorrent internal ID (e.g. 59198)
    cdn_server         SMALLINT,                     -- CDN server number (e.g. 22 from online22)
    cz_name            VARCHAR(500),                 -- Czech title
    en_name            VARCHAR(500),                 -- English title (for TMDB matching)
    year               SMALLINT,                     -- Release year
    lang               VARCHAR(20),                  -- "CZ", "SK", "CZ/EN", etc.
    category           VARCHAR(50),                  -- "filmy-cz-sk", "serialy-cz-sk", etc.
    quality            VARCHAR(20),                  -- "HD" / "SD" from listing
    qualities          VARCHAR(50),                  -- "720p,480p" from detail
    duration_str       VARCHAR(20),                  -- "01:32:40" from listing
    duration_sec       REAL,                         -- 5559.88 from detail
    views              INTEGER,                      -- view count
    rating             SMALLINT,                     -- user rating %
    csfd_rating        SMALLINT,                     -- CSFD % (from title/slug)
    added_days_ago     INTEGER,                      -- days since added (at scrape time)
    description        TEXT,                         -- plot description (from detail)
    subtitles          VARCHAR(100),                 -- "cze,eng,fre" (from detail)
    thumbnail_url      VARCHAR(500),                 -- thumbnail from listing
    raw_title          TEXT,                         -- original unparsed title
    scraped_detail     BOOLEAN     DEFAULT false,    -- whether detail page was scraped
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sktorrent_videos_year ON sktorrent_videos(year);
CREATE INDEX idx_sktorrent_videos_lang ON sktorrent_videos(lang);
CREATE INDEX idx_sktorrent_videos_cz_name ON sktorrent_videos(cz_name);
