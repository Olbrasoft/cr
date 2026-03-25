-- Raw Wikipedia article texts for LLM processing.
-- This is a staging table — raw text is processed by LLM into
-- municipalities.description (original summary).

CREATE TABLE wikipedia_raw (
    id              SERIAL PRIMARY KEY,
    municipality_code TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    extract         TEXT NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_wikipedia_raw_code ON wikipedia_raw(municipality_code);

-- Add description column to municipalities for LLM-generated summaries.
ALTER TABLE municipalities ADD COLUMN description TEXT;
