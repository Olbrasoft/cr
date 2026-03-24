-- Add Wikipedia, official website, coat of arms, and flag data to regions.
-- Data sourced from Wikidata via SPARQL query on class Q38911 (Czech region).

ALTER TABLE regions ADD COLUMN wikipedia_url TEXT;
ALTER TABLE regions ADD COLUMN official_website TEXT;
ALTER TABLE regions ADD COLUMN coat_of_arms_url TEXT;
ALTER TABLE regions ADD COLUMN flag_url TEXT;
ALTER TABLE regions ADD COLUMN population INTEGER;

-- Add flag URL column to municipalities (coat of arms already exists from migration 003).
ALTER TABLE municipalities ADD COLUMN flag_url TEXT;
