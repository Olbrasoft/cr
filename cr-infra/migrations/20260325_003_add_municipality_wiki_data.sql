-- Add Wikipedia, official website, coat of arms, and population data to municipalities.
-- Data sourced from Wikidata via SPARQL query on property P7606 (Czech municipality ID).

ALTER TABLE municipalities ADD COLUMN wikipedia_url TEXT;
ALTER TABLE municipalities ADD COLUMN official_website TEXT;
ALTER TABLE municipalities ADD COLUMN coat_of_arms_url TEXT;
ALTER TABLE municipalities ADD COLUMN population INTEGER;
ALTER TABLE municipalities ADD COLUMN elevation DOUBLE PRECISION;
