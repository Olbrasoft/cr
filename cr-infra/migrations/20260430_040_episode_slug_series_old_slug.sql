-- Episode slug column for SEO-friendly URLs (#459)
ALTER TABLE episodes ADD COLUMN IF NOT EXISTS slug VARCHAR;
CREATE INDEX IF NOT EXISTS idx_episodes_slug ON episodes(slug) WHERE slug IS NOT NULL;

-- Series old_slug for 301 redirect mapping (#458)
ALTER TABLE series ADD COLUMN IF NOT EXISTS old_slug VARCHAR;
