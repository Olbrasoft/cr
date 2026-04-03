-- Add description column to ORP table for AI-generated city descriptions
ALTER TABLE orp ADD COLUMN IF NOT EXISTS description TEXT;
