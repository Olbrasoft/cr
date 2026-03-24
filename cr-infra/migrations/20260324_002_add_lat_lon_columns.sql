-- Add latitude/longitude columns to all territorial tables for map centering.
-- Values are WGS84 (EPSG:4326) centroid coordinates, populated from GeoJSON data.

ALTER TABLE regions ADD COLUMN latitude DOUBLE PRECISION;
ALTER TABLE regions ADD COLUMN longitude DOUBLE PRECISION;

ALTER TABLE districts ADD COLUMN latitude DOUBLE PRECISION;
ALTER TABLE districts ADD COLUMN longitude DOUBLE PRECISION;

ALTER TABLE orp ADD COLUMN latitude DOUBLE PRECISION;
ALTER TABLE orp ADD COLUMN longitude DOUBLE PRECISION;

ALTER TABLE municipalities ADD COLUMN latitude DOUBLE PRECISION;
ALTER TABLE municipalities ADD COLUMN longitude DOUBLE PRECISION;
