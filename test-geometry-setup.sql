-- Test SQL for GEOMETRY support validation
-- Run this against a MySQL/PlanetScale database to create test spatial data

CREATE DATABASE IF NOT EXISTS geometry_test;
USE geometry_test;

-- Create table with various GEOMETRY types
CREATE TABLE spatial_data (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100),
  point_location POINT,
  polygon_boundary POLYGON,
  line_path LINESTRING,
  multi_points MULTIPOINT,
  geometry_collection GEOMETRY,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data with various spatial geometries
INSERT INTO spatial_data (name, point_location, polygon_boundary, line_path, multi_points, geometry_collection) VALUES
('San Francisco Office', 
 ST_GeomFromText('POINT(-122.4194 37.7749)'),
 ST_GeomFromText('POLYGON((-122.42 37.77, -122.41 37.77, -122.41 37.78, -122.42 37.78, -122.42 37.77))'),
 ST_GeomFromText('LINESTRING(-122.42 37.77, -122.41 37.775, -122.405 37.78)'),
 ST_GeomFromText('MULTIPOINT(-122.41 37.77, -122.42 37.78, -122.415 37.775)'),
 ST_GeomFromText('GEOMETRYCOLLECTION(POINT(-122.4194 37.7749), LINESTRING(-122.42 37.77, -122.41 37.78))')
),
('New York Office',
 ST_GeomFromText('POINT(-74.0060 40.7128)'),
 ST_GeomFromText('POLYGON((-74.01 40.71, -74.00 40.71, -74.00 40.72, -74.01 40.72, -74.01 40.71))'),
 ST_GeomFromText('LINESTRING(-74.01 40.71, -74.005 40.715, -74.00 40.72)'),
 ST_GeomFromText('MULTIPOINT(-74.00 40.71, -74.01 40.72, -74.005 40.715)'),
 ST_GeomFromText('POINT(-74.0060 40.7128)')
);

-- Query to verify data was inserted correctly
SELECT 
  id, 
  name,
  ST_AsText(point_location) as point_wkt,
  ST_AsText(polygon_boundary) as polygon_wkt,
  ST_AsText(line_path) as line_wkt,
  ST_AsText(multi_points) as multipoint_wkt,
  ST_AsText(geometry_collection) as geometry_collection_wkt
FROM spatial_data;

-- Update some records to generate change events
UPDATE spatial_data 
SET point_location = ST_GeomFromText('POINT(-122.4194 37.7750)') 
WHERE name = 'San Francisco Office';

-- Insert additional test record
INSERT INTO spatial_data (name, point_location, polygon_boundary) VALUES
('Customer Location',
 ST_GeomFromText('POINT(-118.2437 34.0522)'),  -- Los Angeles
 ST_GeomFromText('POLYGON((-118.25 34.05, -118.24 34.05, -118.24 34.06, -118.25 34.06, -118.25 34.05))')
);