-- Drop tables if exist
DROP TABLE IF EXISTS climate_7days CASCADE;
DROP TABLE IF EXISTS climate_daily CASCADE;
DROP TABLE IF EXISTS grid_100m CASCADE;
DROP TABLE IF EXISTS ppi_points CASCADE;

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- PPI Points table (points d'intérêt)
CREATE TABLE ppi_points (
    geohash TEXT PRIMARY KEY,
    ppi_nom TEXT,
    gov_name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 100m grid table with geohash as primary key
CREATE TABLE grid_100m (
    geohash TEXT PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    geom GEOMETRY(Polygon, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Daily climate data
CREATE TABLE climate_daily (
    id SERIAL,
    geohash TEXT REFERENCES grid_100m(geohash) ON DELETE CASCADE,
    date DATE NOT NULL,
    tmin DOUBLE PRECISION,
    tmax DOUBLE PRECISION,
    radiation DOUBLE PRECISION,
    rain DOUBLE PRECISION,
    rh DOUBLE PRECISION,
    wind DOUBLE PRECISION,
    et0 DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (geohash, date)
);

-- Rolling 7-day averages
CREATE TABLE climate_7days (
    id SERIAL,
    geohash TEXT REFERENCES grid_100m(geohash) ON DELETE CASCADE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    avg_tmin DOUBLE PRECISION,
    avg_tmax DOUBLE PRECISION,
    avg_radiation DOUBLE PRECISION,
    total_rain DOUBLE PRECISION,
    avg_rh DOUBLE PRECISION,
    avg_wind DOUBLE PRECISION,
    avg_et0 DOUBLE PRECISION,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (geohash, end_date)
);

-- Create indexes
CREATE INDEX idx_climate_daily_date ON climate_daily(date);
CREATE INDEX idx_climate_7days_end_date ON climate_7days(end_date);
CREATE INDEX idx_grid_100m_geom ON grid_100m USING GIST(geom);

-- Create view for latest data
CREATE OR REPLACE VIEW latest_climate AS
SELECT DISTINCT ON (geohash) 
    g.geohash,
    g.lat,
    g.lon,
    d.date,
    d.tmin,
    d.tmax,
    d.rain,
    d.rh,
    d.wind,
    d.et0
FROM grid_100m g
LEFT JOIN climate_daily d ON g.geohash = d.geohash
ORDER BY g.geohash, d.date DESC;

-- Create view for weekly summaries
CREATE OR REPLACE VIEW weekly_summary AS
SELECT 
    w.*,
    g.lat,
    g.lon
FROM climate_7days w
JOIN grid_100m g ON w.geohash = g.geohash
ORDER BY w.end_date DESC, w.geohash;