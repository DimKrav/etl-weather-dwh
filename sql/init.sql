-- Create schemas for organizing the database layers
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dwh;

-- Create table for raw JSON data storage
CREATE TABLE IF NOT EXISTS raw.raw_weather_data (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name TEXT NOT NULL,
    weather_response JSONB NOT NULL,
    polution_response JSONB NOT NULL,
    is_processed BOOLEAN DEFAULT FALSE
);



-- Create dimensions in the DWH layer
CREATE TABLE IF NOT EXISTS dwh.dim_location (
    city_id SERIAL PRIMARY KEY,
    city_name TEXT NOT NULL,
    country TEXT NOT NULL,
    latitude NUMERIC,
    longitude NUMERIC
);

INSERT INTO dwh.dim_location (city_id, city_name, country, latitude, longitude)
SELECT city_id, city_name, country, latitude, longitude FROM etl.etl_locations;

CREATE TABLE IF NOT EXISTS dwh.dim_pollution (
    id SERIAL PRIMARY KEY,
    aqi INT NOT NULL UNIQUE,
    level TEXT NOT NULL,
    description TEXT NOT NULL
);

-- Create fact table for weather data
CREATE TABLE IF NOT EXISTS dwh.fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL REFERENCES dwh.dim_location(city_id),
    air_quality_index INT NOT NULL REFERENCES dwh.dim_pollution(id),
    temperature NUMERIC,
    humidity NUMERIC,
    wind_speed NUMERIC,
    weather_main TEXT,
    weather_description TEXT,
    observation_time TIMESTAMP NOT NULL
);


-- Insert initial data into dim_pollution (example data)
INSERT INTO dwh.dim_pollution (aqi, level, description) VALUES
    (1, 'Good', 'Air quality is considered satisfactory'),
    (2, 'Moderate', 'Air quality is acceptable'),
    (3, 'Unhealthy for Sensitive Groups', 'Sensitive groups may experience health effects'),
    (4, 'Unhealthy', 'Everyone may experience more serious health effects'),
    (5, 'Very Unhealthy', 'Health alert: everyone may experience serious effects'),
    (6, 'Hazardous', 'Health warnings of emergency conditions') ON CONFLICT (aqi) DO NOTHING;

-- Grant privileges (optional, for multi-user environments)
GRANT ALL PRIVILEGES ON SCHEMA raw TO public;
GRANT ALL PRIVILEGES ON SCHEMA dwh TO public;
