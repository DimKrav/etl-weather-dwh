-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;

-- Raw Layer: Table for storing raw JSON data from APIs
CREATE TABLE IF NOT EXISTS raw.raw_weather_data (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    weather_data JSONB NOT NULL,
    air_quality_data JSONB NOT NULL,
    is_processed BOOLEAN DEFAULT FALSE,
    extract_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Staging Layer: Tables for cleaning and processing data
CREATE TABLE IF NOT EXISTS staging.staging_weather (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    weather_id INT NOT NULL,
    weather_main VARCHAR(255),
    weather_description VARCHAR(255),
    weather_icon VARCHAR(50),
    temperature FLOAT NOT NULL,
    feels_like FLOAT NOT NULL,
    pressure INT NOT NULL,
    humidity INT NOT NULL,
    visibility INT NOT NULL,
    wind_speed FLOAT,
    wind_deg INT,
    observation_datetime TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.staging_air_quality (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    aqi INT NOT NULL,
    co FLOAT,
    no FLOAT,
    no2 FLOAT,
    o3 FLOAT,
    so2 FLOAT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    nh3 FLOAT,
    observation_datetime TIMESTAMP NOT NULL
);

-- Core Layer: Fact and dimension tables
CREATE TABLE IF NOT EXISTS core.dim_location (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.dim_time (
    id SERIAL PRIMARY KEY,
    observation_time TIMESTAMP NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    hour INT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.dim_weather_conditions (
    id SERIAL PRIMARY KEY,
    condition_name VARCHAR(255) NOT NULL,
    description VARCHAR(255),
    icon VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS core.dim_pollution (
    aqi INT PRIMARY KEY,
    level TEXT NOT NULL,
    description TEXT NOT NULL
);

INSERT INTO core.dim_pollution (aqi, level, description) VALUES
    (1, 'Good', 'Air quality is considered satisfactory'),
    (2, 'Moderate', 'Air quality is acceptable'),
    (3, 'Unhealthy for Sensitive Groups', 'Sensitive groups may experience health effects'),
    (4, 'Unhealthy', 'Everyone may experience more serious health effects'),
    (5, 'Very Unhealthy', 'Health alert: everyone may experience serious effects'),
    (6, 'Hazardous', 'Health warnings of emergency conditions') ON CONFLICT (aqi) DO NOTHING;


CREATE TABLE IF NOT EXISTS core.fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    time_id INT NOT NULL,
    temperature FLOAT NOT NULL,
    humidity FLOAT NOT NULL,
    wind_speed FLOAT,
    weather_condition_id INT,
    FOREIGN KEY (city_id) REFERENCES core.dim_location(id),
    FOREIGN KEY (time_id) REFERENCES core.dim_time(id),
    FOREIGN KEY (weather_condition_id) REFERENCES core.dim_weather_conditions(id)
);

CREATE TABLE IF NOT EXISTS core.fact_air_quality (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    time_id INT NOT NULL,
    aqi INT NOT NULL,
    pm25 FLOAT,
    pm10 FLOAT,
    co FLOAT,
    no2 FLOAT,
    so2 FLOAT,
    o3 FLOAT,
    FOREIGN KEY (city_id) REFERENCES core.dim_location(id),
    FOREIGN KEY (time_id) REFERENCES core.dim_time(id),
    FOREIGN KEY (aqi) REFERENCES core.dim_pollution(aqi)
);

-- Grant permissions to public role
GRANT ALL PRIVILEGES ON SCHEMA raw TO public;
GRANT ALL PRIVILEGES ON SCHEMA staging TO public;
GRANT ALL PRIVILEGES ON SCHEMA core TO public;
