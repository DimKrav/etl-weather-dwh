-- Create schemas for each layer
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS serving;

-- Raw Layer: Table for storing raw JSON data from APIs
CREATE TABLE IF NOT EXISTS raw.raw_weather_data (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    weather_data JSONB NOT NULL,
    air_quality_data JSONB NOT NULL,
    is_processed BOOLEAN DEFAULT FALSE,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Staging Layer: Tables for cleaning and processing data
CREATE TABLE IF NOT EXISTS staging.staging_weather (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    datetime_utc TIMESTAMP NOT NULL,
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
    wind_deg INT
);

CREATE TABLE IF NOT EXISTS staging.staging_air_quality (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    datetime_utc TIMESTAMP NOT NULL,
    aqi INT NOT NULL,
    co FLOAT,
    no FLOAT,
    no2 FLOAT,
    o3 FLOAT,
    so2 FLOAT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    nh3 FLOAT
);

-- Serving Layer: Fact and dimension tables
CREATE TABLE IF NOT EXISTS serving.dim_date (
    date_id BIGINT PRIMARY KEY,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    number_of_week INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN
);


CREATE TABLE IF NOT EXISTS serving.dim_location (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL,
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    timezone_name VARCHAR(60) NOT NULL,
    CONSTRAINT unique_city_country UNIQUE (city_name, country)
);


CREATE TABLE IF NOT EXISTS serving.dim_weather_conditions (
    weather_id SERIAL PRIMARY KEY,
    weather_main VARCHAR(255) NOT NULL,
    weather_description VARCHAR(255),
    weather_icon VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS serving.dim_pollution (
    aqi INT PRIMARY KEY,
    level VARCHAR(50) NOT NULL,
    description VARCHAR(255) NOT NULL
);

INSERT INTO serving.dim_pollution (aqi, level, description) VALUES
    (1, 'Good', 'Air quality is considered satisfactory'),
    (2, 'Moderate', 'Air quality is acceptable'),
    (3, 'Unhealthy for Sensitive Groups', 'Sensitive groups may experience health effects'),
    (4, 'Unhealthy', 'Everyone may experience more serious health effects'),
    (5, 'Very Unhealthy', 'Health alert: everyone may experience serious effects'),
    (6, 'Hazardous', 'Health warnings of emergency conditions') ON CONFLICT (aqi) DO NOTHING;


INSERT INTO serving.dim_location (
    city_name, country, latitude, longitude, timezone_name
)
VALUES 
    ('Moscow', 'Russia', 55.7558, 37.6173, 'Europe/Moscow'),
    ('New York', 'USA', 40.7128, -74.0060, 'America/New_York'),
    ('Tokyo', 'Japan', 35.6895, 139.6917, 'Asia/Tokyo'),
    ('Buenos Aires', 'Argentina', -34.6037, -58.3816, 'America/Argentina/Buenos_Aires'),
    ('Cape Town', 'South Africa', -33.9249, 18.4241, 'Africa/Johannesburg')
ON CONFLICT (city_name, country) DO NOTHING;


-- insert into dim_date
INSERT INTO serving.dim_date (
    date_id, year, quarter, month, number_of_week, day, day_of_week, day_name, is_weekend
)
SELECT 
    EXTRACT(EPOCH FROM date::DATE)::BIGINT AS date_id, -- The epoch timestamp 1736899200 corresponds to 2025-01-15 00:00:00 UTC
    EXTRACT(YEAR FROM date)::INT AS year,
    EXTRACT(QUARTER FROM date)::INT AS quarter,
    EXTRACT(MONTH FROM date)::INT AS month,
    EXTRACT(WEEK FROM date)::INT AS number_of_week,
    EXTRACT(DAY FROM date)::INT AS day,
    EXTRACT(ISODOW FROM date)::INT AS day_of_week, -- ISO день недели (1 = понедельник, 7 = воскресенье)
    TO_CHAR(date, 'FMDay') AS day_name, -- Название дня недели
    CASE WHEN EXTRACT(ISODOW FROM date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend -- Определение выходных
FROM GENERATE_SERIES(
    '2025-01-01'::DATE, 
    '2030-12-31'::DATE, 
    '1 day'::INTERVAL
) AS date;



CREATE TABLE IF NOT EXISTS serving.fact_weather (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    datetime_utc TIMESTAMP NOT NULL,
    datetime_local TIMESTAMP NOT NULL,
    date_local_id BIGINT NOT NULL,
    weather_id INT,
    temperature FLOAT NOT NULL,
    feels_like FLOAT NOT NULL,
    humidity FLOAT NOT NULL,
    visibility INT NOT NULL,
    wind_speed FLOAT,
    wind_deg INT,
    FOREIGN KEY (city_id) REFERENCES serving.dim_location(city_id),
    FOREIGN KEY (date_local_id) REFERENCES serving.dim_date(date_id),
    FOREIGN KEY (weather_id) REFERENCES serving.dim_weather_conditions(weather_id),
    CONSTRAINT fw_city_date_utc_unique UNIQUE (city_id, datetime_utc)
);

CREATE TABLE IF NOT EXISTS serving.fact_air_quality (
    id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(255) NOT NULL,
    datetime_utc TIMESTAMP NOT NULL,
    datetime_local TIMESTAMP NOT NULL,
    date_local_id BIGINT NOT NULL,
    aqi INT NOT NULL,
    co FLOAT,
    no FLOAT,
    no2 FLOAT,
    o3 FLOAT,
    so2 FLOAT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    nh3 FLOAT,
    FOREIGN KEY (city_id) REFERENCES serving.dim_location(city_id),
    FOREIGN KEY (date_local_id) REFERENCES serving.dim_date(date_id),
    FOREIGN KEY (aqi) REFERENCES serving.dim_pollution(aqi),
    CONSTRAINT faq_city_date_utc_unique UNIQUE (city_id, datetime_utc)
);

-- Grant permissions to public role
GRANT ALL PRIVILEGES ON SCHEMA raw TO public;
GRANT ALL PRIVILEGES ON SCHEMA staging TO public;
GRANT ALL PRIVILEGES ON SCHEMA serving TO public;
