UPDATE raw.raw_weather_data 
SET is_processed = FALSE;

-- Удаление всех таблиц и ограничений из STAGING слоя
DROP TABLE IF EXISTS staging.staging_weather_data CASCADE;
DROP TABLE IF EXISTS staging.staging_air_pollution_data CASCADE;

-- Удаление всех таблиц и ограничений из CORE слоя
DROP TABLE IF EXISTS serving.fact_air_pollution CASCADE;
DROP TABLE IF EXISTS serving.dim_time CASCADE;
DROP TABLE IF EXISTS serving.dim_location CASCADE;
DROP TABLE IF EXISTS serving.dim_pollutants CASCADE;
DROP TABLE IF EXISTS serving.dim_air_quality_index CASCADE;
DROP TABLE IF EXISTS serving.dim_air_quality_ranges CASCADE;

-- Удаление схем
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS serving CASCADE;
