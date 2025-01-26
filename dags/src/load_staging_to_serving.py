import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Load PostgreSQL credentials from environment variables
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "weather-dwh-postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "weather-dwh-db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

def get_database_connection():
    """
    Establishes a new database connection using the configuration in POSTGRES_CONFIG.
    """
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"Error establishing database connection: {e}")
        raise

def fetch_unprocessed_staging_ids(conn, staging_table, serving_table):
    """
    Fetch all ids from the staging table that are not present in the serving table based on city_id and datetime_utc.
    """
    query = f"""
        SELECT s.id
        FROM {staging_table} s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {serving_table} f
            WHERE f.city_id = s.city_id
              AND f.datetime_utc = s.datetime_utc
        );
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]

def insert_data(conn, query, data):
    """
    Generic function to execute an insert query with provided data.
    """
    if not data:
        logger.info("No data to insert for query.")
        return
    with conn.cursor() as cursor:
        cursor.execute(query, (data,))


def insert_dim_weather_conditions(conn, weather_ids):
    """
    Inserts weather conditions into dim_weather_conditions.
    """
    query = """
        INSERT INTO serving.dim_weather_conditions (weather_id, weather_main, weather_description, weather_icon)
        SELECT DISTINCT weather_id, weather_main, weather_description, weather_icon 
        FROM staging.staging_weather
        WHERE id = ANY(%s)
        ON CONFLICT (weather_id) DO NOTHING;
    """
    insert_data(conn, query, weather_ids)

def insert_fact_weather(conn, weather_ids):
    """
    Inserts weather data into fact_weather.
    """
    query = """
        INSERT INTO serving.fact_weather (city_id, city_name, datetime_utc, datetime_local, date_local_id, weather_id, temperature, feels_like, humidity, visibility, wind_speed, wind_deg)
        SELECT DISTINCT 
            sw.city_id, 
            loc.city_name,
            sw.datetime_utc, 
            sw.datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE loc.timezone_name AS datetime_local, 
            EXTRACT(EPOCH FROM (sw.datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE loc.timezone_name)::DATE)::BIGINT AS date_local_id,
            sw.weather_id, 
            sw.temperature, 
            sw.feels_like, 
            sw.humidity, 
            sw.visibility, 
            sw.wind_speed, 
            sw.wind_deg
        FROM staging.staging_weather sw
        JOIN serving.dim_location loc USING (city_id)
        WHERE sw.id = ANY(%s)
        ON CONFLICT (city_id, datetime_utc) DO NOTHING;
    """
    insert_data(conn, query, weather_ids)

def insert_fact_air_quality(conn, air_quality_ids):
    """
    Inserts air quality data into fact_air_quality.
    """
    query = """
        INSERT INTO serving.fact_air_quality (city_id, city_name, datetime_utc, datetime_local, date_local_id, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
        SELECT DISTINCT 
            saq.city_id, 
            loc.city_name,
            saq.datetime_utc, 
            saq.datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE loc.timezone_name AS datetime_local, 
            EXTRACT(EPOCH FROM (saq.datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE loc.timezone_name)::DATE)::BIGINT AS date_local_id,
            saq.aqi,
            saq.co,
            saq.no,
            saq.no2,
            saq.o3,
            saq.so2,
            saq.pm2_5,
            saq.pm10,
            saq.nh3
        FROM staging.staging_air_quality saq
        JOIN serving.dim_location loc USING (city_id)
        WHERE saq.id = ANY(%s)
        ON CONFLICT (city_id, datetime_utc) DO NOTHING;
    """
    insert_data(conn, query, air_quality_ids)

def load_staging_to_serving():
    """
    Main function to process and load data from staging to serving.
    """
    try:
        conn = get_database_connection()

        # Fetch IDs for unprocessed data
        weather_staging_ids = fetch_unprocessed_staging_ids(conn, "staging.staging_weather", "serving.fact_weather")
        air_quality_staging_ids = fetch_unprocessed_staging_ids(conn, "staging.staging_air_quality", "serving.fact_air_quality")

        if not weather_staging_ids and not air_quality_staging_ids:
            logger.info("No new data to process.")
            return

        # Process and load data
        if weather_staging_ids:
            insert_dim_weather_conditions(conn, weather_staging_ids)
            insert_fact_weather(conn, weather_staging_ids)

        if air_quality_staging_ids:
            insert_fact_air_quality(conn, air_quality_staging_ids)

        # Commit transaction
        conn.commit()
        logger.info(f"Successfully processed {len(weather_staging_ids)} weather records and {len(air_quality_staging_ids)} air quality records.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error during data load: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_staging_to_serving()