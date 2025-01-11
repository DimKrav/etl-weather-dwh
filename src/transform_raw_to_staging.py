import os
from dotenv import load_dotenv
from datetime import datetime, UTC

import psycopg2
from psycopg2.extras import execute_values

import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Load PostgreSQL credentials from environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather-dwh-db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def fetch_unprocessed_records(conn):
    """Fetch all not already processed records from the raw_weather_data table"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM raw.raw_weather_data WHERE NOT is_processed;")
        not_processed_raw_records = cursor.fetchall()
    return not_processed_raw_records


def convert_unix_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp, UTC)


def transform_weather(record):
    """
    Transform weather data from raw records into the staging format
    """
    record_id, city_id, city_name, weather_data, *_ = record

    try:
        return (
            int(city_id),
            city_name,
            int(weather_data['weather'][0]['id']),
            weather_data['weather'][0]['main'],
            weather_data['weather'][0]['description'],
            weather_data['weather'][0]['icon'],
            float(weather_data['main']['temp']),
            float(weather_data['main']['feels_like']),
            int(weather_data['main']['pressure']),
            int(weather_data['main']['humidity']),
            int(weather_data['visibility']),
            float(weather_data['wind']['speed']),
            int(weather_data['wind']['deg']),
            convert_unix_to_datetime(weather_data.get('dt'))
        )
    except KeyError as e:
        logger.error(f"Missing key in weather data: {e}")
        raise


def transform_pollution(record):
    """
    Transform air pollution data from raw records into the staging format
    """
    record_id, city_id, city_name, _, air_quality_data, *_ = record

    try:
        components = air_quality_data['list'][0]['components']
        return (
            int(city_id),
            city_name,
            int(air_quality_data['list'][0]['main']['aqi']),
            float(components['co']),
            float(components['no']),
            float(components['no2']),
            float(components['o3']),
            float(components['so2']),
            float(components['pm2_5']),
            float(components['pm10']),
            float(components['nh3']),
            convert_unix_to_datetime(air_quality_data['list'][0]['dt'])
        )
    except KeyError as e:
        logger.error(f"Missing key in air quality data: {e}")
        raise


def insert_weather_staging(conn, weather_staging_data):
    """Save weather data to the staging.staging_weather table"""
    with conn.cursor() as cursor:
        insert_query = """INSERT INTO staging.staging_weather (city_id, city_name, weather_id, weather_main, 
        weather_description, weather_icon, temperature, feels_like, pressure, humidity, visibility, wind_speed, 
        wind_deg, observation_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cursor.executemany(insert_query, weather_staging_data)


def insert_air_quality_staging(conn, pollution_staging_data):
    with conn.cursor() as cursor:
        insert_query = """INSERT INTO staging.staging_air_quality (city_id, city_name, aqi, co, no, no2, o3, so2, 
        pm2_5, pm10, nh3, observation_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        cursor.executemany(insert_query, pollution_staging_data)


def update_processed_flag(conn, ids):
    """
    Update the is_processed flag for processed records in raw layer.
    """
    with conn.cursor() as cursor:
        execute_values(cursor, """
            UPDATE raw.raw_weather_data
            SET is_processed = TRUE
            WHERE id IN %s
        """, (ids,))


def transform_raw_to_staging():
    """
    Main function to transform data from raw to staging.
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

        conn.autocommit = False

        # Step 1: Fetch raw records to process
        raw_records_to_process = fetch_unprocessed_records(conn)
        if not raw_records_to_process:
            logger.info("No new data to process.")
            return

        logger.info(f"Processing {len(raw_records_to_process)} records...")

        # Step 2: Transform records into staging data
        weather_staging_data = []
        pollution_staging_data = []
        raw_records_ids = []

        for record in raw_records_to_process:
            raw_records_ids.append(record[0])
            weather_staging_data.append(transform_weather(record))
            pollution_staging_data.append(transform_pollution(record))

        # Step 3: Load transformed data into staging tables
        insert_weather_staging(conn, weather_staging_data)
        insert_air_quality_staging(conn, pollution_staging_data)

        # Step 4 Update is_processed flag
        update_processed_flag(conn, raw_records_ids)

        # Commit the transaction
        conn.commit()
        logger.info(f"Successfully processed {len(raw_records_to_process)} records.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.info(f"Error during transformation: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    transform_raw_to_staging()
