from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
import logging
import requests
import json
from dotenv import load_dotenv

from src.transform_raw_to_staging import transform_raw_to_staging as _transform_raw_to_staging
from src.load_staging_to_serving import load_staging_to_serving as _load_staging_to_serving

# Decorate functions in @task
transform_raw_to_staging = task(_transform_raw_to_staging)
load_staging_to_serving = task(_load_staging_to_serving)


# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# OpenWeather API key from environment variable
API_KEY = os.getenv("OPENWEATHER_API_KEY")
API_WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
API_AIR_POLLUTION_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
}

def get_db_connection():
    """Establish and return a connection to the database."""
    hook = PostgresHook(postgres_conn_id='etl_weather_dwh')
    return hook.get_conn()


def fetch_from_api(session, url, params):
    """Fetch data from an API endpoint using a provided session."""
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from API. URL: {url}, Params: {params}, Error: {e}")
        return None

@task
def fetch_locations():
    """Fetch all locations from the database."""
    query = """
        SELECT city_id, city_name, country, latitude, longitude 
        FROM etl.etl_locations;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            locations = cursor.fetchall()
    return locations


@task
def fetch_raw_data(locations):
    """Fetch weather and air quality data for all locations."""
    if not locations:
        logger.error("No locations found. Exiting task.")
        return []

    session = requests.Session()
    session.params = {'appid': API_KEY}
    collected_data = []

    for city_id, city_name, country, lat, lon in locations:
        weather_data = fetch_from_api(session, API_WEATHER_URL, {"lat": lat, "lon": lon, "units": "metric"})
        air_quality_data = fetch_from_api(session, API_AIR_POLLUTION_URL, {"lat": lat, "lon": lon})
        if weather_data and air_quality_data:
            collected_data.append((city_id, city_name, weather_data, air_quality_data))
            logger.info(f"Data fetched for {city_name}")
        else:
            logger.warning(f"Skipping {city_name} due to missing data.")
    session.close()
    return collected_data


@task
def insert_to_raw_table(collected_data):
    """Insert fetched data into the database."""
    if not collected_data:
        logger.error("No data to insert. Exiting task.")
        return

    query = """
        INSERT INTO raw.raw_weather_data (city_id, city_name, weather_data, air_quality_data)
        VALUES (%s, %s, %s, %s);
    """
    formatted_data = [
        (
            city_id,
            city_name,
            json.dumps(weather_data),  # serialization of weather_data to a string
            json.dumps(air_quality_data),  # serialization of air_quality_data to a string
        )
        for city_id, city_name, weather_data, air_quality_data in collected_data
    ]

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, formatted_data)
        conn.commit()
    logger.info("All data inserted successfully.")


with DAG(
    'etl_weather_dag',
    default_args=default_args,
    schedule_interval='15 * * * *',
    catchup=False
) as dag:
    with TaskGroup("extract", tooltip="Extract data tasks") as extract:
        locations = fetch_locations()
        raw_data = fetch_raw_data(locations)
        inserted = insert_to_raw_table(raw_data)

        # Set the order of execution for the tasks in the "extract" TaskGroup
        locations >> raw_data >> inserted
    
    # The tasks after the "extract" TaskGroup
    transformed = transform_raw_to_staging()
    loaded = load_staging_to_serving()

    # Set the order of execution for the tasks after the "extract" TaskGroup
    extract >> transformed >> loaded
