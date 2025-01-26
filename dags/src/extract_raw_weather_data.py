import os
import requests
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Register a custom adapter to convert Python dictionaries to PostgreSQL JSON type
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

# OpenWeather API key from environment variable
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("API_KEY not found in environment variables")

# OpenWeather API URLs
API_WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
API_AIR_POLLUTION_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

# PostgreSQL credentials
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "weather-dwh-postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather-dwh-db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def fetch_locations(conn):
    """Fetch all locations from the etl_locations table."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT city_id, city_name, country, latitude, longitude FROM etl.etl_locations;")
        return cursor.fetchall()


def fetch_from_api(url, params):
    """Fetch data from the given API endpoint with the specified parameters."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed. URL: {url}, Params: {params}, Error: {e}")
        return None


def get_weather(lat, lon):
    """Get weather data for a specific location using the OpenWeather API."""
    return fetch_from_api(API_WEATHER_URL, {"lat": lat, "lon": lon, "appid": API_KEY, "units": "metric"})


def get_air_pollution(lat, lon):
    """Get air pollution data for a specific location using the OpenWeather API."""
    return fetch_from_api(API_AIR_POLLUTION_URL, {"lat": lat, "lon": lon, "appid": API_KEY})


def save_to_raw_table(conn, data):
    """Save weather and air pollution data to the raw_weather_data table."""
    with conn.cursor() as cursor:
        insert_query = """
            INSERT INTO raw.raw_weather_data (city_id, city_name, weather_data, air_quality_data)
            VALUES (%s, %s, %s, %s);
        """
        cursor.executemany(insert_query, data)
        conn.commit()


def extract_weather_data():
    """Main function to fetch weather and air pollution data and store it in raw_weather_data."""
    try:
        # Connect to the database
        with psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        ) as conn:
            conn.autocommit = False

            # Fetch all locations
            locations = fetch_locations(conn)
            if not locations:
                logger.warning("No locations found in the database.")
                return

            raw_data = []

            # Get weather and air pollution data for each location
            for location in locations:
                city_id, city_name, country, latitude, longitude = location
                weather_response = get_weather(latitude, longitude)
                pollution_response = get_air_pollution(latitude, longitude)

                if weather_response and pollution_response:
                    raw_data.append([city_id, city_name, weather_response, pollution_response])
                    logger.info(f"Fetched data for city: {city_name} (lat: {latitude}, lon: {longitude})")
                else:
                    logger.warning(f"Skipped city: {city_name}. Invalid API response.")

            # Save raw data to the database
            if raw_data:
                save_to_raw_table(conn, raw_data)
                logger.info(f"Successfully saved {len(raw_data)} records to raw_weather_data table.")
            else:
                logger.info("No valid data to save.")

    except Exception as e:
        logger.error(f"Error during execution: {e}")


if __name__ == "__main__":
    extract_weather_data()
