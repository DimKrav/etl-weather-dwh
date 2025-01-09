import pandas as pd
import requests 
import json
import os
from dotenv import load_dotenv
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

# Register a custom adapter to convert Python dictionaries to PostgreSQL JSON type
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

# OpenWeather API key from environment variable
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("API_KEY не найден в переменных окружения")

# OpenWeather API URLs
API_WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
API_AIR_POLLUTION_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

# Database connection details from environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "weather-dwh-db")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def fetch_locations():
    """Fetch all locations from the etl_locations table."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute("SELECT city_id, city_name, country, latitude, longitude FROM etl.etl_locations;")
    locations = cursor.fetchall()
    conn.close()
    return locations


def get_weather(lat, lon):
    """Get weather data for a specific location using the OpenWeather API"""
    params = {"lat": lat, "lon": lon, "appid": API_KEY, "units": "metric"}
    response = requests.get(API_WEATHER_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": response.status_code, "message": response.reason}


def get_air_polution(lat, lon):
    """Get air polution data for a specific location using the OpenWeather API."""
    params = {"lat": lat, "lon": lon, "appid": API_KEY}
    response = requests.get(API_AIR_POLLUTION_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": response.status_code, "message": response.reason}


def save_to_raw_data(data):
    """Save weather data to the raw_weather_data table"""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    with conn.cursor() as cursor:
        insert_query = """
            INSERT INTO raw.raw_weather_data (city_id, city_name, weather_response, polution_response)
            VALUES (%s, %s, %s, %s);
        """
        cursor.executemany(insert_query, data)
        conn.commit()


def main():
    """Main function to fetch weather data and store it in raw_weather_data."""
    # Fetch all locations
    locations = fetch_locations()

    # Container for raw weather data
    raw_data = []
    # Get weather and airpolution data for each location
    for location in locations:
        city_id, city_name, country, latitude, longitude = location
        weather_response = get_weather(latitude, longitude)
        polution_response = get_air_polution(latitude, longitude)
        raw_data.append([city_id, city_name, weather_response, polution_response])

    # Save raw data to the raw_weather_data table 
    save_to_raw_data(raw_data)


if __name__ == "__main__":
    main()