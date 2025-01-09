# ETL Pipeline for Building a Data Warehouse with Weather Conditions and Air Pollution Data for Major Cities

## Description
This project is an ETL pipeline designed for collecting, storing and analyzing weather and air pollution data. Key features include:
- Data extraction from the OpenWeatherMap API.
- Data warehouse implementation using PostgreSQL.
- Containerized deployment with Docker.
- Future integration with Apache Airflow for workflow orchestration.

## Features
- Extract weather and air pollution data for multiple cities (Moscow, New York, Tokyo, Buenos Aires and Cape Town). The dataset is limited to five cities due to the constraints of the free OpenWeatherMap subscription.
- Store raw JSON responses in a raw data staging layer.
- Store structured data in the form of facts and dimensions in the data warehouse (DWH) staging layer.
- Future development: Analyze weather trends and air quality metrics.

## Quick Start

1. Clone this repository:
   ```bash
   git clone https://github.com/DimKrav/etl-weather-dwh.git
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure your `.env` file (use `.env.example` as a reference).

5. Start services:
   ```bash
   make start_services
   ```

6. Setup the database:
   ```bash
   make setup_database
   ```

7. Run the extraction module:
   ```bash
   make extract_data
   