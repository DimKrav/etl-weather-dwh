# Load environment variables from .env file
include .env
export $(shell sed 's/=.*//' .env)

# Start all services in detached mode
start_services:
	docker-compose up -d

# Setup the database
setup_database:	
	docker exec -it $(POSTGRES_CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f /sql/init.sql   
	docker exec -it $(POSTGRES_CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f /sql/etl_locations.sql   

# Drop the database
drop_database:
	docker exec -it $(POSTGRES_CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f /sql/drop_layers.sql


# Reload the database
reload_database: 
	make drop_database 
	make setup_database

# Load data into the database
extract_data:	
	python src/extract_raw_weather_data.py

transform_data:
	python src/transform_raw_to_staging.py

load_data:
	python src/load_staging_to_serving.py

# Run the ETL pipeline
run_etl:
	make extract_data
	make transform_data
	make load_data