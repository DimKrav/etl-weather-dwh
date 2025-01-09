# Load environment variables from .env file
include .env
export $(shell sed 's/=.*//' .env)

# Start all services in detached mode
start_services:
	docker-compose up -d

# Setup the database
setup_database::	
	docker exec -it $(POSTGRES_CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f /sql/init.sql   
	docker exec -it $(POSTGRES_CONTAINER_NAME) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f /sql/etl_locations.sql   

# Load data into the database
extract_data:	
	python src/download_raw_weather_data.py