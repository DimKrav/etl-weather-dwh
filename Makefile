# Load environment variables from .env file
include .env
export $(shell sed 's/=.*//' .env)


# Start etl services
start_etl:
	docker-compose up -d
