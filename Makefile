.PHONY: help install kafka-up spark-up down clean test lint

help:
	@echo "AI Log Analytics Platform Commands:"
	@echo "  install      - Install all dependencies"
	@echo "  kafka-up     - Start Kafka cluster"
	@echo "  spark-up     - Start Spark cluster"
	@echo "  services-up  - Start all backend services"
	@echo "  dashboard    - Start React dashboard"
	@echo "  down         - Stop all services"
	@echo "  test         - Run all tests"
	@echo "  lint         - Run code formatting"
	@echo "  clean        - Clean up containers and data"

install:
	pip install -r requirements.txt
	cd react-dashboard && npm install

kafka-up:
	docker-compose -f kafka/docker-compose-kafka.yml up -d
	./scripts/kafka/create-topics.sh

spark-up:
	docker-compose -f spark/docker-compose-spark.yml up -d

services-up:
	docker-compose up -d

dashboard:
	cd react-dashboard && npm start

down:
	docker-compose down
	docker-compose -f kafka/docker-compose-kafka.yml down
	docker-compose -f spark/docker-compose-spark.yml down

test:
	pytest tests/
	cd react-dashboard && npm test

lint:
	black backend/
	isort backend/
	flake8 backend/

clean:
	docker system prune -f
	docker volume prune -f
