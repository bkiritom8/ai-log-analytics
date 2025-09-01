#!/bin/bash

echo "📋 Creating Kafka topics..."

# Wait for Kafka to be ready
sleep 10

# Create topics
docker exec -it $(docker ps -q -f name=kafka) kafka-topics \
  --create --topic raw-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it $(docker ps -q -f name=kafka) kafka-topics \
  --create --topic processed-logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it $(docker ps -q -f name=kafka) kafka-topics \
  --create --topic anomalies --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it $(docker ps -q -f name=kafka) kafka-topics \
  --create --topic alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "✅ Kafka topics created successfully!"
