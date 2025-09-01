#!/bin/bash
# scripts/kafka/create-topics.sh (FIXED VERSION)

set -e  # Exit on any error

echo "📋 Creating Kafka topics..."

# Configuration
KAFKA_CONTAINER_PREFIX="ai-log-kafka"
BOOTSTRAP_SERVER="localhost:9092"
MAX_RETRIES=30
RETRY_DELAY=2

# Function to wait for Kafka to be ready
wait_for_kafka() {
    echo "⏳ Waiting for Kafka to be ready..."
    
    for i in $(seq 1 $MAX_RETRIES); do
        # Check if container is running
        if ! docker ps --format "table {{.Names}}" | grep -q "${KAFKA_CONTAINER_PREFIX}"; then
            echo "❌ Kafka container not found. Expected container name containing: ${KAFKA_CONTAINER_PREFIX}"
            echo "📋 Available containers:"
            docker ps --format "table {{.Names}}\t{{.Status}}"
            exit 1
        fi
        
        # Get the actual Kafka container name
        KAFKA_CONTAINER=$(docker ps --format "{{.Names}}" | grep "${KAFKA_CONTAINER_PREFIX}" | head -1)
        
        if [ -z "$KAFKA_CONTAINER" ]; then
            echo "❌ No running Kafka container found"
            exit 1
        fi
        
        echo "🔍 Attempt $i/$MAX_RETRIES: Testing Kafka readiness..."
        
        # Test Kafka readiness by listing topics
        if docker exec "$KAFKA_CONTAINER" kafka-topics \
            --bootstrap-server "$BOOTSTRAP_SERVER" \
            --list >/dev/null 2>&1; then
            echo "✅ Kafka is ready!"
            return 0
        fi
        
        echo "⏳ Kafka not ready yet, waiting ${RETRY_DELAY}s..."
        sleep $RETRY_DELAY
    done
    
    echo "❌ Kafka failed to become ready after $MAX_RETRIES attempts"
    echo "🔍 Kafka container logs:"
    docker logs "$KAFKA_CONTAINER" --tail 20
    exit 1
}

# Function to create a topic with error handling
create_topic() {
    local topic_name=$1
    local partitions=${2:-3}
    local replication_factor=${3:-1}
    
    echo "📝 Creating topic: $topic_name (partitions: $partitions, replication: $replication_factor)"
    
    if docker exec "$KAFKA_CONTAINER" kafka-topics \
        --create \
        --topic "$topic_name" \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --partitions "$partitions" \
        --replication-factor "$replication_factor" \
        --if-not-exists; then
        echo "✅ Topic '$topic_name' created successfully"
    else
        echo "❌ Failed to create topic: $topic_name"
        return 1
    fi
}

# Function to verify topic creation
verify_topic() {
    local topic_name=$1
    
    echo "🔍 Verifying topic: $topic_name"
    
    if docker exec "$KAFKA_CONTAINER" kafka-topics \
        --describe \
        --topic "$topic_name" \
        --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; then
        echo "✅ Topic '$topic_name' verified"
        return 0
    else
        echo "❌ Topic '$topic_name' verification failed"
        return 1
    fi
}

# Main execution
main() {
    echo "🚀 Starting Kafka topic setup..."
    echo "🏠 Working directory: $(pwd)"
    echo "🐳 Docker status: $(docker --version)"
    
    # Wait for Kafka to be ready
    wait_for_kafka
    
    # Define topics to create
    declare -A TOPICS=(
        ["logs-raw"]="6:1"              # High throughput raw logs
        ["logs-processed"]="3:1"        # Processed logs
        ["anomalies-detected"]="3:1"    # Detected anomalies
        ["alerts-high-priority"]="1:1"  # Critical alerts
        ["ml-training-data"]="2:1"      # ML training datasets
        ["service-metrics"]="3:1"       # Service performance metrics
    )
    
    # Create topics
    echo "📋 Creating ${#TOPICS[@]} topics..."
    failed_topics=()
    
    for topic in "${!TOPICS[@]}"; do
        IFS=':' read -r partitions replication <<< "${TOPICS[$topic]}"
        
        if create_topic "$topic" "$partitions" "$replication"; then
            verify_topic "$topic" || failed_topics+=("$topic")
        else
            failed_topics+=("$topic")
        fi
    done
    
    # Results summary
    echo ""
    echo "📊 Topic Creation Summary:"
    echo "================================"
    
    if [ ${#failed_topics[@]} -eq 0 ]; then
        echo "✅ All topics created successfully!"
        
        # List all topics
        echo ""
        echo "📋 Available topics:"
        docker exec "$KAFKA_CONTAINER" kafka-topics \
            --list \
            --bootstrap-server "$BOOTSTRAP_SERVER"
        
        echo ""
        echo "🎉 Kafka setup completed successfully!"
        echo "🌐 Kafka UI available at: http://localhost:8081"
        echo "📊 Kafka JMX metrics on port: 19092"
        
    else
        echo "❌ Failed to create ${#failed_topics[@]} topics: ${failed_topics[*]}"
        echo ""
        echo "🔍 Troubleshooting:"
        echo "1. Check Kafka container logs: docker logs $KAFKA_CONTAINER"
        echo "2. Verify Kafka is running: docker ps | grep kafka"
        echo "3. Test Kafka connection: docker exec $KAFKA_CONTAINER kafka-broker-api-versions --bootstrap-server $BOOTSTRAP_SERVER"
        exit 1
    fi
}

# Cleanup function for graceful shutdown
cleanup() {
    echo ""
    echo "🛑 Script interrupted, cleaning up..."
    exit 1
}

# Set up signal handlers
trap cleanup INT TERM

# Run main function
main "$@"