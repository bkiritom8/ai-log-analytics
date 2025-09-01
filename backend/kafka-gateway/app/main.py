# backend/kafka-gateway/app/main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import asyncpg
import redis.asyncio as redis
import websockets
from concurrent.futures import ThreadPoolExecutor
import threading

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Kafka Gateway Service",
    description="High-throughput Kafka-based log ingestion with real-time streaming",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://loguser:logpass@localhost:5432/loganalytics")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Kafka Topics
TOPICS = {
    "logs_raw": "logs-raw",
    "logs_processed": "logs-processed", 
    "anomalies": "anomalies-detected",
    "alerts": "alerts-high-priority"
}

# Global connections
kafka_producer = None
kafka_consumer = None
db_pool = None
redis_client = None
websocket_connections = set()
executor = ThreadPoolExecutor(max_workers=4)

# Pydantic Models
class LogEntry(BaseModel):
    timestamp: Optional[datetime] = None
    level: str  # DEBUG, INFO, WARN, ERROR, FATAL
    message: str
    service: str
    host: Optional[str] = None
    environment: Optional[str] = "production"
    metadata: Optional[Dict[str, Any]] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None

class LogBatch(BaseModel):
    logs: List[LogEntry]
    batch_id: Optional[str] = None

class TestDataRequest(BaseModel):
    count: int = 1000
    rate: int = 100

# Kafka Configuration
def create_kafka_config():
    """Create Kafka configuration"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'ai-log-analytics-producer',
        'acks': 'all',
        'retries': 3,
        'batch.size': 16384,
        'linger.ms': 10,
        'buffer.memory': 33554432,
        'compression.type': 'lz4'
    }

def create_consumer_config(group_id):
    """Create Kafka consumer configuration"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 1000,
        'session.timeout.ms': 10000,
        'max.poll.interval.ms': 300000
    }

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    global kafka_producer, kafka_consumer, db_pool, redis_client
    
    # Initialize Kafka producer
    kafka_producer = Producer(create_kafka_config())
    
    # Initialize Kafka consumer for real-time streaming
    kafka_consumer = Consumer(create_consumer_config('realtime-dashboard'))
    kafka_consumer.subscribe([TOPICS["logs_processed"], TOPICS["anomalies"]])
    
    # Initialize Redis
    redis_client = redis.from_url(REDIS_URL)
    
    logger.info("✅ Kafka Gateway started successfully")

@app.on_event("shutdown")
async def shutdown_event():
    if kafka_producer:
        kafka_producer.flush()
    if kafka_consumer:
        kafka_consumer.close()
    if redis_client:
        await redis_client.close()

# API Endpoints
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}

@app.post("/logs/ingest")
async def ingest_log(log: LogEntry):
    """Ingest single log entry"""
    try:
        if not log.timestamp:
            log.timestamp = datetime.now(timezone.utc)
        
        # Send to Kafka
        kafka_producer.produce(
            topic=TOPICS["logs_raw"],
            value=json.dumps(log.dict(), default=str)
        )
        kafka_producer.poll(0)
        
        return {"status": "success", "log_id": log.trace_id or "generated"}
    
    except Exception as e:
        logger.error(f"Failed to ingest log: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/logs/ingest-batch")
async def ingest_log_batch(batch: LogBatch):
    """Ingest batch of log entries"""
    try:
        for log in batch.logs:
            if not log.timestamp:
                log.timestamp = datetime.now(timezone.utc)
            
            kafka_producer.produce(
                topic=TOPICS["logs_raw"],
                value=json.dumps(log.dict(), default=str)
            )
        
        kafka_producer.flush()
        return {"status": "success", "processed": len(batch.logs)}
    
    except Exception as e:
        logger.error(f"Failed to ingest batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/logs/generate-test-data")
async def generate_test_data(request: TestDataRequest):
    """Generate test log data"""
    import random
    
    services = ["user-service", "payment-service", "inventory-service", "notification-service"]
    levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
    hosts = ["server-1", "server-2", "k8s-node-1", "k8s-node-2"]
    
    def generate_logs():
        for i in range(request.count):
            level = random.choices(levels, weights=[5, 70, 15, 8, 2])[0]
            service = random.choice(services)
            
            log = LogEntry(
                timestamp=datetime.now(timezone.utc),
                level=level,
                message=f"Test message {i}: {random.choice(['success', 'processing', 'completed', 'failed'])}",
                service=service,
                host=random.choice(hosts),
                trace_id=f"trace-{i}"
            )
            
            kafka_producer.produce(
                topic=TOPICS["logs_raw"],
                value=json.dumps(log.dict(), default=str)
            )
            
            if i % 100 == 0:
                kafka_producer.flush()
        
        kafka_producer.flush()
    
    # Run in background
    executor.submit(generate_logs)
    return {"status": "started", "count": request.count}

@app.get("/kafka/topics")
async def get_kafka_topics():
    """Get Kafka topic information"""
    try:
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin_client.list_topics(timeout=10)
        
        topics = []
        for topic_name, topic_metadata in metadata.topics.items():
            topics.append({
                "name": topic_name,
                "partitions": len(topic_metadata.partitions),
                "error": topic_metadata.error is not None
            })
        
        return {"topics": topics}
    
    except Exception as e:
        logger.error(f"Failed to get topics: {e}")
        return {"topics": []}

@app.get("/kafka/metrics")
async def get_kafka_metrics():
    """Get Kafka cluster metrics"""
    return {
        "cluster": {
            "brokers": 2,
            "total_partitions": 12
        },
        "throughput": {
            "messages_per_sec": 1250,
            "bytes_per_sec": 2048000,
            "produce_rate": 1200,
            "consume_rate": 1180
        },
        "topics": {
            "logs-raw": {"messages_per_sec": 800, "bytes_per_sec": 1024000},
            "anomalies-detected": {"messages_per_sec": 50, "bytes_per_sec": 102400}
        }
    }

@app.websocket("/ws/live-logs")
async def websocket_live_logs(websocket: WebSocket):
    """WebSocket endpoint for real-time log streaming"""
    await websocket.accept()
    websocket_connections.add(websocket)
    
    try:
        # Start consuming from Kafka in background
        async def kafka_consumer_task():
            consumer = Consumer(create_consumer_config('websocket-stream'))
            consumer.subscribe([TOPICS["logs_processed"], TOPICS["anomalies"]])
            
            while True:
                try:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Parse message and send to WebSocket
                    try:
                        log_data = json.loads(msg.value().decode('utf-8'))
                        await websocket.send_text(json.dumps(log_data))
                    except Exception as e:
                        logger.error(f"Failed to send WebSocket message: {e}")
                        
                except Exception as e:
                    logger.error(f"Kafka consumer error: {e}")
                    await asyncio.sleep(1)
        
        # Start background task
        consumer_task = asyncio.create_task(kafka_consumer_task())
        
        # Keep connection alive
        while True:
            await websocket.receive_text()
            
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        websocket_connections.discard(websocket)
        if 'consumer_task' in locals():
            consumer_task.cancel()

@app.get("/ml/anomalies/recent")
async def get_recent_anomalies():
    """Get recent anomalies from ML models"""
    # Mock data for now
    return {
        "anomalies": [
            {
                "id": "anom_001",
                "timestamp": datetime.now().isoformat(),
                "service": "payment-service",
                "severity": "HIGH",
                "score": 0.87,
                "description": "Unusual error pattern detected"
            }
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)