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

class KafkaTopicInfo(BaseModel):
    topic: str
    partitions: int
    replication_factor: int
    config: Dict[str, str]

class StreamingStats(BaseModel):
    topic: str
    partition: int
    offset: int
    lag: int
    consumer_group: str

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
    
    # Initialize PostgreSQL
    db_pool = await asyncpg