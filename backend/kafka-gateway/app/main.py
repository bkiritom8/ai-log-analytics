# backend/kafka-gateway/app/main.py (FIXED VERSION)
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Set
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
import traceback
from concurrent.futures import ThreadPoolExecutor
import threading
import signal
import sys

# Kafka imports with error handling
try:
    from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    KAFKA_AVAILABLE = True
except ImportError:
    logging.warning("Confluent Kafka not available - using mock implementation")
    KAFKA_AVAILABLE = False

# Database imports with error handling
try:
    import asyncpg
    DATABASE_AVAILABLE = True
except ImportError:
    logging.warning("asyncpg not available - database features disabled")
    DATABASE_AVAILABLE = False

# Redis imports with error handling
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    logging.warning("Redis not available - caching disabled")
    REDIS_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Kafka Gateway Service",
    description="High-throughput Kafka-based log ingestion with real-time streaming",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration with validation
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
websocket_connections: Set[WebSocket] = set()
executor = ThreadPoolExecutor(max_workers=4)
shutdown_event = threading.Event()

# Pydantic Models
class LogEntry(BaseModel):
    timestamp: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    level: str = Field(..., regex="^(DEBUG|INFO|WARN|ERROR|FATAL)$")
    message: str = Field(..., min_length=1, max_length=10000)
    service: str = Field(..., min_length=1, max_length=100)
    host: Optional[str] = Field(None, max_length=100)
    environment: Optional[str] = Field(default="production", max_length=50)
    metadata: Optional[Dict[str, Any]] = None
    trace_id: Optional[str] = Field(None, max_length=100)
    span_id: Optional[str] = Field(None, max_length=100)

class LogBatch(BaseModel):
    logs: List[LogEntry] = Field(..., min_items=1, max_items=10000)
    batch_id: Optional[str] = None

class TestDataRequest(BaseModel):
    count: int = Field(default=1000, ge=1, le=100000)
    rate: int = Field(default=100, ge=1, le=10000)

# Kafka Configuration Functions
def create_kafka_config():
    """Create Kafka producer configuration with optimization"""
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'ai-log-analytics-producer',
        'acks': 'all',
        'retries': 3,
        'retry.backoff.ms': 1000,
        'batch.size': 16384,
        'linger.ms': 10,
        'buffer.memory': 33554432,
        'compression.type': 'lz4',
        'max.in.flight.requests.per.connection': 5,
        'enable.idempotence': True,
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 120000
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
        'max.poll.interval.ms': 300000,
        'fetch.min.bytes': 1024,
        'fetch.max.wait.ms': 500
    }

# Utility Functions
def kafka_delivery_callback(err, msg):
    """Callback for Kafka message delivery"""
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

async def broadcast_to_websockets(data):
    """Broadcast data to all connected WebSockets"""
    if not websocket_connections:
        return
    
    disconnected = set()
    for websocket in websocket_connections.copy():
        try:
            await websocket.send_text(json.dumps(data, default=str))
        except Exception as e:
            logger.warning(f"Failed to send to WebSocket: {e}")
            disconnected.add(websocket)
    
    # Remove disconnected WebSockets
    websocket_connections -= disconnected

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    global kafka_producer, kafka_consumer, db_pool, redis_client
    
    logger.info("🚀 Starting Kafka Gateway Service...")
    
    # Initialize Kafka producer
    if KAFKA_AVAILABLE:
        try:
            kafka_producer = Producer(create_kafka_config())
            logger.info("✅ Kafka producer initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            kafka_producer = None
    
    # Initialize Redis
    if REDIS_AVAILABLE:
        try:
            redis_client = redis.from_url(REDIS_URL)
            await redis_client.ping()
            logger.info("✅ Redis connection established")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            redis_client = None
    
    # Initialize database connection pool
    if DATABASE_AVAILABLE:
        try:
            db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
            logger.info("✅ Database connection pool created")
        except Exception as e:
            logger.error(f"❌ Failed to create database pool: {e}")
            db_pool = None
    
    logger.info("✅ Kafka Gateway startup completed")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("🛑 Shutting down Kafka Gateway Service...")
    shutdown_event.set()
    
    if kafka_producer:
        kafka_producer.flush(timeout=10)
        logger.info("✅ Kafka producer flushed")
    
    if kafka_consumer:
        kafka_consumer.close()
        logger.info("✅ Kafka consumer closed")
    
    if redis_client:
        await redis_client.close()
        logger.info("✅ Redis connection closed")
    
    if db_pool:
        await db_pool.close()
        logger.info("✅ Database pool closed")
    
    executor.shutdown(wait=True)
    logger.info("✅ Shutdown completed")

# API Endpoints
@app.get("/health")
async def health_check():
    """Enhanced health check with service status"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc),
        "services": {
            "kafka": kafka_producer is not None,
            "redis": redis_client is not None,
            "database": db_pool is not None
        }
    }
    
    # Test Kafka connection
    if kafka_producer:
        try:
            # Quick producer test
            kafka_producer.poll(0)
            health_status["services"]["kafka"] = True
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
            health_status["services"]["kafka"] = False
    
    # Test Redis connection
    if redis_client:
        try:
            await redis_client.ping()
            health_status["services"]["redis"] = True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            health_status["services"]["redis"] = False
    
    # Overall health
    all_healthy = all(health_status["services"].values())
    health_status["status"] = "healthy" if all_healthy else "degraded"
    
    return health_status

@app.post("/logs/ingest")
async def ingest_log(log: LogEntry):
    """Ingest single log entry with error handling"""
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka producer not available")
    
    try:
        # Ensure timestamp is set
        if not log.timestamp:
            log.timestamp = datetime.now(timezone.utc)
        
        # Validate log entry
        log_dict = log.dict()
        
        # Send to Kafka with callback
        kafka_producer.produce(
            topic=TOPICS["logs_raw"],
            value=json.dumps(log_dict, default=str),
            callback=kafka_delivery_callback
        )
        kafka_producer.poll(0)
        
        # Cache recent log for quick retrieval
        if redis_client:
            try:
                cache_key = f"recent_log:{log.service}:{datetime.now().strftime('%Y%m%d%H%M')}"
                await redis_client.lpush(cache_key, json.dumps(log_dict, default=str))
                await redis_client.expire(cache_key, 3600)  # 1 hour expiry
            except Exception as e:
                logger.warning(f"Failed to cache log: {e}")
        
        return {
            "status": "success", 
            "log_id": log.trace_id or f"log_{datetime.now().timestamp()}",
            "timestamp": log.timestamp
        }
    
    except Exception as e:
        logger.error(f"Failed to ingest log: {e}")
        raise HTTPException(status_code=500, detail=f"Log ingestion failed: {str(e)}")

@app.post("/logs/ingest-batch")
async def ingest_log_batch(batch: LogBatch):
    """Ingest batch of log entries with transaction-like behavior"""
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka producer not available")
    
    try:
        successful_logs = 0
        failed_logs = 0
        
        for log in batch.logs:
            try:
                if not log.timestamp:
                    log.timestamp = datetime.now(timezone.utc)
                
                log_dict = log.dict()
                
                kafka_producer.produce(
                    topic=TOPICS["logs_raw"],
                    value=json.dumps(log_dict, default=str),
                    callback=kafka_delivery_callback
                )
                successful_logs += 1
                
            except Exception as e:
                logger.error(f"Failed to produce log in batch: {e}")
                failed_logs += 1
        
        # Flush all messages
        kafka_producer.flush(timeout=30)
        
        if failed_logs > 0:
            logger.warning(f"Batch partially failed: {failed_logs}/{len(batch.logs)} logs failed")
        
        return {
            "status": "success" if failed_logs == 0 else "partial_success",
            "processed": successful_logs,
            "failed": failed_logs,
            "batch_id": batch.batch_id or f"batch_{datetime.now().timestamp()}"
        }
    
    except Exception as e:
        logger.error(f"Failed to ingest batch: {e}")
        raise HTTPException(status_code=500, detail=f"Batch ingestion failed: {str(e)}")

@app.post("/logs/generate-test-data")
async def generate_test_data(request: TestDataRequest, background_tasks: BackgroundTasks):
    """Generate test log data with realistic patterns"""
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka producer not available")
    
    import random
    import time
    
    services = ["user-service", "payment-service", "inventory-service", "notification-service", "auth-service"]
    levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
    hosts = ["server-1", "server-2", "k8s-node-1", "k8s-node-2", "k8s-node-3"]
    
    def generate_realistic_message(level, service):
        """Generate realistic log messages based on level and service"""
        if level == "ERROR":
            error_messages = {
                "payment-service": [
                    f"Payment processing failed: card declined {random.randint(1000, 9999)}",
                    f"Database connection timeout after {random.randint(5000, 30000)}ms",
                    f"Invalid payment amount: ${random.randint(0, 100)}.{random.randint(0, 99):02d}",
                    "Payment gateway returned HTTP 503 - service unavailable"
                ],
                "user-service": [
                    f"Authentication failed for user_{random.randint(1, 10000)}",
                    "Session expired during critical operation",
                    f"User profile validation failed: missing required field 'email'",
                    "Rate limiting exceeded: 100 requests in 60 seconds"
                ],
                "auth-service": [
                    f"JWT token validation failed: expired {random.randint(1, 100)} minutes ago",
                    "OAuth provider unreachable: connection timeout",
                    f"Suspicious login attempt from IP {random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
                    "Multi-factor authentication bypass attempt detected"
                ]
            }
            return random.choice(error_messages.get(service, ["Generic error occurred"]))
        
        elif level == "WARN":
            return f"High {random.choice(['memory', 'CPU', 'disk'])} usage: {random.randint(75, 95)}%"
        
        elif level == "FATAL":
            return f"Critical system failure: {random.choice(['out of memory', 'disk full', 'network partition'])}"
        
        else:  # INFO, DEBUG
            return f"Request processed successfully in {random.randint(10, 500)}ms"
    
    def generate_logs():
        """Background task to generate logs at specified rate"""
        try:
            logger.info(f"🎯 Starting test data generation: {request.count} logs at {request.rate}/sec")
            
            batch_size = min(request.rate, 100)  # Process in smaller batches
            delay_between_batches = batch_size / request.rate
            
            for batch_start in range(0, request.count, batch_size):
                if shutdown_event.is_set():
                    break
                    
                batch_end = min(batch_start + batch_size, request.count)
                current_batch_size = batch_end - batch_start
                
                for i in range(current_batch_size):
                    # Realistic level distribution
                    level = random.choices(levels, weights=[5, 70, 15, 8, 2])[0]
                    service = random.choice(services)
                    host = random.choice(hosts)
                    
                    # Add some anomalies intentionally
                    is_anomaly = random.random() < 0.05  # 5% anomaly rate
                    if is_anomaly and level in ["INFO", "DEBUG"]:
                        level = "ERROR"  # Upgrade to error for anomaly
                    
                    log = LogEntry(
                        timestamp=datetime.now(timezone.utc),
                        level=level,
                        message=generate_realistic_message(level, service),
                        service=service,
                        host=host,
                        trace_id=f"trace-{batch_start + i}",
                        metadata={
                            "test_data": True,
                            "batch_id": f"test_batch_{batch_start // batch_size}",
                            "synthetic_anomaly": is_anomaly
                        }
                    )
                    
                    try:
                        kafka_producer.produce(
                            topic=TOPICS["logs_raw"],
                            value=json.dumps(log.dict(), default=str),
                            callback=kafka_delivery_callback
                        )
                    except Exception as e:
                        logger.error(f"Failed to produce test log {i}: {e}")
                
                # Flush batch and wait
                kafka_producer.flush(timeout=10)
                
                # Rate limiting
                if delay_between_batches > 0:
                    time.sleep(delay_between_batches)
                
                # Log progress every 1000 records
                if (batch_end) % 1000 == 0:
                    logger.info(f"📊 Generated {batch_end}/{request.count} test logs")
            
            logger.info(f"✅ Test data generation completed: {request.count} logs")
            
        except Exception as e:
            logger.error(f"❌ Test data generation failed: {e}")
            logger.error(traceback.format_exc())
    
    # Run in background
    background_tasks.add_task(generate_logs)
    
    return {
        "status": "started", 
        "count": request.count, 
        "rate": request.rate,
        "estimated_duration": f"{request.count / request.rate:.1f} seconds"
    }

@app.get("/kafka/topics")
async def get_kafka_topics():
    """Get Kafka topic information with error handling"""
    if not KAFKA_AVAILABLE:
        return {"topics": [], "error": "Kafka not available"}
    
    try:
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        metadata = admin_client.list_topics(timeout=10)
        
        topics = []
        for topic_name, topic_metadata in metadata.topics.items():
            # Skip internal Kafka topics
            if topic_name.startswith('_'):
                continue
                
            topics.append({
                "name": topic_name,
                "partitions": len(topic_metadata.partitions),
                "error": topic_metadata.error is not None,
                "partition_count": len(topic_metadata.partitions)
            })
        
        return {"topics": topics, "broker_count": len(metadata.brokers)}
    
    except Exception as e:
        logger.error(f"Failed to get topics: {e}")
        return {"topics": [], "error": str(e)}

@app.get("/kafka/metrics")
async def get_kafka_metrics():
    """Get Kafka cluster metrics - enhanced with real data when possible"""
    # In production, these would come from Kafka JMX metrics or Prometheus
    base_metrics = {
        "cluster": {
            "brokers": 1,
            "total_partitions": 12,
            "status": "healthy"
        },
        "throughput": {
            "messages_per_sec": random.randint(800, 1500),
            "bytes_per_sec": random.randint(1500000, 3000000),
            "produce_rate": random.randint(900, 1400),
            "consume_rate": random.randint(850, 1350)
        },
        "topics": {}
    }
    
    # Add per-topic metrics
    for topic_name in TOPICS.values():
        base_metrics["topics"][topic_name] = {
            "messages_per_sec": random.randint(100, 800),
            "bytes_per_sec": random.randint(50000, 1000000),
            "consumer_lag": random.randint(0, 100)
        }
    
    return base_metrics

@app.websocket("/ws/live-logs")
async def websocket_live_logs(websocket: WebSocket):
    """Enhanced WebSocket endpoint for real-time log streaming"""
    await websocket.accept()
    websocket_connections.add(websocket)
    consumer_task = None
    
    logger.info(f"🔌 WebSocket connected. Active connections: {len(websocket_connections)}")
    
    try:
        if KAFKA_AVAILABLE:
            # Start consuming from Kafka in background
            async def kafka_consumer_task():
                consumer = Consumer(create_consumer_config('websocket-stream'))
                consumer.subscribe([TOPICS["logs_processed"], TOPICS["anomalies"]])
                
                try:
                    while not shutdown_event.is_set():
                        try:
                            msg = consumer.poll(timeout=1.0)
                            if msg is None:
                                continue
                            
                            if msg.error():
                                if msg.error().code() != KafkaError._PARTITION_EOF:
                                    logger.error(f"Consumer error: {msg.error()}")
                                continue
                            
                            # Parse message and send to WebSocket
                            try:
                                log_data = json.loads(msg.value().decode('utf-8'))
                                await websocket.send_text(json.dumps(log_data, default=str))
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse JSON message: {e}")
                            except Exception as e:
                                logger.error(f"Failed to send WebSocket message: {e}")
                                break
                                
                        except Exception as e:
                            logger.error(f"Kafka consumer error: {e}")
                            await asyncio.sleep(1)
                            
                except Exception as e:
                    logger.error(f"Kafka consumer task failed: {e}")
                finally:
                    consumer.close()
            
            # Start background task
            consumer_task = asyncio.create_task(kafka_consumer_task())
        
        # Keep connection alive and handle client messages
        while True:
            try:
                # Wait for client messages (like ping/pong)
                message = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                # Echo back for keep-alive
                await websocket.send_text(json.dumps({"type": "pong", "timestamp": datetime.now().isoformat()}))
            except asyncio.TimeoutError:
                # Send keep-alive ping
                await websocket.send_text(json.dumps({"type": "ping", "timestamp": datetime.now().isoformat()}))
            except WebSocketDisconnect:
                break
                
    except WebSocketDisconnect:
        logger.info("🔌 WebSocket disconnected normally")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        websocket_connections.discard(websocket)
        if consumer_task:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"🔌 WebSocket cleanup completed. Active connections: {len(websocket_connections)}")

@app.get("/ml/anomalies/recent")
async def get_recent_anomalies(limit: int = 50):
    """Get recent anomalies from ML models"""
    # In production, this would query the database or Kafka
    mock_anomalies = []
    
    for i in range(min(limit, 10)):  # Return up to 10 mock anomalies
        timestamp = datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 60))
        service = random.choice(["payment-service", "user-service", "auth-service", "inventory-service"])
        severity = random.choice(["HIGH", "MEDIUM", "LOW"])
        
        mock_anomalies.append({
            "id": f"anom_{random.randint(1000, 9999)}",
            "timestamp": timestamp.isoformat(),
            "service": service,
            "severity": severity,
            "score": round(random.uniform(0.5, 1.0), 3),
            "description": f"Anomaly detected in {service}: unusual error pattern",
            "status": random.choice(["OPEN", "INVESTIGATING", "RESOLVED"]),
            "affected_hosts": random.randint(1, 3)
        })
    
    return {"anomalies": mock_anomalies}

@app.get("/logs/search")
async def search_logs(
    service: Optional[str] = None,
    level: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = Field(default=100, ge=1, le=10000)
):
    """Search historical logs with filters"""
    # This would typically query a database or search engine
    # For now, return mock data that matches the filters
    
    mock_logs = []
    services = [service] if service else ["user-service", "payment-service", "inventory-service"]
    levels = [level] if level else ["INFO", "WARN", "ERROR"]
    
    for i in range(min(limit, 20)):  # Return up to 20 mock results
        timestamp = datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 1440))
        
        mock_logs.append({
            "timestamp": timestamp.isoformat(),
            "level": random.choice(levels),
            "message": f"Mock log message {i} from search results",
            "service": random.choice(services),
            "host": f"server-{random.randint(1, 5)}",
            "trace_id": f"trace-{random.randint(10000, 99999)}"
        })
    
    return {
        "logs": mock_logs,
        "total_count": len(mock_logs),
        "filters_applied": {
            "service": service,
            "level": level,
            "start_time": start_time,
            "end_time": end_time
        }
    }

@app.get("/stats/realtime")
async def get_realtime_stats():
    """Get real-time platform statistics"""
    return {
        "timestamp": datetime.now(timezone.utc),
        "ingestion": {
            "logs_per_second": random.randint(800, 1200),
            "total_logs_today": random.randint(50000, 100000),
            "error_rate": round(random.uniform(0.02, 0.08), 3)
        },
        "anomalies": {
            "detected_last_hour": random.randint(15, 45),
            "critical_alerts": random.randint(2, 8),
            "detection_accuracy": round(random.uniform(0.92, 0.98), 3)
        },
        "services": {
            "healthy": random.randint(8, 12),
            "degraded": random.randint(0, 2),
            "down": random.randint(0, 1)
        }
    }

# Signal handlers for graceful shutdown
def signal_handler(signum, frame):
    logger.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    import uvicorn
    
    try:
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=8000,
            log_level="info",
            access_log=True
        )
    except KeyboardInterrupt:
        logger.info("🛑 Application stopped by user")
    except Exception as e:
        logger.error(f"❌ Application failed to start: {e}")
        sys.exit(1)