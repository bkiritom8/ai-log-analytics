# spark/streaming/anomaly_detector.py (FIXED VERSION)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
import json
import logging
import os
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealTimeAnomalyDetector:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Real-Time Log Anomaly Detection") \
            .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                   "org.apache.kafka:kafka-clients:3.4.0") \
            .getOrCreate()
        
        # Set appropriate log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.input_topic = "logs-raw"
        self.output_topic = "anomalies-detected"
        
        # Initialize Kafka producer for alerts (with error handling)
        try:
            from kafka import KafkaProducer
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers.split(","),
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                retries=3,
                acks='all',
                max_in_flight_requests_per_connection=1,
                enable_idempotence=True
            )
            logger.info("✅ Kafka producer initialized successfully")
        except ImportError:
            logger.warning("⚠️ kafka-python not available, using Spark Kafka sink only")
            self.kafka_producer = None
        except Exception as e:
            logger.error(f"⚠️ Failed to initialize Kafka producer: {e}")
            self.kafka_producer = None
        
        # Define log schema
        self.log_schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("level", StringType(), True),
            StructField("message", StringType(), True),
            StructField("service", StringType(), True),
            StructField("host", StringType(), True),
            StructField("environment", StringType(), True),
            StructField("metadata", StringType(), True),  # JSON string
            StructField("trace_id", StringType(), True),
            StructField("span_id", StringType(), True)
        ])
        
        logger.info("🚀 RealTimeAnomalyDetector initialized")
        
    def create_kafka_stream(self):
        """Create Kafka streaming DataFrame with error handling"""
        try:
            return self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.input_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .option("kafka.session.timeout.ms", "10000") \
                .option("kafka.request.timeout.ms", "20000") \
                .load()
        except Exception as e:
            logger.error(f"Failed to create Kafka stream: {e}")
            raise e
    
    def parse_log_data(self, kafka_df):
        """Parse JSON log data from Kafka with error handling"""
        try:
            return kafka_df.select(
                from_json(col("value").cast("string"), self.log_schema).alias("log"),
                col("timestamp").alias("kafka_timestamp"),
                col("offset"),
                col("partition")
            ).select("log.*", "kafka_timestamp", "offset", "partition") \
            .filter(col("timestamp").isNotNull()) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("message_length", length(col("message"))) \
            .withColumn("word_count", size(split(col("message"), " "))) \
            .withColumn("level_numeric", 
                       when(col("level") == "DEBUG", 0)
                       .when(col("level") == "INFO", 1)
                       .when(col("level") == "WARN", 2)
                       .when(col("level") == "ERROR", 3)
                       .when(col("level") == "FATAL", 4)
                       .otherwise(1)) \
            .withColumn("has_exception", col("message").rlike("(?i).*exception.*").cast("int")) \
            .withColumn("has_timeout", col("message").rlike("(?i).*timeout.*").cast("int")) \
            .withColumn("has_failed", col("message").rlike("(?i).*failed.*").cast("int"))
        except Exception as e:
            logger.error(f"Failed to parse log data: {e}")
            raise e
    
    def detect_anomalies_rules(self, df):
        """Enhanced rule-based anomaly detection"""
        return df.withColumn(
            "is_anomaly",
            # Critical patterns
            when(col("level") == "FATAL", True)
            .when(col("message").rlike("(?i).*(out of memory|heap space|segmentation fault).*"), True)
            .when(col("message").rlike("(?i).*(exception|error|failed|timeout|refused|panic|critical).*"), True)
            .when(col("message_length") > 2000, True)  # Unusually long messages
            .when(col("word_count") > 150, True)  # Too many words
            # Time-based anomalies (night time errors are suspicious)
            .when((col("hour") < 6) & (col("level").isin(["ERROR", "FATAL"])), True)
            # Service-specific patterns
            .when((col("service") == "payment-service") & col("message").rlike("(?i).*declined.*"), True)
            .when((col("service") == "auth-service") & col("message").rlike("(?i).*unauthorized.*"), True)
            .otherwise(False)
        ).withColumn(
            "anomaly_score",
            when(col("level") == "FATAL", 1.0)
            .when(col("message").rlike("(?i).*(out of memory|heap space).*"), 0.95)
            .when(col("level") == "ERROR", 0.8)
            .when(col("has_exception") == 1, 0.7)
            .when(col("has_timeout") == 1, 0.6)
            .when(col("has_failed") == 1, 0.5)
            .otherwise(0.1)
        ).withColumn(
            "severity",
            when(col("anomaly_score") >= 0.9, "CRITICAL")
            .when(col("anomaly_score") >= 0.7, "HIGH")
            .when(col("anomaly_score") >= 0.5, "MEDIUM")
            .otherwise("LOW")
        )
    
    def enrich_anomalies(self, anomaly_df):
        """Add additional context to detected anomalies"""
        return anomaly_df.filter(col("is_anomaly") == True) \
            .withColumn("detection_timestamp", current_timestamp()) \
            .withColumn("alert_id", concat(lit("alert_"), 
                       date_format(col("detection_timestamp"), "yyyyMMddHHmmss"),
                       lit("_"), monotonically_increasing_id())) \
            .withColumn("description", 
                       concat(lit("Anomaly detected in "), col("service"), 
                             lit(" service: "), substring(col("message"), 1, 100))) \
            .select(
                col("alert_id"),
                col("detection_timestamp"),
                col("timestamp").alias("log_timestamp"),
                col("service"),
                col("level"),
                col("host"),
                col("environment"),
                col("message"),
                col("anomaly_score"),
                col("severity"),
                col("description"),
                col("trace_id"),
                col("span_id")
            )
    
    def send_to_kafka(self, df, topic):
        """Send processed data back to Kafka with error handling"""
        checkpoint_dir = f"/tmp/checkpoint/{topic}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return df.select(
            to_json(struct(col("*"))).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_dir) \
            .outputMode("append") \
            .option("kafka.retries", "3") \
            .option("kafka.acks", "all")
    
    def start_streaming(self):
        """Start the real-time anomaly detection pipeline with proper error handling"""
        logger.info(f"🚀 Starting Real-Time Anomaly Detection...")
        logger.info(f"📥 Input Topic: {self.input_topic}")
        logger.info(f"📤 Output Topic: {self.output_topic}")
        logger.info(f"🔗 Kafka Brokers: {self.kafka_bootstrap_servers}")
        
        try:
            # Create input stream
            kafka_stream = self.create_kafka_stream()
            
            # Parse and process logs
            parsed_logs = self.parse_log_data(kafka_stream)
            
            # Detect anomalies
            anomaly_logs = self.detect_anomalies_rules(parsed_logs)
            
            # Enrich anomalies with additional context
            enriched_anomalies = self.enrich_anomalies(anomaly_logs)
            
            # Send anomalies to output topic
            anomaly_stream = self.send_to_kafka(enriched_anomalies, self.output_topic) \
                .trigger(processingTime='10 seconds') \
                .start()
            
            # Console output for monitoring
            console_stream = enriched_anomalies \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime='30 seconds') \
                .start()
            
            # Real-time statistics with proper windowing
            stats_stream = anomaly_logs \
                .withWatermark("timestamp", "5 minutes") \
                .groupBy(
                    window(col("timestamp"), "1 minute", "30 seconds"),
                    col("service"),
                    col("level")
                ) \
                .agg(
                    count("*").alias("total_logs"),
                    sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
                    avg("anomaly_score").alias("avg_anomaly_score"),
                    max("anomaly_score").alias("max_anomaly_score")
                ) \
                .withColumn("anomaly_rate", col("anomaly_count") / col("total_logs")) \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime='60 seconds') \
                .start()
            
            logger.info("✅ Streaming jobs started successfully!")
            logger.info("🖥️  Monitor at: http://localhost:4040 (Spark UI)")
            logger.info("📊 Kafka UI at: http://localhost:8081")
            
            # Wait for termination with proper cleanup
            try:
                anomaly_stream.awaitTermination()
            except KeyboardInterrupt:
                logger.info("🛑 Stopping streaming jobs...")
                anomaly_stream.stop()
                console_stream.stop() 
                stats_stream.stop()
            finally:
                self.spark.stop()
                
        except Exception as e:
            logger.error(f"❌ Streaming pipeline failed: {e}")
            self.spark.stop()
            raise e

def create_spark_session():
    """Fixed function from original anomaly_detector.py"""
    return SparkSession.builder \
        .appName("LogAnomalyDetector") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
        .getOrCreate()

def main():
    """Fixed main function with proper error handling"""
    try:
        spark = create_spark_session()
        
        # Read from Kafka with error handling
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "raw-logs") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Process logs with proper schema
        log_schema = StructType([
            StructField("message", StringType(), True),
            StructField("level", StringType(), True),
            StructField("service", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        logs_df = kafka_df.select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), log_schema).alias("log")
        ).select("kafka_timestamp", "log.*") \
        .filter(col("message").isNotNull() & col("level").isNotNull())
        
        # Enhanced anomaly detection
        anomalies = logs_df.filter(
            (col("level").isin(["ERROR", "FATAL"])) | 
            (length(col("message")) > 1000) |
            (col("message").rlike("(?i).*(exception|timeout|failed|critical).*"))
        ).withColumn("detection_timestamp", current_timestamp()) \
        .withColumn("anomaly_score", 
                   when(col("level") == "FATAL", 1.0)
                   .when(col("level") == "ERROR", 0.8)
                   .otherwise(0.6))
        
        # Output to console with proper formatting
        query = anomalies.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='15 seconds') \
            .start()
        
        logger.info("✅ Anomaly detection started successfully")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"❌ Main function failed: {e}")
        if 'spark' in locals():
            spark.stop()
        raise e

if __name__ == "__main__":
    # Use the new class for full functionality
    detector = RealTimeAnomalyDetector()
    detector.start_streaming()