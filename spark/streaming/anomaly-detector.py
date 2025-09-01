# spark/streaming/anomaly-detector.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
import json
import os
from datetime import datetime, timedelta

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
                acks='all'
            )
            print("✅ Kafka producer initialized successfully")
        except ImportError:
            print("⚠️ kafka-python not available, using Spark Kafka sink only")
            self.kafka_producer = None
        except Exception as e:
            print(f"⚠️ Failed to initialize Kafka producer: {e}")
            self.kafka_producer = None
        
        # Define log schema
        self.log_schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("level", StringType(), True),
            StructField("message", StringType(), True),
            StructField("service", StringType(), True),
            StructField("host", StringType(), True),
            StructField("environment", StringType(), True),
            StructField("metadata", StringType(), True)  # JSON string
        ])
        
    def create_kafka_stream(self):
        """Create Kafka streaming DataFrame"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def parse_log_data(self, kafka_df):
        """Parse JSON log data from Kafka"""
        return kafka_df.select(
            from_json(col("value").cast("string"), self.log_schema).alias("log")
        ).select("log.*") \
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
                   .otherwise(1))
    
    def detect_anomalies_ml(self, df):
        """Detect anomalies using machine learning"""
        
        # Feature engineering
        feature_cols = ["hour", "day_of_week", "message_length", "word_count", "level_numeric"]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Use KMeans for anomaly detection (points far from cluster centers)
        kmeans = KMeans(
            featuresCol="features",
            predictionCol="cluster",
            k=5,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # This is a simplified approach - in production you'd load a pre-trained model
        # For streaming, we'll use statistical methods
        return df.withColumn(
            "is_anomaly",
            # Simple rule-based anomaly detection
            when((col("level") == "FATAL") | 
                 (col("message_length") > 1000) |
                 (col("word_count") > 100) |
                 (col("message").contains("Exception")) |
                 (col("message").contains("timeout")), True)
            .otherwise(False)
        ).withColumn(
            "anomaly_score",
            when(col("is_anomaly"), 
                 rand() * 0.5 + 0.5)  # Random score between 0.5-1.0 for demo
            .otherwise(rand() * 0.3)  # Lower score for normal logs
        )
    
    def detect_anomalies_rules(self, df):
        """Rule-based anomaly detection for real-time processing"""
        return df.withColumn(
            "is_anomaly",
            # Critical error patterns
            when(col("level") == "FATAL", True)
            .when(col("message").rlike("(?i).*(exception|error|failed|timeout|refused|panic).*"), True)
            .when(col("message_length") > 2000, True)  # Unusually long messages
            .when(col("word_count") > 150, True)  # Too many words
            # Time-based anomalies
            .when((col("hour") < 2) & (col("level") != "DEBUG"), True)  # Late night errors
            .otherwise(False)
        ).withColumn(
            "anomaly_score",
            when(col("level") == "FATAL", 1.0)
            .when(col("level") == "ERROR", 0.8)
            .when(col("message").rlike("(?i).*exception.*"), 0.7)
            .when(col("message").rlike("(?i).*(timeout|failed).*"), 0.6)
            .otherwise(0.1)
        ).withColumn(
            "severity",
            when(col("anomaly_score") >= 0.8, "CRITICAL")
            .when(col("anomaly_score") >= 0.6, "HIGH")
            .when(col("anomaly_score") >= 0.4, "MEDIUM")
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
                             lit(" service: "), col("message"))) \
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
                col("description")
            )
    
    def send_to_kafka(self, df, topic):
        """Send processed data back to Kafka"""
        return df.select(
            to_json(struct(col("*"))).alias("value")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", f"/tmp/checkpoint/{topic}") \
            .outputMode("append")
    
    def start_streaming(self):
        """Start the real-time anomaly detection pipeline"""
        print(f"🚀 Starting Real-Time Anomaly Detection...")
        print(f"📥 Input Topic: {self.input_topic}")
        print(f"📤 Output Topic: {self.output_topic}")
        print(f"🔗 Kafka Brokers: {self.kafka_bootstrap_servers}")
        
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
        
        # Also output to console for monitoring
        console_stream = enriched_anomalies \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='30 seconds') \
            .start()
        
        # Real-time statistics
        stats_stream = anomaly_logs \
            .groupBy(
                window(col("timestamp"), "1 minute"),
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
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime='60 seconds') \
            .start()
        
        print("✅ Streaming jobs started successfully!")
        print("🖥️  Monitor at: http://localhost:4040 (Spark UI)")
        print("📊 Kafka UI at: http://localhost:8080")
        
        # Wait for termination
        try:
            anomaly_stream.awaitTermination()
        except KeyboardInterrupt:
            print("🛑 Stopping streaming jobs...")
            anomaly_stream.stop()
            console_stream.stop()
            stats_stream.stop()
            self.spark.stop()

if __name__ == "__main__":
    detector = RealTimeAnomalyDetector()
    detector.start_streaming()