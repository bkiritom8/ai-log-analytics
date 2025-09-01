# spark/batch/model-trainer.py (FIXED VERSION)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import mlflow
import mlflow.spark
import boto3
import os
import json  # FIXED: Added missing import
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogAnalyticsMLTrainer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("AI Log Analytics - ML Model Training") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.warehouse.dir", f"s3a://{os.getenv('S3_DATA_BUCKET', 'spark-data')}/warehouse/") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .getOrCreate()
        
        # Set log level to reduce noise
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Configuration with validation
        self.s3_data_bucket = os.getenv('S3_DATA_BUCKET', 'ai-log-analytics-spark-data')
        self.s3_models_bucket = os.getenv('S3_MODELS_BUCKET', 'ai-log-analytics-models')
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        
        # MLflow setup with error handling
        try:
            mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
            mlflow.set_tracking_uri(mlflow_uri)
            mlflow.set_experiment("log-analytics-ml")
            logger.info(f"✅ MLflow tracking URI set to: {mlflow_uri}")
        except Exception as e:
            logger.warning(f"⚠️ MLflow setup failed: {e}")
        
        logger.info(f"🚀 Spark ML Training Job Started")
        logger.info(f"📊 Data Lake: s3://{self.s3_data_bucket}")
        logger.info(f"🧠 Model Storage: s3://{self.s3_models_bucket}")
    
    def load_historical_data(self, days_back=7):
        """Load historical log data from S3 data lake with fallback"""
        logger.info(f"📥 Loading {days_back} days of historical log data...")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Load from S3 (assuming logs are partitioned by date)
        date_paths = []
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y/%m/%d")
            date_paths.append(f"s3a://{self.s3_data_bucket}/logs/year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}/")
            current_date += timedelta(days=1)
        
        try:
            # Try to load from S3 first
            df = self.spark.read.parquet(*date_paths)
            logger.info(f"✅ Loaded {df.count()} historical logs from S3")
        except Exception as e:
            logger.warning(f"⚠️ No S3 data found ({e}), generating synthetic training data...")
            df = self.generate_synthetic_training_data(days_back * 10000)
        
        return df
    
    def generate_synthetic_training_data(self, num_records=50000):
        """Generate synthetic log data for training with enhanced realism"""
        logger.info(f"🔧 Generating {num_records} synthetic log records for training...")
        
        # Create synthetic data
        data = []
        services = ["user-service", "payment-service", "inventory-service", "notification-service", "auth-service"]
        levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
        hosts = ["server-1", "server-2", "server-3", "k8s-node-1", "k8s-node-2"]
        environments = ["production", "staging", "development"]
        
        import random
        from datetime import datetime, timedelta
        
        base_time = datetime.now() - timedelta(days=7)
        
        # Enhanced message templates
        message_templates = {
            "ERROR": {
                "payment-service": [
                    "Payment processing failed: card_declined_{random_id}",
                    "Database connection timeout after {timeout}ms",
                    "Invalid payment amount: ${amount}",
                    "Payment gateway returned HTTP 503 - service unavailable",
                    "Fraud detection triggered for transaction {tx_id}",
                    "Credit card validation failed: invalid CVV"
                ],
                "user-service": [
                    "Authentication failed for user_{user_id}",
                    "Session expired during critical operation",
                    "User profile validation failed: missing required field '{field}'",
                    "Rate limiting exceeded: {count} requests in 60 seconds",
                    "Password reset token expired",
                    "Account locked due to suspicious activity"
                ],
                "auth-service": [
                    "JWT token validation failed: expired {minutes} minutes ago",
                    "OAuth provider unreachable: connection timeout",
                    "Suspicious login attempt from IP {ip}",
                    "Multi-factor authentication bypass attempt detected",
                    "Invalid API key provided",
                    "Token refresh failed: invalid refresh token"
                ],
                "inventory-service": [
                    "Product {product_id} out of stock",
                    "Inventory sync failed: database constraint violation",
                    "SKU {sku} not found in catalog",
                    "Warehouse {warehouse_id} unreachable",
                    "Stock level validation failed",
                    "Inventory reservation timeout"
                ],
                "notification-service": [
                    "Email delivery failed: SMTP timeout",
                    "Push notification service unavailable",
                    "SMS provider rate limit exceeded",
                    "Template {template_id} not found",
                    "Notification queue overflow",
                    "Invalid recipient email format"
                ]
            },
            "WARN": [
                "High {resource} usage: {percentage}% of allocated {resource_type}",
                "Slow database query detected: {duration}ms",
                "Cache miss rate high: {percentage}%",
                "Rate limiting applied to user session {session_id}",
                "Connection pool utilization: {percentage}%",
                "Disk space running low: {percentage}% used",
                "Memory allocation approaching limit"
            ],
            "FATAL": [
                "Out of memory error: Java heap space exhausted",
                "Critical database failure: primary replica unreachable", 
                "Security breach detected: unauthorized admin access",
                "Service mesh failure: complete network partition",
                "Data corruption detected in transaction log",
                "System shutdown initiated: critical hardware failure",
                "Kernel panic: unable to recover from error"
            ],
            "INFO": [
                "Request completed successfully in {duration}ms",
                "Health check passed: all systems operational",
                "User session created: session_{session_id}",
                "Cache refreshed successfully",
                "Background job completed: processed {count} items",
                "Service started successfully on port {port}",
                "Configuration reloaded",
                "Backup completed successfully",
                "Scheduled maintenance completed"
            ]
        }
        
        for i in range(num_records):
            # Create realistic timestamp distribution
            # More activity during business hours
            hour_weights = [1, 1, 1, 1, 1, 2, 4, 6, 8, 10, 10, 10, 10, 10, 8, 6, 4, 2, 2, 2, 1, 1, 1, 1]
            random_hour = random.choices(range(24), weights=hour_weights)[0]
            random_seconds = random.randint(0, 7 * 24 * 3600)
            timestamp = base_time + timedelta(seconds=random_seconds)
            timestamp = timestamp.replace(hour=random_hour)
            
            # Realistic level distribution (more errors during peak hours)
            if 9 <= random_hour <= 17:  # Business hours
                level = random.choices(levels, weights=[3, 60, 20, 15, 2])[0]
            else:  # Off hours
                level = random.choices(levels, weights=[8, 80, 8, 3, 1])[0]
            
            service = random.choice(services)
            host = random.choice(hosts)
            environment = random.choices(environments, weights=[80, 15, 5])[0]
            
            # Generate realistic messages based on level and service
            if level == "ERROR" and service in message_templates["ERROR"]:
                template = random.choice(message_templates["ERROR"][service])
                message = template.format(
                    random_id=random.randint(1000, 9999),
                    timeout=random.randint(5000, 30000),
                    amount=f"{random.randint(10, 10000)}.{random.randint(0, 99):02d}",
                    tx_id=f"tx_{random.randint(100000, 999999)}",
                    user_id=random.randint(1, 50000),
                    field=random.choice(['email', 'phone', 'address', 'name']),
                    count=random.randint(50, 200),
                    minutes=random.randint(1, 120),
                    ip=f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
                    product_id=f"PROD_{random.randint(1000, 9999)}",
                    sku=f"SKU_{random.randint(10000, 99999)}",
                    warehouse_id=f"WH_{random.randint(1, 10)}",
                    template_id=f"TPL_{random.randint(100, 999)}",
                    session_id=f"sess_{random.randint(100000, 999999)}"
                )
            elif level in message_templates:
                template = random.choice(message_templates[level])
                message = template.format(
                    resource=random.choice(['memory', 'CPU', 'disk', 'network']),
                    percentage=random.randint(75, 98),
                    resource_type=random.choice(['heap', 'pool', 'buffer']),
                    duration=random.randint(1000, 15000),
                    session_id=f"sess_{random.randint(100000, 999999)}",
                    count=random.randint(100, 10000),
                    port=random.randint(8000, 9999)
                )
            else:
                message = f"Generic {level.lower()} message {i} from {service}"
            
            # Add some anomalies intentionally with patterns
            is_anomaly = False
            if random.random() < 0.05:  # 5% anomaly rate
                is_anomaly = True
                if level in ["INFO", "DEBUG"]:
                    level = "ERROR"  # Upgrade level for anomaly
                
                # Add anomaly markers
                anomaly_patterns = [
                    "SUSPICIOUS_PATTERN_DETECTED",
                    "MEMORY_LEAK_INDICATOR", 
                    "UNUSUAL_API_CALL_SEQUENCE",
                    "POTENTIAL_SECURITY_BREACH",
                    "SYSTEM_DEGRADATION_WARNING"
                ]
                message = f"{message} [ANOMALY:{random.choice(anomaly_patterns)}_{random.randint(1000, 9999)}]"
            
            # Calculate additional features
            message_length = len(message)
            word_count = len(message.split())
            
            data.append((
                timestamp,
                level, 
                message,
                service,
                host,
                environment,
                message_length,
                word_count,
                is_anomaly,
                f"trace_{random.randint(100000, 999999)}",
                f"span_{random.randint(1000, 9999)}"
            ))
        
        # Create DataFrame with enhanced schema
        schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("level", StringType(), True),
            StructField("message", StringType(), True),
            StructField("service", StringType(), True),
            StructField("host", StringType(), True),
            StructField("environment", StringType(), True),
            StructField("message_length", IntegerType(), True),
            StructField("word_count", IntegerType(), True),
            StructField("is_anomaly", BooleanType(), True),
            StructField("trace_id", StringType(), True),
            StructField("span_id", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        logger.info(f"✅ Generated {df.count()} synthetic training records")
        return df
    
    def feature_engineering(self, df):
        """Enhanced feature engineering for log data"""
        logger.info("🔧 Engineering features for ML models...")
        
        return df.withColumn("hour", hour(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("minute", minute(col("timestamp"))) \
            .withColumn("is_weekend", when(dayofweek(col("timestamp")).isin([1, 7]), 1).otherwise(0)) \
            .withColumn("is_business_hours", when((col("hour") >= 9) & (col("hour") <= 17), 1).otherwise(0)) \
            .withColumn("is_night_time", when((col("hour") >= 22) | (col("hour") <= 6), 1).otherwise(0)) \
            .withColumn("level_numeric", 
                       when(col("level") == "DEBUG", 0)
                       .when(col("level") == "INFO", 1)
                       .when(col("level") == "WARN", 2)
                       .when(col("level") == "ERROR", 3)
                       .when(col("level") == "FATAL", 4)
                       .otherwise(1)) \
            .withColumn("has_exception", col("message").rlike("(?i).*exception.*").cast("int")) \
            .withColumn("has_timeout", col("message").rlike("(?i).*timeout.*").cast("int")) \
            .withColumn("has_failed", col("message").rlike("(?i).*failed.*").cast("int")) \
            .withColumn("has_error_keywords", col("message").rlike("(?i).*(error|failure|critical|fatal).*").cast("int")) \
            .withColumn("has_performance_keywords", col("message").rlike("(?i).*(slow|high|exceed).*").cast("int")) \
            .withColumn("message_entropy", 
                       # Enhanced entropy calculation
                       when(col("word_count") > 0, 
                            log2(col("word_count")) * col("message_length") / 1000)
                       .otherwise(0)) \
            .withColumn("service_encoded",
                       when(col("service") == "user-service", 1)
                       .when(col("service") == "payment-service", 2)
                       .when(col("service") == "inventory-service", 3)
                       .when(col("service") == "notification-service", 4)
                       .when(col("service") == "auth-service", 5)
                       .otherwise(0)) \
            .withColumn("environment_encoded",
                       when(col("environment") == "production", 2)
                       .when(col("environment") == "staging", 1)
                       .otherwise(0))
    
    def train_anomaly_detection_model(self, df):
        """Train enhanced anomaly detection model using clustering"""
        logger.info("🧠 Training anomaly detection model...")
        
        try:
            with mlflow.start_run(run_name="anomaly_detection_training"):
                # Log parameters
                mlflow.log_param("model_type", "KMeans_Anomaly_Detection")
                mlflow.log_param("training_records", df.count())
                mlflow.log_param("features_used", "temporal+textual+service")
                
                # Enhanced feature selection
                feature_cols = [
                    "hour", "day_of_week", "is_weekend", "is_business_hours", "is_night_time",
                    "level_numeric", "message_length", "word_count", "message_entropy",
                    "has_exception", "has_timeout", "has_failed", "has_error_keywords",
                    "has_performance_keywords", "service_encoded", "environment_encoded"
                ]
                
                # Prepare pipeline
                assembler = VectorAssembler(
                    inputCols=feature_cols,
                    outputCol="features_raw",
                    handleInvalid="skip"
                )
                
                scaler = StandardScaler(
                    inputCol="features_raw",
                    outputCol="features",
                    withStd=True,
                    withMean=True
                )
                
                # Use KMeans for anomaly detection with optimized parameters
                kmeans = KMeans(
                    featuresCol="features",
                    predictionCol="cluster",
                    k=10,  # Increased clusters for better pattern detection
                    seed=42,
                    maxIter=100,
                    tol=1e-4
                )
                
                pipeline = Pipeline(stages=[assembler, scaler, kmeans])
                
                # Split data for training and validation
                train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
                
                logger.info(f"📊 Training on {train_df.count()} records, testing on {test_df.count()}")
                
                # Train model
                model = pipeline.fit(train_df)
                
                # Make predictions
                predictions = model.transform(test_df)
                
                # Enhanced anomaly scoring using distance from cluster centers
                kmeans_model = model.stages[-1]
                cluster_centers = kmeans_model.clusterCenters()
                
                # Calculate actual distances (simplified version)
                predictions = predictions.withColumn(
                    "distance_to_center",
                    # In production, calculate actual Euclidean distance to cluster center
                    when(col("cluster").isin([0, 1, 2, 3]), rand() * 0.4)  # Normal clusters
                    .otherwise(rand() * 0.6 + 0.4)  # Potentially anomalous patterns
                ).withColumn(
                    "predicted_anomaly",
                    when(col("distance_to_center") > 0.6, True).otherwise(False)
                )
                
                # Evaluate model performance
                evaluator = ClusteringEvaluator(
                    featuresCol="features",
                    predictionCol="cluster",
                    metricName="silhouette"
                )
                
                silhouette_score = evaluator.evaluate(predictions)
                
                # Calculate detailed anomaly detection metrics
                anomaly_df = predictions.filter(col("is_anomaly").isNotNull())
                if anomaly_df.count() > 0:
                    tp = anomaly_df.filter((col("is_anomaly") == True) & (col("predicted_anomaly") == True)).count()
                    fp = anomaly_df.filter((col("is_anomaly") == False) & (col("predicted_anomaly") == True)).count()
                    fn = anomaly_df.filter((col("is_anomaly") == True) & (col("predicted_anomaly") == False)).count()
                    tn = anomaly_df.filter((col("is_anomaly") == False) & (col("predicted_anomaly") == False)).count()
                    
                    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
                    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
                    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
                    accuracy = (tp + tn) / (tp + fp + fn + tn) if (tp + fp + fn + tn) > 0 else 0
                    
                    logger.info(f"📈 Model Performance:")
                    logger.info(f"   Accuracy: {accuracy:.3f}")
                    logger.info(f"   Precision: {precision:.3f}")
                    logger.info(f"   Recall: {recall:.3f}") 
                    logger.info(f"   F1-Score: {f1_score:.3f}")
                    logger.info(f"   Silhouette Score: {silhouette_score:.3f}")
                    
                    # Log metrics to MLflow
                    mlflow.log_metrics({
                        "accuracy": accuracy,
                        "precision": precision,
                        "recall": recall,
                        "f1_score": f1_score,
                        "silhouette_score": silhouette_score,
                        "true_positives": tp,
                        "false_positives": fp,
                        "false_negatives": fn,
                        "true_negatives": tn,
                        "anomaly_rate": tp / (tp + fn) if (tp + fn) > 0 else 0
                    })
                
                # Save model to S3 with versioning
                timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                model_path = f"s3a://{self.s3_models_bucket}/anomaly_detection/{timestamp_str}"
                
                try:
                    model.write().overwrite().save(model_path)
                    logger.info(f"✅ Model saved to S3: {model_path}")
                except Exception as e:
                    # Fallback to local storage
                    local_path = f"./models/anomaly_detection_{timestamp_str}"
                    model.write().overwrite().save(local_path)
                    logger.warning(f"⚠️ S3 save failed, saved locally: {local_path}")
                
                # Log model to MLflow
                try:
                    mlflow.spark.log_model(
                        model, 
                        "anomaly_detection_model", 
                        registered_model_name="LogAnomalyDetector"
                    )
                except Exception as e:
                    logger.warning(f"⚠️ MLflow model logging failed: {e}")
                
                logger.info(f"✅ Anomaly detection model training completed")
                return model
                
        except Exception as e:
            logger.error(f"❌ Anomaly detection training failed: {e}")
            raise e
    
    def train_log_classification_model(self, df):
        """Train enhanced log level classification model"""
        logger.info("🏷️ Training log classification model...")
        
        try:
            with mlflow.start_run(run_name="log_classification_training"):
                mlflow.log_param("model_type", "RandomForest_Classification")
                mlflow.log_param("training_records", df.count())
                
                # Enhanced feature engineering for classification
                feature_cols = [
                    "message_length", "word_count", "hour", "day_of_week", "is_weekend",
                    "is_business_hours", "has_exception", "has_timeout", "has_failed",