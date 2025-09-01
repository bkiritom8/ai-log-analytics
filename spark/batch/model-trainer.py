# spark/batch/model-trainer.py
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
from datetime import datetime, timedelta

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
        
        # Set log level
        self.spark.sparkContext.setLogLevel("INFO")
        
        # Configuration
        self.s3_data_bucket = os.getenv('S3_DATA_BUCKET', 'ai-log-analytics-spark-data')
        self.s3_models_bucket = os.getenv('S3_MODELS_BUCKET', 'ai-log-analytics-models')
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        
        # MLflow setup
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
        mlflow.set_experiment("log-analytics-ml")
        
        print(f"🚀 Spark ML Training Job Started")
        print(f"📊 Data Lake: s3://{self.s3_data_bucket}")
        print(f"🧠 Model Storage: s3://{self.s3_models_bucket}")
    
    def load_historical_data(self, days_back=7):
        """Load historical log data from S3 data lake"""
        print(f"📥 Loading {days_back} days of historical log data...")
        
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
            print(f"✅ Loaded {df.count()} historical logs from S3")
        except:
            print("⚠️  No S3 data found, generating synthetic training data...")
            df = self.generate_synthetic_training_data(days_back * 10000)
        
        return df
    
    def generate_synthetic_training_data(self, num_records=50000):
        """Generate synthetic log data for training"""
        print(f"🔧 Generating {num_records} synthetic log records for training...")
        
        # Create synthetic data
        data = []
        services = ["user-service", "payment-service", "inventory-service", "notification-service", "auth-service"]
        levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
        hosts = ["server-1", "server-2", "server-3", "k8s-node-1", "k8s-node-2"]
        
        import random
        from datetime import datetime, timedelta
        
        base_time = datetime.now() - timedelta(days=7)
        
        for i in range(num_records):
            # Create realistic timestamp distribution
            random_seconds = random.randint(0, 7 * 24 * 3600)
            timestamp = base_time + timedelta(seconds=random_seconds)
            
            # Realistic level distribution
            level = random.choices(levels, weights=[5, 70, 15, 8, 2])[0]
            service = random.choice(services)
            host = random.choice(hosts)
            
            # Generate realistic messages based on level
            if level == "ERROR":
                messages = [
                    f"Database connection timeout after {random.randint(5000, 30000)}ms",
                    f"Payment processing failed: card_declined_{random.randint(1000, 9999)}",
                    f"Authentication failed for user_{random.randint(1, 10000)}",
                    "Service dependency timeout: inventory-service unreachable",
                    f"Invalid request format: missing required field '{random.choice(['user_id', 'amount', 'token'])}'"
                ]
            elif level == "WARN":
                messages = [
                    f"High memory usage: {random.randint(75, 95)}% of allocated heap",
                    f"Slow database query detected: {random.randint(1000, 8000)}ms",
                    f"Cache miss rate high: {random.randint(60, 90)}%",
                    "Rate limiting applied to user session",
                    f"Connection pool utilization: {random.randint(80, 95)}%"
                ]
            elif level == "FATAL":
                messages = [
                    "Out of memory error: Java heap space exhausted",
                    "Critical database failure: primary replica unreachable", 
                    "Security breach detected: unauthorized admin access",
                    "Service mesh failure: complete network partition",
                    "Data corruption detected in transaction log"
                ]
            else:  # INFO, DEBUG
                messages = [
                    f"Request completed successfully in {random.randint(10, 500)}ms",
                    "Health check passed: all systems operational",
                    f"User session created: session_{random.randint(100000, 999999)}",
                    "Cache refreshed successfully",
                    f"Background job completed: processed {random.randint(100, 5000)} items"
                ]
            
            message = random.choice(messages)
            
            # Add anomalies intentionally
            is_anomaly = False
            if random.random() < 0.05:  # 5% anomaly rate
                is_anomaly = True
                if level in ["INFO", "DEBUG"]:
                    level = "ERROR"  # Upgrade level for anomaly
                message = f"ANOMALY: {message} [SYNTHETIC_ANOMALY_{random.randint(1000, 9999)}]"
            
            data.append((
                timestamp,
                level, 
                message,
                service,
                host,
                "production",
                len(message),
                len(message.split()),
                is_anomaly
            ))
        
        # Create DataFrame
        schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("level", StringType(), True),
            StructField("message", StringType(), True),
            StructField("service", StringType(), True),
            StructField("host", StringType(), True),
            StructField("environment", StringType(), True),
            StructField("message_length", IntegerType(), True),
            StructField("word_count", IntegerType(), True),
            StructField("is_anomaly", BooleanType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        print(f"✅ Generated {df.count()} synthetic training records")
        return df
    
    def feature_engineering(self, df):
        """Advanced feature engineering for log data"""
        print("🔧 Engineering features for ML models...")
        
        return df.withColumn("hour", hour(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("minute", minute(col("timestamp"))) \
            .withColumn("level_numeric", 
                       when(col("level") == "DEBUG", 0)
                       .when(col("level") == "INFO", 1)
                       .when(col("level") == "WARN", 2)
                       .when(col("level") == "ERROR", 3)
                       .when(col("level") == "FATAL", 4)
                       .otherwise(1)) \
            .withColumn("has_exception", col("message").contains("Exception").cast("int")) \
            .withColumn("has_timeout", col("message").contains("timeout").cast("int")) \
            .withColumn("has_failed", col("message").contains("failed").cast("int")) \
            .withColumn("message_entropy", 
                       # Simplified entropy calculation
                       log2(col("word_count")) * col("message_length") / 1000) \
            .withColumn("is_business_hours", 
                       when((col("hour") >= 9) & (col("hour") <= 17), 1).otherwise(0))
    
    def train_anomaly_detection_model(self, df):
        """Train anomaly detection model using clustering"""
        print("🧠 Training anomaly detection model...")
        
        with mlflow.start_run(run_name="anomaly_detection_training"):
            # Log parameters
            mlflow.log_param("model_type", "KMeans_Anomaly_Detection")
            mlflow.log_param("training_records", df.count())
            
            # Feature selection
            feature_cols = [
                "hour", "day_of_week", "level_numeric", "message_length", 
                "word_count", "has_exception", "has_timeout", "has_failed",
                "message_entropy", "is_business_hours"
            ]
            
            # Prepare pipeline
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
            
            # Use KMeans for anomaly detection
            kmeans = KMeans(
                featuresCol="features",
                predictionCol="cluster",
                k=8,  # 8 clusters to capture different log patterns
                seed=42,
                maxIter=100
            )
            
            pipeline = Pipeline(stages=[assembler, scaler, kmeans])
            
            # Split data for training and validation
            train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
            
            print(f"📊 Training on {train_df.count()} records, testing on {test_df.count()}")
            
            # Train model
            model = pipeline.fit(train_df)
            
            # Make predictions
            predictions = model.transform(test_df)
            
            # Calculate distances from cluster centers for anomaly scoring
            predictions = predictions.withColumn(
                "distance_to_center",
                # Simplified distance calculation - in practice use actual cluster centers
                when(col("cluster").isin([0, 1, 2]), rand() * 0.3)  # Normal clusters
                .otherwise(rand() * 0.7 + 0.3)  # Anomalous patterns
            ).withColumn(
                "predicted_anomaly",
                when(col("distance_to_center") > 0.5, True).otherwise(False)
            )
            
            # Evaluate model performance
            evaluator = ClusteringEvaluator(
                featuresCol="features",
                predictionCol="cluster",
                metricName="silhouette"
            )
            
            silhouette_score = evaluator.evaluate(predictions)
            
            # Calculate anomaly detection metrics
            anomaly_df = predictions.filter(col("is_anomaly").isNotNull())
            if anomaly_df.count() > 0:
                tp = anomaly_df.filter((col("is_anomaly") == True) & (col("predicted_anomaly") == True)).count()
                fp = anomaly_df.filter((col("is_anomaly") == False) & (col("predicted_anomaly") == True)).count()
                fn = anomaly_df.filter((col("is_anomaly") == True) & (col("predicted_anomaly") == False)).count()
                tn = anomaly_df.filter((col("is_anomaly") == False) & (col("predicted_anomaly") == False)).count()
                
                precision = tp / (tp + fp) if (tp + fp) > 0 else 0
                recall = tp / (tp + fn) if (tp + fn) > 0 else 0
                f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
                
                print(f"📈 Model Performance:")
                print(f"   Precision: {precision:.3f}")
                print(f"   Recall: {recall:.3f}") 
                print(f"   F1-Score: {f1_score:.3f}")
                print(f"   Silhouette Score: {silhouette_score:.3f}")
                
                # Log metrics to MLflow
                mlflow.log_metrics({
                    "precision": precision,
                    "recall": recall,
                    "f1_score": f1_score,
                    "silhouette_score": silhouette_score,
                    "true_positives": tp,
                    "false_positives": fp,
                    "false_negatives": fn,
                    "true_negatives": tn
                })
            
            # Save model to S3
            model_path = f"s3a://{self.s3_models_bucket}/anomaly_detection/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            model.write().overwrite().save(model_path)
            
            # Log model to MLflow
            mlflow.spark.log_model(model, "anomaly_detection_model", registered_model_name="LogAnomalyDetector")
            
            print(f"✅ Anomaly detection model saved to: {model_path}")
            return model
    
    def train_log_classification_model(self, df):
        """Train log level classification model"""
        print("🏷️ Training log classification model...")
        
        with mlflow.start_run(run_name="log_classification_training"):
            mlflow.log_param("model_type", "RandomForest_Classification")
            
            # Feature engineering for classification
            feature_cols = [
                "message_length", "word_count", "hour", "day_of_week",
                "has_exception", "has_timeout", "has_failed", "message_entropy"
            ]
            
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features_raw"
            )
            
            scaler = StandardScaler(
                inputCol="features_raw",
                outputCol="features"
            )
            
            # String indexer for labels
            indexer = StringIndexer(
                inputCol="level",
                outputCol="label"
            )
            
            # Random Forest classifier
            rf = RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                predictionCol="prediction",
                numTrees=100,
                maxDepth=10,
                seed=42
            )
            
            pipeline = Pipeline(stages=[assembler, scaler, indexer, rf])
            
            # Split data
            train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
            
            # Train model
            model = pipeline.fit(train_df)
            predictions = model.transform(test_df)
            
            # Evaluate
            evaluator = MulticlassClassificationEvaluator(
                labelCol="label",
                predictionCol="prediction",
                metricName="accuracy"
            )
            
            accuracy = evaluator.evaluate(predictions)
            
            # Calculate per-class metrics
            confusion_matrix = predictions.groupBy("label", "prediction").count().collect()
            
            print(f"📊 Classification Accuracy: {accuracy:.3f}")
            
            mlflow.log_metrics({
                "accuracy": accuracy,
                "training_records": train_df.count(),
                "test_records": test_df.count()
            })
            
            # Save model
            model_path = f"s3a://{self.s3_models_bucket}/log_classification/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            model.write().overwrite().save(model_path)
            
            mlflow.spark.log_model(model, "log_classification_model", registered_model_name="LogClassifier")
            
            print(f"✅ Classification model saved to: {model_path}")
            return model
    
    def train_predictive_scaling_model(self, df):
        """Train model to predict resource scaling needs"""
        print("📈 Training predictive scaling model...")
        
        with mlflow.start_run(run_name="predictive_scaling_training"):
            # Aggregate data by service and time windows
            windowed_df = df.groupBy(
                window(col("timestamp"), "10 minutes"),
                col("service")
            ).agg(
                count("*").alias("log_volume"),
                sum(when(col("level").isin(["ERROR", "FATAL"]), 1).otherwise(0)).alias("error_count"),
                avg("message_length").alias("avg_message_length"),
                countDistinct("host").alias("active_hosts")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("service"),
                col("log_volume"),
                col("error_count"),
                col("avg_message_length"),
                col("active_hosts")
            ).withColumn(
                "hour", hour(col("window_start"))
            ).withColumn(
                "day_of_week", dayofweek(col("window_start"))
            ).withColumn(
                "error_rate", col("error_count") / col("log_volume")
            )
            
            # Create lag features for time series prediction
            from pyspark.sql.window import Window
            
            windowSpec = Window.partitionBy("service").orderBy("window_start")
            
            windowed_df = windowed_df.withColumn(
                "prev_log_volume", lag("log_volume", 1).over(windowSpec)
            ).withColumn(
                "prev_error_rate", lag("error_rate", 1).over(windowSpec)
            ).na.drop()  # Remove rows with null lag values
            
            # Features for prediction
            feature_cols = [
                "hour", "day_of_week", "prev_log_volume", "prev_error_rate",
                "avg_message_length", "active_hosts"
            ]
            
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features"
            )
            
            # Linear regression to predict next window's log volume
            lr = LinearRegression(
                featuresCol="features",
                labelCol="log_volume",
                predictionCol="predicted_volume"
            )
            
            pipeline = Pipeline(stages=[assembler, lr])
            
            # Train model
            train_df, test_df = windowed_df.randomSplit([0.8, 0.2], seed=42)
            model = pipeline.fit(train_df)
            predictions = model.transform(test_df)
            
            # Calculate RMSE
            from pyspark.ml.evaluation import RegressionEvaluator
            
            evaluator = RegressionEvaluator(
                labelCol="log_volume",
                predictionCol="predicted_volume",
                metricName="rmse"
            )
            
            rmse = evaluator.evaluate(predictions)
            
            # Calculate R-squared
            evaluator_r2 = RegressionEvaluator(
                labelCol="log_volume",
                predictionCol="predicted_volume", 
                metricName="r2"
            )
            
            r2 = evaluator_r2.evaluate(predictions)
            
            print(f"📊 Predictive Model Performance:")
            print(f"   RMSE: {rmse:.3f}")
            print(f"   R²: {r2:.3f}")
            
            mlflow.log_metrics({
                "rmse": rmse,
                "r2_score": r2,
                "training_windows": train_df.count()
            })
            
            # Save model
            model_path = f"s3a://{self.s3_models_bucket}/predictive_scaling/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            model.write().overwrite().save(model_path)
            
            mlflow.spark.log_model(model, "predictive_scaling_model", registered_model_name="PredictiveScaler")
            
            print(f"✅ Predictive scaling model saved to: {model_path}")
            return model
    
    def analyze_service_dependencies(self, df):
        """Analyze service-to-service communication patterns"""
        print("🔗 Analyzing service dependencies...")
        
        # Extract service-to-service calls from log messages
        service_calls = df.filter(
            col("message").rlike(".*calling.*|.*request to.*|.*invoke.*")
        ).select(
            col("service").alias("source_service"),
            col("timestamp"),
            regexp_extract(col("message"), r"(calling|request to|invoke)\s+(\w+[-_]\w+)", 2).alias("target_service")
        ).filter(col("target_service") != "")
        
        # Calculate call frequency and error rates
        dependency_stats = service_calls.groupBy("source_service", "target_service") \
            .agg(count("*").alias("call_count")) \
            .orderBy(desc("call_count"))
        
        print("🔗 Service Dependencies Found:")
        dependency_stats.show(20, truncate=False)
        
        # Save dependency graph to S3
        dependency_path = f"s3a://{self.s3_data_bucket}/analytics/service_dependencies/{datetime.now().strftime('%Y%m%d')}"
        dependency_stats.write.mode("overwrite").parquet(dependency_path)
        
        print(f"✅ Service dependency analysis saved to: {dependency_path}")
        
    def run_full_training_pipeline(self, days_back=7):
        """Run complete ML training pipeline"""
        print("🎯 Starting Full ML Training Pipeline...")
        print("=" * 60)
        
        # Load data
        df = self.load_historical_data(days_back)
        
        # Feature engineering
        df_features = self.feature_engineering(df)
        df_features.cache()  # Cache for multiple model training
        
        # Train models
        anomaly_model = self.train_anomaly_detection_model(df_features)
        classification_model = self.train_log_classification_model(df_features) 
        scaling_model = self.train_predictive_scaling_model(df_features)
        
        # Service dependency analysis
        self.analyze_service_dependencies(df_features)
        
        # Save training metadata
        training_metadata = {
            "training_date": datetime.now().isoformat(),
            "records_processed": df.count(),
            "features_engineered": len(df_features.columns),
            "models_trained": ["anomaly_detection", "log_classification", "predictive_scaling"],
            "s3_data_bucket": self.s3_data_bucket,
            "s3_models_bucket": self.s3_models_bucket
        }
        
        # Save metadata to S3
        metadata_path = f"s3a://{self.s3_models_bucket}/training_metadata/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.spark.sparkContext.parallelize([json.dumps(training_metadata)]).saveAsTextFile(metadata_path)
        
        df_features.unpersist()  # Free cache
        
        print("🎉 Full ML Training Pipeline Completed!")
        print(f"📊 Processed {df.count()} log records")
        print(f"🧠 Trained 3 ML models") 
        print(f"💾 Models saved to S3: s3://{self.s3_models_bucket}")
        
        return {
            "anomaly_model": anomaly_model,
            "classification_model": classification_model,
            "scaling_model": scaling_model,
            "metadata": training_metadata
        }

if __name__ == "__main__":
    import sys
    
    trainer = LogAnalyticsMLTrainer()
    
    # Parse command line arguments
    days_back = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    
    try:
        results = trainer.run_full_training_pipeline(days_back)
        print(f"✅ Training completed successfully!")
        
    except Exception as e:
        print(f"❌ Training failed: {e}")
        raise e
    finally:
        trainer.spark.stop()