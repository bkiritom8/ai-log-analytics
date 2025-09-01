"""
Real-time Anomaly Detection with Spark Streaming
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("LogAnomalyDetector") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "raw-logs") \
        .load()
    
    # Process logs
    logs_df = kafka_df.select(
        col("timestamp"),
        from_json(col("value").cast("string"), 
                 StructType([
                     StructField("message", StringType()),
                     StructField("level", StringType()),
                     StructField("service", StringType())
                 ])).alias("log")
    ).select("timestamp", "log.*")
    
    # Simple anomaly detection (to be enhanced with ML)
    anomalies = logs_df.filter(
        (col("level") == "ERROR") | 
        (length(col("message")) > 1000)
    )
    
    # Output to console (for now)
    query = anomalies.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
