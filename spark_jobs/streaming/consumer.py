"""
Spark Streaming Consumer - Reads from Kafka
Step 5.1: Basic Kafka to Spark streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import *


def create_spark_session():
    """Create Spark session with necessary configurations"""
    
    spark = SparkSession.builder \
        .appName(SPARK_CONFIG['app_name']) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.input.uri", MONGO_URI) \
        .config("spark.mongodb.output.uri", MONGO_URI) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel(SPARK_CONFIG['log_level'])
    
    print(" Spark Session created")
    return spark


def define_telemetry_schema():
    """Define schema for telemetry messages"""
    return StructType([
        StructField("vehicle_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("fuel_level", DoubleType(), True),
        StructField("engine_temp", DoubleType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("odometer", DoubleType(), True)
    ])


def define_delivery_schema():
    """Define schema for delivery messages"""
    return StructType([
        StructField("delivery_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("driver_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), False),
        StructField("scheduled_time", StringType(), True),
        StructField("actual_time", StringType(), True)
    ])


def define_incident_schema():
    """Define schema for incident messages"""
    return StructType([
        StructField("incident_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("driver_id", StringType(), True),
        StructField("incident_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("speed_at_incident", DoubleType(), True),
        StructField("timestamp", StringType(), False)
    ])


def read_kafka_stream(spark, topic, schema):
    """
    Read streaming data from Kafka topic
    
    Args:
        spark: SparkSession
        topic: Kafka topic name
        schema: StructType schema for the data
    
    Returns:
        DataFrame with parsed data
    """
    
    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and apply schema
    parsed_df = df.select(
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    ).select("data.*", "kafka_timestamp", "partition", "offset")
    
    print(f"Reading from Kafka topic: {topic}")
    return parsed_df


def validate_data(df, data_type):
    """
    Validate incoming data
    
    Args:
        df: Input DataFrame
        data_type: Type of data (telemetry, delivery, incident)
    
    Returns:
        Validated DataFrame
    """
    
    if data_type == 'telemetry':
        # Remove nulls in critical fields
        validated = df.filter(
            col("vehicle_id").isNotNull() &
            col("timestamp").isNotNull() &
            col("speed").isNotNull()
        )
        
        # Filter out invalid values
        validated = validated.filter(
            (col("speed") >= 0) & (col("speed") <= 200) &
            (col("fuel_level") >= 0) & (col("fuel_level") <= 100)
        )
        
    elif data_type == 'delivery':
        validated = df.filter(
            col("delivery_id").isNotNull() &
            col("vehicle_id").isNotNull() &
            col("status").isNotNull()
        )
    
    elif data_type == 'incident':
        validated = df.filter(
            col("incident_id").isNotNull() &
            col("vehicle_id").isNotNull() &
            col("incident_type").isNotNull()
        )
    
    else:
        validated = df
    
    return validated


def write_to_console(df, query_name, output_mode="append"):
    """
    Write stream to console for debugging
    
    Args:
        df: DataFrame to write
        query_name: Name for the query
        output_mode: Output mode (append, update, complete)
    """
    
    query = df \
        .writeStream \
        .outputMode(output_mode) \
        .format("console") \
        .option("truncate", False) \
        .queryName(query_name) \
        .start()
    
    return query


if __name__ == "__main__":
    print("="*60)
    print("SPARK STREAMING CONSUMER - STEP 5.1")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Define schemas
    telemetry_schema = define_telemetry_schema()
    delivery_schema = define_delivery_schema()
    incident_schema = define_incident_schema()
    
    # Read telemetry stream
    print("\n📡 Starting telemetry stream...")
    telemetry_stream = read_kafka_stream(
        spark,
        KAFKA_TOPICS['telemetry'],
        telemetry_schema
    )
    
    # Validate telemetry data
    validated_telemetry = validate_data(telemetry_stream, 'telemetry')
    
    # Write to console for testing
    query1 = write_to_console(
        validated_telemetry.select("vehicle_id", "speed", "fuel_level", "timestamp"),
        "telemetry-console"
    )
    
    print("\nTelemetry stream started")
    print("Monitoring incoming data... (Press Ctrl+C to stop)")
    
    # Wait for termination
    try:
        query1.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streams...")
        query1.stop()
        spark.stop()
        print("✅ Stopped cleanly")
