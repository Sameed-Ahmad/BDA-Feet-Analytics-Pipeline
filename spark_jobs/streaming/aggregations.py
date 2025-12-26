"""
Spark Streaming Aggregations
Step 5.2: Windowing and real-time calculations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import *
from streaming.consumer import *


def calculate_running_averages(df):
    """
    Calculate running averages with windowing
    
    5-minute tumbling window for aggregations
    """
    
    # Add event time column
    df_with_time = df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )
    
    # Define 5-minute window
    windowed_df = df_with_time \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("vehicle_id")
        ) \
        .agg(
            avg("speed").alias("avg_speed"),
            max("speed").alias("max_speed"),
            min("speed").alias("min_speed"),
            avg("fuel_level").alias("avg_fuel_level"),
            avg("engine_temp").alias("avg_engine_temp"),
            count("*").alias("record_count")
        )
    
    # Flatten window column
    result = windowed_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("vehicle_id"),
        round(col("avg_speed"), 2).alias("avg_speed"),
        round(col("max_speed"), 2).alias("max_speed"),
        round(col("min_speed"), 2).alias("min_speed"),
        round(col("avg_fuel_level"), 2).alias("avg_fuel_level"),
        round(col("avg_engine_temp"), 2).alias("avg_engine_temp"),
        col("record_count")
    )
    
    return result


def detect_anomalies(df):
    """
    Detect anomalies in telemetry data
    
    Anomalies:
    - Speed > 140 km/h or < 0
    - Engine temp > 110°C or < 40°C
    - Fuel level < 5%
    """
    
    anomalies = df.filter(
        (col("speed") > ANOMALY_THRESHOLDS['speed_max']) |
        (col("speed") < ANOMALY_THRESHOLDS['speed_min']) |
        (col("engine_temp") > ANOMALY_THRESHOLDS['engine_temp_max']) |
        (col("engine_temp") < ANOMALY_THRESHOLDS['engine_temp_min']) |
        (col("fuel_level") < ANOMALY_THRESHOLDS['fuel_level_min'])
    )
    
    # Add anomaly type
    anomalies_with_type = anomalies.withColumn(
        "anomaly_type",
        when(col("speed") > ANOMALY_THRESHOLDS['speed_max'], "OVER_SPEED")
        .when(col("speed") < ANOMALY_THRESHOLDS['speed_min'], "INVALID_SPEED")
        .when(col("engine_temp") > ANOMALY_THRESHOLDS['engine_temp_max'], "OVERHEATING")
        .when(col("engine_temp") < ANOMALY_THRESHOLDS['engine_temp_min'], "ENGINE_COLD")
        .when(col("fuel_level") < ANOMALY_THRESHOLDS['fuel_level_min'], "LOW_FUEL")
        .otherwise("UNKNOWN")
    )
    
    return anomalies_with_type


if __name__ == "__main__":
    print("="*60)
    print("SPARK STREAMING AGGREGATIONS - STEP 5.2")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Read telemetry stream
    telemetry_schema = define_telemetry_schema()
    telemetry_stream = read_kafka_stream(
        spark,
        KAFKA_TOPICS['telemetry'],
        telemetry_schema
    )
    
    # Validate data
    validated = validate_data(telemetry_stream, 'telemetry')
    
    # Calculate running averages
    print("\nCalculating running averages (5-min windows)...")
    averages = calculate_running_averages(validated)
    
    # Detect anomalies
    print(" Detecting anomalies...")
    anomalies = detect_anomalies(validated)
    
    # Write averages to console
    query_avg = averages \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .queryName("running-averages") \
        .start()
    
    # Write anomalies to console
    query_anomalies = anomalies \
        .select("vehicle_id", "speed", "engine_temp", "fuel_level", "anomaly_type", "timestamp") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .queryName("anomalies") \
        .start()
    
    print("\nAggregations running... (Press Ctrl+C to stop)")
    
    # Wait for termination
    try:
        query_avg.awaitTermination()
    except KeyboardInterrupt:
        print("\n Stopping streams...")
        query_avg.stop()
        query_anomalies.stop()
        spark.stop()
        print("Stopped cleanly")
