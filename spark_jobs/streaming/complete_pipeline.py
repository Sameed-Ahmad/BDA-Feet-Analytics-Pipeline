"""
Complete Spark Streaming Pipeline
Integrates: Kafka → Spark → Aggregations → MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import *
from streaming.consumer import *
from streaming.aggregations import *
from streaming.mongodb_writer import *


def main():
    """Main streaming pipeline"""
    
    print("="*70)
    print(" FLEET ANALYTICS - COMPLETE STREAMING PIPELINE")
    print("="*70)
    print("\n Starting complete streaming pipeline...")
    print("   - Reading from Kafka")
    print("   - Real-time aggregations")
    print("   - Anomaly detection")
    print("   - Writing to MongoDB")
    print("\n" + "="*70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Define schemas
    telemetry_schema = define_telemetry_schema()
    delivery_schema = define_delivery_schema()
    incident_schema = define_incident_schema()
    
    # ============================================
    # TELEMETRY STREAM
    # ============================================
    print("\n 1. Setting up TELEMETRY stream...")
    
    telemetry_stream = read_kafka_stream(
        spark,
        KAFKA_TOPICS['telemetry'],
        telemetry_schema
    )
    
    validated_telemetry = validate_data(telemetry_stream, 'telemetry')
    
    # Write raw telemetry to MongoDB
    query_telemetry = write_stream_to_mongodb(
        validated_telemetry,
        MONGO_COLLECTIONS['telemetry'],
        "telemetry-raw",
        STREAMING_CONFIG['checkpoint_location']
    )
    
    # Calculate aggregations
    aggregations = calculate_running_averages(validated_telemetry)
    
    # Write aggregations to MongoDB
    query_aggregations = write_stream_to_mongodb(
        aggregations,
        MONGO_COLLECTIONS['aggregations'],
        "telemetry-aggregations",
        STREAMING_CONFIG['checkpoint_location']
    )
    
    # Detect and write anomalies
    anomalies = detect_anomalies(validated_telemetry)
    
    # Show anomalies in console
    query_anomalies_console = anomalies \
        .select("vehicle_id", "speed", "engine_temp", "fuel_level", 
                "anomaly_type", "timestamp") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .queryName("anomalies-console") \
        .start()
    
    print(" Telemetry stream configured")
    
    # ============================================
    # DELIVERY STREAM
    # ============================================
    print("\n 2. Setting up DELIVERY stream...")
    
    delivery_stream = read_kafka_stream(
        spark,
        KAFKA_TOPICS['deliveries'],
        delivery_schema
    )
    
    validated_deliveries = validate_data(delivery_stream, 'delivery')
    
    # Write deliveries to MongoDB
    query_deliveries = write_stream_to_mongodb(
        validated_deliveries,
        MONGO_COLLECTIONS['deliveries'],
        "deliveries-stream",
        STREAMING_CONFIG['checkpoint_location']
    )
    
    print("Delivery stream configured")
    
    # ============================================
    # INCIDENT STREAM
    # ============================================
    print("\n3. Setting up INCIDENT stream...")
    
    incident_stream = read_kafka_stream(
        spark,
        KAFKA_TOPICS['incidents'],
        incident_schema
    )
    
    validated_incidents = validate_data(incident_stream, 'incident')
    
    # Write incidents to MongoDB
    query_incidents = write_stream_to_mongodb(
        validated_incidents,
        MONGO_COLLECTIONS['incidents'],
        "incidents-stream",
        STREAMING_CONFIG['checkpoint_location']
    )
    
    print("Incident stream configured")
    
    # ============================================
    # MONITORING
    # ============================================
    print("\n" + "="*70)
    print(" ALL STREAMS RUNNING")
    print("="*70)
    print("\nActive Streams:")
    print("   1. Telemetry → MongoDB (raw)")
    print("   2. Telemetry → MongoDB (aggregations)")
    print("   3. Deliveries → MongoDB")
    print("   4. Incidents → MongoDB")
    print("   5. Anomalies → Console")
    print("\nData is being written to MongoDB!")
    print("   Database: fleet_analytics")
    print("   Collections: telemetry_events, deliveries, incidents, telemetry_aggregations")
    print("\n⏱️Press Ctrl+C to stop all streams")
    print("="*70 + "\n")
    
    # Wait for termination
    try:
        query_telemetry.awaitTermination()
    except KeyboardInterrupt:
        print("\n Stopping all streams...")
        query_telemetry.stop()
        query_aggregations.stop()
        query_deliveries.stop()
        query_incidents.stop()
        query_anomalies_console.stop()
        spark.stop()
        print("All streams stopped cleanly")


if __name__ == "__main__":
    main()
