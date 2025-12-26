"""
Spark Configuration for Fleet Analytics
"""

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPICS = {
    'telemetry': 'vehicle-telemetry',
    'deliveries': 'deliveries',
    'incidents': 'incidents'
}

# MongoDB Configuration
MONGO_URI = "mongodb://admin:admin123@mongodb:27017/fleet_analytics?authSource=admin"
MONGO_DATABASE = "fleet_analytics"
MONGO_COLLECTIONS = {
    'telemetry': 'telemetry_events',
    'deliveries': 'deliveries',
    'incidents': 'incidents',
    'aggregations': 'telemetry_aggregations'
}

# Spark Configuration
SPARK_CONFIG = {
    'app_name': 'FleetAnalytics-Streaming',
    'master': 'spark://spark-master:7077',
    'executor_memory': '1g',
    'executor_cores': 2,
    'log_level': 'WARN'
}

# Streaming Configuration
STREAMING_CONFIG = {
    'batch_interval': 10,
    'checkpoint_location': '/tmp/spark-checkpoint',
    'window_duration': '5 minutes',
    'slide_duration': '1 minute'
}

# Anomaly Detection Thresholds
ANOMALY_THRESHOLDS = {
    'speed_max': 140,
    'speed_min': 0,
    'engine_temp_max': 110,
    'engine_temp_min': 40,
    'fuel_level_min': 5
}