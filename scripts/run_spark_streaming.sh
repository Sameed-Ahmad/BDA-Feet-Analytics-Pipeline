#!/bin/bash
# Run Spark Streaming Pipeline

cd "$(dirname "$0")/.."
source venv/bin/activate

echo " Starting Spark Streaming Pipeline..."
echo "   This will consume from Kafka and write to MongoDB"
echo ""

# Submit to Spark cluster
docker exec -it fleet-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  --conf "spark.mongodb.input.uri=mongodb://admin:admin123@mongodb:27017/fleet_analytics?authSource=admin" \
  --conf "spark.mongodb.output.uri=mongodb://admin:admin123@mongodb:27017/fleet_analytics?authSource=admin" \
  /opt/spark-jobs/streaming/complete_pipeline.py
