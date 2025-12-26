#!/bin/bash
echo "Creating HDFS directories..."

docker exec fleet-namenode hdfs dfs -mkdir -p /warehouse/warm || true
docker exec fleet-namenode hdfs dfs -mkdir -p /warehouse/cold || true
docker exec fleet-namenode hdfs dfs -mkdir -p /warehouse/warm/telemetry_events || true
docker exec fleet-namenode hdfs dfs -mkdir -p /warehouse/warm/deliveries || true
docker exec fleet-namenode hdfs dfs -mkdir -p /warehouse/warm/incidents || true
docker exec fleet-namenode hdfs dfs -mkdir -p /warehouse/cold/telemetry_events || true
docker exec fleet-namenode hdfs dfs -mkdir -p /warehouse/cold/deliveries || true
docker exec fleet-namenode hdfs dfs -mkdir -p /warehouse/cold/incidents || true
docker exec fleet-namenode hdfs dfs -chmod -R 777 /warehouse || true

echo "✓ HDFS directories created!"
docker exec fleet-namenode hdfs dfs -ls /warehouse
