#!/bin/bash
echo "🔄 Syncing Spark code to containers..."
docker cp spark_jobs/ fleet-spark-master:/opt/
docker cp spark_jobs/ fleet-spark-worker-1:/opt/
echo "✅ Code synced!"
