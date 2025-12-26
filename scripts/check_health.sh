#!/bin/bash
echo "=== Fleet Analytics Health Check ==="
echo ""

# Check web services
echo "Checking web services..."
curl -f http://localhost:8080/health 2>/dev/null && echo "✓ Airflow OK" || echo "✗ Airflow FAIL"
curl -f http://localhost:3000/api/health 2>/dev/null && echo "✓ Grafana OK" || echo "✗ Grafana FAIL"
curl -f http://localhost:8081 2>/dev/null && echo "✓ Spark OK" || echo "✗ Spark FAIL"

echo ""
echo "Container status:"
docker ps --filter "name=fleet-" --format "table {{.Names}}\t{{.Status}}" | head -20
