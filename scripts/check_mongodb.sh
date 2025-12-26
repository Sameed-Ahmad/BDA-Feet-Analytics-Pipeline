#!/bin/bash
# MongoDB Health Check Script

echo "=========================================="
echo "MongoDB Health Check"
echo "=========================================="

# Check if container is running
if docker ps | grep -q fleet-mongodb; then
    echo "MongoDB container is running"
else
    echo "MongoDB container is NOT running"
    exit 1
fi

# Check MongoDB connection
if docker exec fleet-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo "MongoDB is accepting connections"
else
    echo "MongoDB is NOT accepting connections"
    exit 1
fi

# Check database exists (fixed)
DB_EXISTS=$(docker exec fleet-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --quiet --eval "db.getSiblingDB('fleet_analytics').getName()" 2>/dev/null | tr -d '\r\n' | grep -o "fleet_analytics")

if [ "$DB_EXISTS" == "fleet_analytics" ]; then
    echo "fleet_analytics database exists"
else
    echo "fleet_analytics database will be created when data is inserted"
fi

# Check collection counts
echo ""
docker exec fleet-mongodb mongosh \
  -u admin -p admin123 \
  --authenticationDatabase admin \
  --quiet \
  --eval "
    const db = db.getSiblingDB('fleet_analytics');
    console.log('Collection Counts:');
    console.log('  Vehicles:', db.dim_vehicles.countDocuments());
    console.log('  Drivers:', db.dim_drivers.countDocuments());
    console.log('  Warehouses:', db.dim_warehouses.countDocuments());
    console.log('  Customers:', db.dim_customers.countDocuments());
  " 2>/dev/null

echo ""
echo "=========================================="
echo "MongoDB health check complete!"
echo "=========================================="
