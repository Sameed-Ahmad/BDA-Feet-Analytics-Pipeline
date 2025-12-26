#!/bin/bash
# Reset dimension data script

echo "This will delete all dimension data and regenerate it."
read -p "Are you sure? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

cd "$(dirname "$0")/.."
source venv/bin/activate

echo "Deleting existing dimension data..."
docker exec -it fleet-mongodb mongosh \
  -u admin -p admin123 \
  --authenticationDatabase admin \
  --quiet \
  --eval "
    const db = db.getSiblingDB('fleet_analytics');
    db.dim_vehicles.deleteMany({});
    db.dim_drivers.deleteMany({});
    db.dim_warehouses.deleteMany({});
    db.dim_customers.deleteMany({});
    console.log('Dimension data deleted');
  "

echo ""
echo "Regenerating dimension data..."
python mongodb/scripts/generate_dimensions.py

echo ""
echo "Dimension data reset complete!"
