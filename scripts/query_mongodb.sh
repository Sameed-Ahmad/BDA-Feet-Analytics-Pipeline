#!/bin/bash
# Interactive MongoDB queries

echo " MongoDB Query Interface"
echo "=========================="
echo ""
echo "Choose a query:"
echo "1. Count all documents"
echo "2. Show recent telemetry"
echo "3. Show aggregations"
echo "4. Show incidents"
echo "5. Show deliveries"
echo "6. Custom query"
echo ""
read -p "Enter choice (1-6): " choice

case $choice in
  1)
    docker exec -it fleet-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
      use fleet_analytics;
      print('Telemetry: ' + db.telemetry_events.countDocuments());
      print('Deliveries: ' + db.deliveries.countDocuments());
      print('Incidents: ' + db.incidents.countDocuments());
      print('Aggregations: ' + db.telemetry_aggregations.countDocuments());
    "
    ;;
  2)
    docker exec -it fleet-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
      use fleet_analytics;
      db.telemetry_events.find().sort({timestamp: -1}).limit(5).forEach(printjson);
    "
    ;;
  3)
    docker exec -it fleet-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
      use fleet_analytics;
      db.telemetry_aggregations.find().sort({window_start: -1}).limit(5).forEach(printjson);
    "
    ;;
  4)
    docker exec -it fleet-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
      use fleet_analytics;
      db.incidents.find().sort({timestamp: -1}).limit(5).forEach(printjson);
    "
    ;;
  5)
    docker exec -it fleet-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "
      use fleet_analytics;
      db.deliveries.find().sort({scheduled_time: -1}).limit(5).forEach(printjson);
    "
    ;;
  6)
    echo "Opening MongoDB shell..."
    docker exec -it fleet-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin fleet_analytics
    ;;
  *)
    echo "Invalid choice"
    ;;
esac
