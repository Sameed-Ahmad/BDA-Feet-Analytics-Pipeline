#!/bin/bash
# Monitor MongoDB collections

echo "MongoDB Collection Statistics"
echo "================================="

docker exec -it fleet-mongodb mongosh \
  -u admin -p admin123 \
  --authenticationDatabase admin \
  --eval "
    use fleet_analytics;
    print('\\n Collection Counts:');
    print('Telemetry Events: ' + db.telemetry_events.countDocuments());
    print('Deliveries: ' + db.deliveries.countDocuments());
    print('Incidents: ' + db.incidents.countDocuments());
    print('Aggregations: ' + db.telemetry_aggregations.countDocuments());
    
    print('\\nSample Telemetry:');
    printjson(db.telemetry_events.findOne());
    
    print('\\nRecent Aggregations:');
    printjson(db.telemetry_aggregations.find().sort({window_start: -1}).limit(1).toArray());
  "
