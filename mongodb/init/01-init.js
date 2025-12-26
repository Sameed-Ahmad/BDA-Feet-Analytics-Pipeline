// MongoDB Initialization Script
print("Initializing Fleet Analytics Database...");

db = db.getSiblingDB('fleet_analytics');

// Create collections
db.createCollection("vehicles");
db.createCollection("drivers");
db.createCollection("warehouses");
db.createCollection("customers");
db.createCollection("telemetry_events");
db.createCollection("deliveries");
db.createCollection("incidents");
db.createCollection("routes");

// Create indexes
db.vehicles.createIndex({ "vehicle_id": 1 }, { unique: true });
db.drivers.createIndex({ "driver_id": 1 }, { unique: true });
db.warehouses.createIndex({ "warehouse_id": 1 }, { unique: true });
db.customers.createIndex({ "customer_id": 1 }, { unique: true });
db.telemetry_events.createIndex({ "vehicle_id": 1, "timestamp": -1 });
db.telemetry_events.createIndex({ "timestamp": -1 });
db.deliveries.createIndex({ "delivery_id": 1 }, { unique: true });
db.incidents.createIndex({ "incident_id": 1 }, { unique: true });
db.routes.createIndex({ "route_id": 1 }, { unique: true });

print("✓ MongoDB initialization complete!");
