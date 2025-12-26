"""
MongoDB Schema Creation and Validation
Creates all 8 required collections with validation rules and indexes
"""

from pymongo import MongoClient, ASCENDING, DESCENDING, GEO2D
from pymongo.errors import CollectionInvalid
import logging
from datetime import datetime

class MongoDBSchemaManager:
    """
    Manages MongoDB schema creation, validation, and indexes
    """
    
    def __init__(self, connection_string='mongodb://admin:admin123@localhost:27017/'):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string: MongoDB connection URI
        """
        self.client = MongoClient(connection_string)
        self.db = self.client['fleet_analytics']
        self.logger = self._setup_logger()
        
        # Collection names
        self.collections = {
            'vehicles': 'dim_vehicles',
            'drivers': 'dim_drivers',
            'warehouses': 'dim_warehouses',
            'customers': 'dim_customers',
            'telemetry': 'telemetry_events',
            'aggregations': 'telemetry_aggregations',
            'deliveries': 'deliveries',
            'incidents': 'incidents'
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('MongoDBSchemaManager')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def create_all_schemas(self):
        """Create all collections with validation rules"""
        self.logger.info("🏗️  Creating MongoDB schemas...")
        
        # Create each collection
        self._create_vehicles_collection()
        self._create_drivers_collection()
        self._create_warehouses_collection()
        self._create_customers_collection()
        self._create_telemetry_collection()
        self._create_aggregations_collection()
        self._create_deliveries_collection()
        self._create_incidents_collection()
        
        self.logger.info(" All schemas created successfully!")
    
    def _create_vehicles_collection(self):
        """Create vehicles dimension collection"""
        collection_name = self.collections['vehicles']
        
        try:
            self.db.create_collection(
                collection_name,
                validator={
                    '$jsonSchema': {
                        'bsonType': 'object',
                        'required': ['vehicle_id', 'vehicle_type', 'make', 'model'],
                        'properties': {
                            'vehicle_id': {
                                'bsonType': 'string',
                                'description': 'Unique vehicle identifier'
                            },
                            'vehicle_type': {
                                'enum': ['Van', 'Truck', 'Pickup', 'SUV'],
                                'description': 'Type of vehicle'
                            },
                            'make': {
                                'bsonType': 'string',
                                'description': 'Vehicle manufacturer'
                            },
                            'model': {
                                'bsonType': 'string',
                                'description': 'Vehicle model'
                            },
                            'year': {
                                'bsonType': 'int',
                                'minimum': 2010,
                                'maximum': 2025
                            },
                            'capacity_kg': {
                                'bsonType': 'int',
                                'minimum': 0
                            },
                            'fuel_capacity': {
                                'bsonType': 'int',
                                'minimum': 0
                            },
                            'status': {
                                'enum': ['Active', 'Maintenance', 'Retired'],
                                'description': 'Vehicle operational status'
                            }
                        }
                    }
                }
            )
            self.logger.info(f" Created collection: {collection_name}")
        except CollectionInvalid:
            self.logger.warning(f"  Collection {collection_name} already exists")
        
        # Create indexes
        self.db[collection_name].create_index([('vehicle_id', ASCENDING)], unique=True)
        self.db[collection_name].create_index([('vehicle_type', ASCENDING)])
        self.db[collection_name].create_index([('status', ASCENDING)])
    
    def _create_drivers_collection(self):
        """Create drivers dimension collection"""
        collection_name = self.collections['drivers']
        
        try:
            self.db.create_collection(
                collection_name,
                validator={
                    '$jsonSchema': {
                        'bsonType': 'object',
                        'required': ['driver_id', 'name', 'license_number'],
                        'properties': {
                            'driver_id': {
                                'bsonType': 'string',
                                'description': 'Unique driver identifier'
                            },
                            'name': {
                                'bsonType': 'string',
                                'description': 'Driver full name'
                            },
                            'license_number': {
                                'bsonType': 'string',
                                'description': 'Driver license number'
                            },
                            'experience_years': {
                                'bsonType': 'int',
                                'minimum': 0
                            },
                            'rating': {
                                'bsonType': 'double',
                                'minimum': 0.0,
                                'maximum': 5.0
                            },
                            'status': {
                                'enum': ['Available', 'OnRoute', 'OnBreak', 'OffDuty'],
                                'description': 'Driver current status'
                            }
                        }
                    }
                }
            )
            self.logger.info(f" Created collection: {collection_name}")
        except CollectionInvalid:
            self.logger.warning(f"  Collection {collection_name} already exists")
        
        # Create indexes
        self.db[collection_name].create_index([('driver_id', ASCENDING)], unique=True)
        self.db[collection_name].create_index([('license_number', ASCENDING)], unique=True)
        self.db[collection_name].create_index([('status', ASCENDING)])
        self.db[collection_name].create_index([('rating', DESCENDING)])
    
    def _create_warehouses_collection(self):
        """Create warehouses dimension collection"""
        collection_name = self.collections['warehouses']
        
        try:
            self.db.create_collection(
                collection_name,
                validator={
                    '$jsonSchema': {
                        'bsonType': 'object',
                        'required': ['warehouse_id', 'name', 'location'],
                        'properties': {
                            'warehouse_id': {
                                'bsonType': 'string',
                                'description': 'Unique warehouse identifier'
                            },
                            'name': {
                                'bsonType': 'string',
                                'description': 'Warehouse name'
                            },
                            'location': {
                                'bsonType': 'object',
                                'required': ['latitude', 'longitude'],
                                'properties': {
                                    'latitude': {'bsonType': 'double'},
                                    'longitude': {'bsonType': 'double'}
                                }
                            },
                            'city': {'bsonType': 'string'},
                            'capacity_sqft': {
                                'bsonType': 'int',
                                'minimum': 0
                            }
                        }
                    }
                }
            )
            self.logger.info(f" Created collection: {collection_name}")
        except CollectionInvalid:
            self.logger.warning(f"  Collection {collection_name} already exists")
        
        # Create indexes
        self.db[collection_name].create_index([('warehouse_id', ASCENDING)], unique=True)
        self.db[collection_name].create_index([('location', GEO2D)])
        self.db[collection_name].create_index([('city', ASCENDING)])
    
    def _create_customers_collection(self):
        """Create customers dimension collection"""
        collection_name = self.collections['customers']
        
        try:
            self.db.create_collection(
                collection_name,
                validator={
                    '$jsonSchema': {
                        'bsonType': 'object',
                        'required': ['customer_id', 'name', 'location'],
                        'properties': {
                            'customer_id': {
                                'bsonType': 'string',
                                'description': 'Unique customer identifier'
                            },
                            'name': {
                                'bsonType': 'string',
                                'description': 'Customer name'
                            },
                            'location': {
                                'bsonType': 'object',
                                'required': ['latitude', 'longitude'],
                                'properties': {
                                    'latitude': {'bsonType': 'double'},
                                    'longitude': {'bsonType': 'double'}
                                }
                            },
                            'city': {'bsonType': 'string'},
                            'customer_type': {
                                'enum': ['Retail', 'Wholesale', 'Individual'],
                                'description': 'Type of customer'
                            }
                        }
                    }
                }
            )
            self.logger.info(f" Created collection: {collection_name}")
        except CollectionInvalid:
            self.logger.warning(f"  Collection {collection_name} already exists")
        
        # Create indexes
        self.db[collection_name].create_index([('customer_id', ASCENDING)], unique=True)
        self.db[collection_name].create_index([('location', GEO2D)])
        self.db[collection_name].create_index([('city', ASCENDING)])
        self.db[collection_name].create_index([('customer_type', ASCENDING)])
    
    def _create_telemetry_collection(self):
        """Create telemetry events fact collection"""
        collection_name = self.collections['telemetry']
        
        try:
            self.db.create_collection(
                collection_name,
                validator={
                    '$jsonSchema': {
                        'bsonType': 'object',
                        'required': ['vehicle_id', 'timestamp', 'latitude', 'longitude'],
                        'properties': {
                            'vehicle_id': {'bsonType': 'string'},
                            'timestamp': {'bsonType': 'date'},
                            'latitude': {'bsonType': 'double'},
                            'longitude': {'bsonType': 'double'},
                            'speed': {'bsonType': 'double', 'minimum': 0},
                            'fuel_level': {'bsonType': 'double', 'minimum': 0},
                            'engine_temp': {'bsonType': 'double'},
                            'odometer': {'bsonType': 'double', 'minimum': 0}
                        }
                    }
                }
            )
            self.logger.info(f" Created collection: {collection_name}")
        except CollectionInvalid:
            self.logger.warning(f"  Collection {collection_name} already exists")
        
        # Create indexes
        self.db[collection_name].create_index([('vehicle_id', ASCENDING), ('timestamp', DESCENDING)])
        self.db[collection_name].create_index([('timestamp', DESCENDING)])
        self.db[collection_name].create_index([('latitude', ASCENDING), ('longitude', ASCENDING)])
    
    def _create_aggregations_collection(self):
        """Create telemetry aggregations collection"""
        collection_name = self.collections['aggregations']
        
        try:
            self.db.create_collection(collection_name)
            self.logger.info(f" Created collection: {collection_name}")
        except CollectionInvalid:
            self.logger.warning(f"  Collection {collection_name} already exists")
        
        # Create indexes
        self.db[collection_name].create_index([('vehicle_id', ASCENDING), ('window_start', DESCENDING)])
        self.db[collection_name].create_index([('window_start', DESCENDING)])
    
    def _create_deliveries_collection(self):
        """Create deliveries fact collection"""
        collection_name = self.collections['deliveries']
        
        try:
            self.db.create_collection(
                collection_name,
                validator={
                    '$jsonSchema': {
                        'bsonType': 'object',
                        'required': ['delivery_id', 'vehicle_id', 'driver_id'],
                        'properties': {
                            'delivery_id': {'bsonType': 'string'},
                            'vehicle_id': {'bsonType': 'string'},
                            'driver_id': {'bsonType': 'string'},
                            'warehouse_id': {'bsonType': 'string'},
                            'customer_id': {'bsonType': 'string'},
                            'status': {
                                'enum': ['InProgress', 'Completed', 'Cancelled'],
                                'description': 'Delivery status'
                            }
                        }
                    }
                }
            )
            self.logger.info(f" Created collection: {collection_name}")
        except CollectionInvalid:
            self.logger.warning(f"  Collection {collection_name} already exists")
        
        # Create indexes
        self.db[collection_name].create_index([('delivery_id', ASCENDING)], unique=True)
        self.db[collection_name].create_index([('vehicle_id', ASCENDING)])
        self.db[collection_name].create_index([('driver_id', ASCENDING)])
        self.db[collection_name].create_index([('status', ASCENDING)])
    
    def _create_incidents_collection(self):
        """Create incidents fact collection"""
        collection_name = self.collections['incidents']
        
        try:
            self.db.create_collection(
                collection_name,
                validator={
                    '$jsonSchema': {
                        'bsonType': 'object',
                        'required': ['incident_id', 'vehicle_id', 'timestamp'],
                        'properties': {
                            'incident_id': {'bsonType': 'string'},
                            'vehicle_id': {'bsonType': 'string'},
                            'timestamp': {'bsonType': 'date'},
                            'incident_type': {
                                'enum': ['harsh_braking', 'harsh_acceleration', 'speeding', 'sharp_turn', 'idling_excessive'],
                                'description': 'Type of incident'
                            },
                            'severity': {
                                'enum': ['Low', 'Medium', 'High'],
                                'description': 'Incident severity'
                            }
                        }
                    }
                }
            )
            self.logger.info(f" Created collection: {collection_name}")
        except CollectionInvalid:
            self.logger.warning(f"  Collection {collection_name} already exists")
        
        # Create indexes
        self.db[collection_name].create_index([('incident_id', ASCENDING)], unique=True)
        self.db[collection_name].create_index([('vehicle_id', ASCENDING), ('timestamp', DESCENDING)])
        self.db[collection_name].create_index([('incident_type', ASCENDING)])
        self.db[collection_name].create_index([('severity', ASCENDING)])
    
    def verify_schemas(self):
        """Verify all collections exist with proper indexes"""
        self.logger.info(" Verifying schemas...")
        
        existing_collections = self.db.list_collection_names()
        
        for key, name in self.collections.items():
            if name in existing_collections:
                indexes = list(self.db[name].list_indexes())
                self.logger.info(f" {name}: {len(indexes)} indexes")
            else:
                self.logger.error(f" {name}: NOT FOUND")
        
        self.logger.info(" Schema verification complete!")
    
    def get_collection_stats(self):
        """Get statistics for all collections"""
        stats = {}
        
        for key, name in self.collections.items():
            count = self.db[name].count_documents({})
            stats[name] = count
        
        return stats
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        self.logger.info(" MongoDB connection closed")


def main():
    """Main function"""
    print("=" * 80)
    print("MongoDB Schema Creation for Fleet Analytics")
    print("=" * 80)
    
    # Create schema manager
    manager = MongoDBSchemaManager()
    
    try:
        # Create all schemas
        manager.create_all_schemas()
        
        # Verify schemas
        manager.verify_schemas()
        
        # Show stats
        print("\n" + "=" * 80)
        print(" Collection Statistics")
        print("=" * 80)
        stats = manager.get_collection_stats()
        for collection, count in stats.items():
            print(f"{collection}: {count:,} documents")
        
        print("\n Schema setup complete!")
    
    finally:
        manager.close()


if __name__ == "__main__":
    main()
