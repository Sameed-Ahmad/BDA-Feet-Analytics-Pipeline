"""
Dimension Data Generator for Fleet Analytics
Generates and loads dimension data: vehicles, drivers, warehouses, customers
"""

from pymongo import MongoClient
from faker import Faker
import random
import logging
from datetime import datetime, timedelta

class DimensionDataGenerator:
    """
    Generates dimension data for fleet analytics system
    """
    
    def __init__(self, connection_string='mongodb://admin:admin123@localhost:27017/'):
        """
        Initialize generator
        
        Args:
            connection_string: MongoDB connection URI
        """
        self.client = MongoClient(connection_string)
        self.db = self.client['fleet_analytics']
        self.faker = Faker()
        self.logger = self._setup_logger()
        
        # Karachi coordinates (your location)
        self.base_lat = 24.8607
        self.base_lon = 67.0011
        
        # Data counts (as per requirements)
        self.counts = {
            'vehicles': 500,
            'drivers': 350,
            'warehouses': 20,
            'customers': 10000
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('DimensionDataGenerator')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def generate_vehicles(self, count=None):
        """Generate vehicle dimension data"""
        count = count or self.counts['vehicles']
        self.logger.info(f" Generating {count} vehicles...")
        
        vehicle_types = ['Van', 'Truck', 'Pickup', 'SUV']
        makes = ['Ford', 'Mercedes', 'Toyota', 'Volvo', 'Isuzu', 'Hino', 'MAN']
        statuses = ['Active', 'Maintenance', 'Retired']
        
        vehicles = []
        for i in range(count):
            vehicle_type = random.choice(vehicle_types)
            
            # Set capacity based on type
            if vehicle_type == 'Van':
                capacity = random.randint(500, 1000)
            elif vehicle_type == 'Truck':
                capacity = random.randint(2000, 5000)
            elif vehicle_type == 'Pickup':
                capacity = random.randint(800, 1500)
            else:  # SUV
                capacity = random.randint(400, 800)
            
            # Generate last maintenance date as datetime
            days_ago = random.randint(1, 180)
            last_maintenance = datetime.utcnow() - timedelta(days=days_ago)
            
            vehicle = {
                'vehicle_id': f'VEH{str(i+1).zfill(4)}',
                'vehicle_type': vehicle_type,
                'make': random.choice(makes),
                'model': f'Model-{random.randint(1, 10)}',
                'year': random.randint(2018, 2024),
                'capacity_kg': capacity,
                'fuel_capacity': random.randint(60, 150),
                'license_plate': f'KHI-{random.randint(1000, 9999)}',
                'vin': self.faker.vin(),
                'status': random.choices(statuses, weights=[85, 10, 5])[0],
                'created_at': datetime.utcnow(),
                'last_maintenance': last_maintenance
            }
            vehicles.append(vehicle)
        
        # Insert into MongoDB
        collection = self.db['dim_vehicles']
        collection.delete_many({})  # Clear existing
        result = collection.insert_many(vehicles)
        
        self.logger.info(f" Inserted {len(result.inserted_ids)} vehicles")
        return vehicles
    
    def generate_drivers(self, count=None):
        """Generate driver dimension data"""
        count = count or self.counts['drivers']
        self.logger.info(f" Generating {count} drivers...")
        
        statuses = ['Available', 'OnRoute', 'OnBreak', 'OffDuty']
        
        drivers = []
        for i in range(count):
            experience = random.randint(1, 25)
            
            # Rating correlates with experience
            base_rating = 3.5 + (experience / 25) * 1.5
            rating = min(5.0, max(3.0, base_rating + random.uniform(-0.3, 0.3)))
            
            # Generate hire date as datetime
            days_ago = random.randint(365, 3650)  # 1-10 years
            hire_date = datetime.utcnow() - timedelta(days=days_ago)
            
            driver = {
                'driver_id': f'DRV{str(i+1).zfill(4)}',
                'name': self.faker.name(),
                'license_number': f'LIC{random.randint(100000, 999999)}',
                'phone': self.faker.phone_number(),
                'email': self.faker.email(),
                'experience_years': experience,
                'rating': round(rating, 2),
                'status': random.choices(statuses, weights=[40, 35, 15, 10])[0],
                'hire_date': hire_date,
                'created_at': datetime.utcnow()
            }
            drivers.append(driver)
        
        # Insert into MongoDB
        collection = self.db['dim_drivers']
        collection.delete_many({})  # Clear existing
        result = collection.insert_many(drivers)
        
        self.logger.info(f" Inserted {len(result.inserted_ids)} drivers")
        return drivers
    
    def generate_warehouses(self, count=None):
        """Generate warehouse dimension data"""
        count = count or self.counts['warehouses']
        self.logger.info(f" Generating {count} warehouses...")
        
        # Karachi areas
        areas = [
            ('Karachi North', 24.93, 67.06),
            ('Karachi South', 24.85, 67.03),
            ('Karachi East', 24.88, 67.09),
            ('Karachi West', 24.86, 66.99),
            ('Karachi Central', 24.87, 67.01)
        ]
        
        warehouses = []
        for i in range(count):
            area_name, lat, lon = random.choice(areas)
            
            warehouse = {
                'warehouse_id': f'WH{str(i+1).zfill(3)}',
                'name': f'{area_name} Warehouse {i+1}',
                'location': {
                    'latitude': lat + random.uniform(-0.02, 0.02),
                    'longitude': lon + random.uniform(-0.02, 0.02)
                },
                'address': self.faker.street_address(),
                'city': 'Karachi',
                'postal_code': f'{random.randint(74000, 75999)}',
                'capacity_sqft': random.randint(5000, 50000),
                'manager': self.faker.name(),
                'phone': self.faker.phone_number(),
                'operating_hours': '24/7',
                'created_at': datetime.utcnow()
            }
            warehouses.append(warehouse)
        
        # Insert into MongoDB
        collection = self.db['dim_warehouses']
        collection.delete_many({})  # Clear existing
        result = collection.insert_many(warehouses)
        
        self.logger.info(f" Inserted {len(result.inserted_ids)} warehouses")
        return warehouses
    
    def generate_customers(self, count=None):
        """Generate customer dimension data"""
        count = count or self.counts['customers']
        self.logger.info(f"Generating {count} customers...")
        
        customer_types = ['Retail', 'Wholesale', 'Individual']
        
        customers = []
        batch_size = 1000
        
        for i in range(count):
            customer_type = random.choices(
                customer_types,
                weights=[50, 30, 20]
            )[0]
            
            # Generate location within ~50km radius of Karachi
            lat = self.base_lat + random.uniform(-0.5, 0.5)
            lon = self.base_lon + random.uniform(-0.5, 0.5)
            
            customer = {
                'customer_id': f'CUST{str(i+1).zfill(5)}',
                'name': self.faker.company() if customer_type != 'Individual' else self.faker.name(),
                'customer_type': customer_type,
                'location': {
                    'latitude': lat,
                    'longitude': lon
                },
                'address': self.faker.street_address(),
                'city': 'Karachi',
                'postal_code': f'{random.randint(74000, 75999)}',
                'phone': self.faker.phone_number(),
                'email': self.faker.email(),
                'created_at': datetime.utcnow()
            }
            customers.append(customer)
            
            # Insert in batches to avoid memory issues
            if len(customers) >= batch_size:
                collection = self.db['dim_customers']
                if i < batch_size:
                    collection.delete_many({})  # Clear on first batch
                collection.insert_many(customers)
                self.logger.info(f"   Inserted batch: {i+1}/{count}")
                customers = []
        
        # Insert remaining
        if customers:
            collection = self.db['dim_customers']
            collection.insert_many(customers)
        
        self.logger.info(f" Inserted {count} customers")
    
    def generate_all(self):
        """Generate all dimension data"""
        self.logger.info("=" * 80)
        self.logger.info("Starting dimension data generation...")
        self.logger.info("=" * 80)
        
        self.generate_vehicles()
        self.generate_drivers()
        self.generate_warehouses()
        self.generate_customers()
        
        self.logger.info("=" * 80)
        self.logger.info(" All dimension data generated successfully!")
        self.logger.info("=" * 80)
    
    def verify_data(self):
        """Verify generated data"""
        self.logger.info("\n Verifying dimension data...")
        
        collections = {
            'dim_vehicles': self.counts['vehicles'],
            'dim_drivers': self.counts['drivers'],
            'dim_warehouses': self.counts['warehouses'],
            'dim_customers': self.counts['customers']
        }
        
        all_good = True
        for collection_name, expected_count in collections.items():
            actual_count = self.db[collection_name].count_documents({})
            status = "✅" if actual_count == expected_count else "❌"
            self.logger.info(
                f"{status} {collection_name}: {actual_count:,} / {expected_count:,}"
            )
            if actual_count != expected_count:
                all_good = False
        
        if all_good:
            self.logger.info("\n All dimension data verified!")
        else:
            self.logger.error("\n Some collections have incorrect counts!")
        
        return all_good
    
    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        self.logger.info(" MongoDB connection closed")


def main():
    """Main function"""
    print("=" * 80)
    print("Dimension Data Generator for Fleet Analytics")
    print("=" * 80)
    
    generator = DimensionDataGenerator()
    
    try:
        # Generate all dimension data
        generator.generate_all()
        
        # Verify data
        generator.verify_data()
        
        # Show sample records
        print("\n" + "=" * 80)
        print(" Sample Records")
        print("=" * 80)
        
        print("\n Sample Vehicle:")
        vehicle = generator.db['dim_vehicles'].find_one()
        for key, value in vehicle.items():
            if key != '_id':
                print(f"  {key}: {value}")
        
        print("\n Sample Driver:")
        driver = generator.db['dim_drivers'].find_one()
        for key, value in driver.items():
            if key != '_id':
                print(f"  {key}: {value}")
        
    finally:
        generator.close()


if __name__ == "__main__":
    main()
