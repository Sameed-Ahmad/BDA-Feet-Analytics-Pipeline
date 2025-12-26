"""
Fleet Analytics Pipeline - Main Data Generator
Step 2.7: Orchestrate all statistical models to generate complete fleet data

This is Part 1: Dimension Data Generation
Creates vehicles, drivers, warehouses, customers, and routes
"""

import numpy as np
import json
from datetime import datetime, timedelta
from typing import List, Dict
from faker import Faker
import sys
sys.path.append('.')

from data_generators.utils.base_generator import (
    BaseGenerator, generate_vehicle_id, generate_driver_id,
    generate_warehouse_id, generate_customer_id, generate_delivery_id,
    VEHICLE_TYPES, VEHICLE_MAKES, DRIVER_EXPERIENCE_LEVELS, WAREHOUSE_CITIES
)
from data_generators.models.markov_route import MarkovRouteGenerator
from data_generators.models.gaussian_speed import GaussianSpeedGenerator, DriverBehavior, TimeOfDay
from data_generators.models.poisson_incidents import PoissonIncidentGenerator
from data_generators.models.ar_telemetry import ARTelemetryGenerator
from data_generators.models.hmm_driver import HMMDriverBehavior, DriverState

from loguru import logger


class FleetDataGenerator(BaseGenerator):
    """
    Main orchestrator for generating complete fleet analytics data.
    
    Combines all statistical models:
    1. Markov Chain - Route generation
    2. Gaussian Distribution - Speed profiles
    3. Poisson Distribution - Incident generation
    4. Autoregressive AR(1) - Engine telemetry
    5. Hidden Markov Model - Driver behavior
    """
    
    def __init__(self):
        super().__init__()
        
        # Initialize Faker for realistic names and addresses
        self.faker = Faker()
        
        # Load configuration
        data_config = self.config['data_generation']
        self.num_vehicles = data_config['vehicles']
        self.num_drivers = data_config['drivers']
        self.num_warehouses = data_config['warehouses']
        self.num_customers = data_config['customers']
        
        # Initialize statistical model generators
        self.route_generator = MarkovRouteGenerator()
        self.speed_generator = GaussianSpeedGenerator()
        self.incident_generator = PoissonIncidentGenerator()
        self.telemetry_generator = ARTelemetryGenerator()
        self.behavior_generator = HMMDriverBehavior()
        
        # Storage for dimension data
        self.vehicles = []
        self.drivers = []
        self.warehouses = []
        self.customers = []
        self.routes = []
        
        # Counters for IDs
        self.delivery_counter = 0
        
        logger.info("Fleet Data Generator initialized")
        logger.info(f"Configuration: {self.num_vehicles} vehicles, {self.num_drivers} drivers, "
                   f"{self.num_warehouses} warehouses, {self.num_customers} customers")
    
    def generate_dimension_data(self):
        """Generate all dimension tables (vehicles, drivers, warehouses, customers)."""
        logger.info("Generating dimension data...")
        
        self.drivers = self._generate_drivers()
        self.vehicles = self._generate_vehicles()
        self.warehouses = self._generate_warehouses()
        self.customers = self._generate_customers()
        
        logger.info("Dimension data generation complete!")
        logger.info(f"Generated: {len(self.vehicles)} vehicles, {len(self.drivers)} drivers, "
                   f"{len(self.warehouses)} warehouses, {len(self.customers)} customers")
    
    def _generate_drivers(self) -> List[Dict]:
        """
        Generate driver dimension data.
        
        Returns:
            List of driver dictionaries
        """
        drivers = []
        
        logger.info(f"Generating {self.num_drivers} drivers...")
        
        for i in range(self.num_drivers):
            driver_id = generate_driver_id(i + 1)
            
            # Random attributes
            experience_level = np.random.choice(DRIVER_EXPERIENCE_LEVELS)
            years_experience = self._experience_to_years(experience_level)
            
            # Driver behavior profile (used for HMM)
            behavior_profile = np.random.choice([
                DriverBehavior.CAUTIOUS,
                DriverBehavior.NORMAL,
                DriverBehavior.AGGRESSIVE
            ], p=[0.20, 0.65, 0.15])  # Most drivers are normal
            
            driver = {
                'driver_id': driver_id,
                'driver_name': self.faker.name(),
                'experience_level': experience_level,
                'years_experience': years_experience,
                'behavior_profile': behavior_profile.value,
                'license_number': self.faker.bothify('DL-####-????'),
                'phone_number': self.faker.phone_number(),
                'hire_date': (datetime.now() - timedelta(days=years_experience*365)).strftime('%Y-%m-%d'),
                'city': np.random.choice(WAREHOUSE_CITIES),
                'performance_score': round(np.random.uniform(60, 100), 2),
                'total_deliveries': int(years_experience * 1000 * np.random.uniform(0.8, 1.2)),
                'incident_count': int(years_experience * 5 * (2.0 if behavior_profile == DriverBehavior.AGGRESSIVE else 0.5)),
                'created_at': self.get_timestamp()
            }
            
            drivers.append(driver)
        
        return drivers
    
    def _generate_vehicles(self) -> List[Dict]:
        """
        Generate vehicle dimension data.
        
        Returns:
            List of vehicle dictionaries
        """
        vehicles = []
        
        logger.info(f"Generating {self.num_vehicles} vehicles...")
        
        for i in range(self.num_vehicles):
            vehicle_id = generate_vehicle_id(i + 1)
            
            # Assign driver (some vehicles may share drivers in shifts)
            driver_id = generate_driver_id(np.random.randint(1, self.num_drivers + 1))
            
            # Random attributes
            vehicle_type = np.random.choice(VEHICLE_TYPES)
            make = np.random.choice(VEHICLE_MAKES)
            year = np.random.randint(2015, 2024)
            
            # Capacity based on type
            capacity_map = {
                'Van': (1000, 2000),
                'Truck': (3000, 5000),
                'Heavy Truck': (8000, 15000),
                'Refrigerated Truck': (4000, 8000)
            }
            capacity = np.random.randint(*capacity_map[vehicle_type])
            
            vehicle = {
                'vehicle_id': vehicle_id,
                'driver_id': driver_id,
                'vehicle_type': vehicle_type,
                'make': make,
                'model': f"{make} {vehicle_type}",
                'year': year,
                'vin': self.faker.bothify('VIN-################'),
                'license_plate': self.faker.bothify('???-####'),
                'capacity_kg': capacity,
                'fuel_tank_capacity_liters': 80 if vehicle_type == 'Van' else 150,
                'last_maintenance_date': (datetime.now() - timedelta(days=np.random.randint(1, 90))).strftime('%Y-%m-%d'),
                'next_maintenance_date': (datetime.now() + timedelta(days=np.random.randint(1, 90))).strftime('%Y-%m-%d'),
                'odometer_km': int((2024 - year) * 50000 * np.random.uniform(0.8, 1.2)),
                'status': np.random.choice(['active', 'maintenance', 'inactive'], p=[0.85, 0.10, 0.05]),
                'insurance_expiry': (datetime.now() + timedelta(days=np.random.randint(30, 365))).strftime('%Y-%m-%d'),
                'created_at': self.get_timestamp()
            }
            
            vehicles.append(vehicle)
        
        return vehicles
    
    def _generate_warehouses(self) -> List[Dict]:
        """
        Generate warehouse dimension data.
        
        Returns:
            List of warehouse dictionaries
        """
        warehouses = []
        
        logger.info(f"Generating {self.num_warehouses} warehouses...")
        
        # Use actual warehouse cities from constants
        warehouse_cities = WAREHOUSE_CITIES[:self.num_warehouses]
        
        for i, city in enumerate(warehouse_cities):
            warehouse_id = generate_warehouse_id(i + 1)
            
            # Get coordinates from route generator
            if city in self.route_generator.warehouse_coordinates:
                lat, lon = self.route_generator.warehouse_coordinates[city]
            else:
                # Fallback to Karachi base
                lat, lon = 24.8607 + np.random.uniform(-2, 2), 67.0011 + np.random.uniform(-2, 2)
            
            warehouse = {
                'warehouse_id': warehouse_id,
                'warehouse_name': f"{city} Distribution Center",
                'city': city,
                'address': self.faker.address(),
                'latitude': round(lat, 6),
                'longitude': round(lon, 6),
                'capacity_pallets': np.random.randint(500, 2000),
                'current_utilization_percent': round(np.random.uniform(50, 95), 2),
                'manager_name': self.faker.name(),
                'phone_number': self.faker.phone_number(),
                'operating_hours': '24/7' if np.random.random() > 0.3 else '06:00-22:00',
                'num_loading_docks': np.random.randint(4, 20),
                'created_at': self.get_timestamp()
            }
            
            warehouses.append(warehouse)
        
        return warehouses
    
    def _generate_customers(self) -> List[Dict]:
        """
        Generate customer dimension data.
        
        Returns:
            List of customer dictionaries
        """
        customers = []
        
        logger.info(f"Generating {self.num_customers} customers...")
        
        for i in range(self.num_customers):
            customer_id = generate_customer_id(i + 1)
            
            # Distribute customers across cities
            city = np.random.choice(WAREHOUSE_CITIES)
            
            # Customer type
            customer_type = np.random.choice(['business', 'residential'], p=[0.70, 0.30])
            
            # Generate coordinates near a city
            if city in self.route_generator.warehouse_coordinates:
                base_lat, base_lon = self.route_generator.warehouse_coordinates[city]
            else:
                base_lat, base_lon = 24.8607, 67.0011
            
            lat = base_lat + np.random.uniform(-0.5, 0.5)
            lon = base_lon + np.random.uniform(-0.5, 0.5)
            
            customer = {
                'customer_id': customer_id,
                'customer_name': self.faker.company() if customer_type == 'business' else self.faker.name(),
                'customer_type': customer_type,
                'city': city,
                'address': self.faker.address(),
                'latitude': round(lat, 6),
                'longitude': round(lon, 6),
                'phone_number': self.faker.phone_number(),
                'email': self.faker.email(),
                'registration_date': (datetime.now() - timedelta(days=np.random.randint(30, 1095))).strftime('%Y-%m-%d'),
                'total_orders': np.random.randint(1, 100),
                'customer_segment': np.random.choice(['premium', 'standard', 'basic'], p=[0.15, 0.60, 0.25]),
                'credit_limit': round(np.random.uniform(10000, 500000), 2),
                'created_at': self.get_timestamp()
            }
            
            customers.append(customer)
        
        return customers
    
    def _experience_to_years(self, experience_level: str) -> int:
        """Convert experience level to years."""
        mapping = {
            'Novice': np.random.randint(0, 2),
            'Intermediate': np.random.randint(2, 5),
            'Expert': np.random.randint(5, 10),
            'Master': np.random.randint(10, 20)
        }
        return mapping.get(experience_level, 3)
    
    def save_dimension_data(self, output_dir: str = 'data'):
        """
        Save dimension data to JSON files.
        
        Args:
            output_dir: Directory to save files
        """
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        datasets = {
            'vehicles': self.vehicles,
            'drivers': self.drivers,
            'warehouses': self.warehouses,
            'customers': self.customers
        }
        
        for name, data in datasets.items():
            filepath = f"{output_dir}/{name}.json"
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved {len(data)} {name} to {filepath}")


if __name__ == "__main__":
    # Test dimension data generation
    generator = FleetDataGenerator()
    
    logger.info("Testing dimension data generation...")
    generator.generate_dimension_data()
    
    # Show samples
    logger.info("\nSample Driver:")
    logger.info(json.dumps(generator.drivers[0], indent=2))
    
    logger.info("\nSample Vehicle:")
    logger.info(json.dumps(generator.vehicles[0], indent=2))
    
    logger.info("\nSample Warehouse:")
    logger.info(json.dumps(generator.warehouses[0], indent=2))
    
    logger.info("\nSample Customer:")
    logger.info(json.dumps(generator.customers[0], indent=2))
    
    # Save to files
    generator.save_dimension_data()
    
    logger.info("\n✓ Dimension data generation complete!")