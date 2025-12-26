#!/usr/bin/env python3
"""
MongoDB Data Quality Check Utility
"""

from pymongo import MongoClient

def check_data_quality():
    """Check data quality for all collections"""
    client = MongoClient('mongodb://admin:admin123@localhost:27017/')
    db = client['fleet_analytics']
    
    print("=" * 80)
    print(" Data Quality Check")
    print("=" * 80)
    
    # Check vehicles
    print("\n Vehicles:")
    vehicles_count = db.dim_vehicles.count_documents({})
    vehicles_no_id = db.dim_vehicles.count_documents({'vehicle_id': {'$exists': False}})
    print(f"  Total: {vehicles_count}")
    print(f"  Missing vehicle_id: {vehicles_no_id}")
    print(f"  Status: {'✅' if vehicles_no_id == 0 else '❌'}")
    
    # Check drivers
    print("\n Drivers:")
    drivers_count = db.dim_drivers.count_documents({})
    drivers_no_id = db.dim_drivers.count_documents({'driver_id': {'$exists': False}})
    print(f"  Total: {drivers_count}")
    print(f"  Missing driver_id: {drivers_no_id}")
    print(f"  Status: {'✅' if drivers_no_id == 0 else '❌'}")
    
    # Check warehouses
    print("\n Warehouses:")
    warehouses_count = db.dim_warehouses.count_documents({})
    warehouses_no_location = db.dim_warehouses.count_documents({'location': {'$exists': False}})
    print(f"  Total: {warehouses_count}")
    print(f"  Missing location: {warehouses_no_location}")
    print(f"  Status: {'✅' if warehouses_no_location == 0 else '❌'}")
    
    # Check customers
    print("\n Customers:")
    customers_count = db.dim_customers.count_documents({})
    customers_no_location = db.dim_customers.count_documents({'location': {'$exists': False}})
    print(f"  Total: {customers_count}")
    print(f"  Missing location: {customers_no_location}")
    print(f"  Status: {'✅' if customers_no_location == 0 else '❌'}")
    
    # Check fact tables
    print("\n Fact Tables:")
    print(f"  Telemetry Events: {db.telemetry_events.count_documents({}):,}")
    print(f"  Deliveries: {db.deliveries.count_documents({}):,}")
    print(f"  Incidents: {db.incidents.count_documents({}):,}")
    
    print("\n" + "=" * 80)
    print(" Data quality check complete!")
    print("=" * 80)
    
    client.close()

if __name__ == "__main__":
    check_data_quality()
