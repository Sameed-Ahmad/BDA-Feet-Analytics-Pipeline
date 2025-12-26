#!/usr/bin/env python3
"""
Sample MongoDB Queries for Fleet Analytics
"""

from pymongo import MongoClient
import json

def run_sample_queries():
    """Run sample queries on MongoDB"""
    client = MongoClient('mongodb://admin:admin123@localhost:27017/')
    db = client['fleet_analytics']
    
    print("=" * 80)
    print("Sample MongoDB Queries")
    print("=" * 80)
    
    # Query 1: Top 10 vehicles by type
    print("\n Vehicles by Type:")
    pipeline = [
        {'$group': {'_id': '$vehicle_type', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]
    for doc in db.dim_vehicles.aggregate(pipeline):
        print(f"  {doc['_id']}: {doc['count']}")
    
    # Query 2: Top 10 drivers by rating
    print("\n Top 10 Drivers by Rating:")
    for driver in db.dim_drivers.find().sort('rating', -1).limit(10):
        print(f"  {driver['name']}: {driver['rating']}  ({driver['experience_years']} years)")
    
    # Query 3: Drivers by status
    print("\n Drivers by Status:")
    pipeline = [
        {'$group': {'_id': '$status', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]
    for doc in db.dim_drivers.aggregate(pipeline):
        print(f"  {doc['_id']}: {doc['count']}")
    
    # Query 4: Customers by type
    print("\n Customers by Type:")
    pipeline = [
        {'$group': {'_id': '$customer_type', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]
    for doc in db.dim_customers.aggregate(pipeline):
        print(f"  {doc['_id']}: {doc['count']}")
    
    # Query 5: Latest telemetry events
    print("\n Latest 5 Telemetry Events:")
    for event in db.telemetry_events.find().sort('timestamp', -1).limit(5):
        print(f"  Vehicle {event.get('vehicle_id', 'N/A')}: Speed {event.get('speed', 0):.1f} km/h")
    
    print("\n" + "=" * 80)
    print(" Sample queries complete!")
    print("=" * 80)
    
    client.close()

if __name__ == "__main__":
    run_sample_queries()
