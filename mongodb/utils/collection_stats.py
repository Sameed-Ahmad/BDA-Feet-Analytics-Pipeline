#!/usr/bin/env python3
"""
MongoDB Collection Statistics Utility
"""

from pymongo import MongoClient
from datetime import datetime

def get_collection_stats():
    """Get statistics for all collections"""
    client = MongoClient('mongodb://admin:admin123@localhost:27017/')
    db = client['fleet_analytics']
    
    collections = [
        'dim_vehicles',
        'dim_drivers',
        'dim_warehouses',
        'dim_customers',
        'telemetry_events',
        'telemetry_aggregations',
        'deliveries',
        'incidents'
    ]
    
    print("=" * 80)
    print(f"MongoDB Collection Statistics - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    total_docs = 0
    total_size = 0
    
    for collection_name in collections:
        coll = db[collection_name]
        stats = db.command('collStats', collection_name)
        
        count = stats['count']
        size = stats['size']
        avg_size = stats.get('avgObjSize', 0)
        indexes = stats.get('nindexes', 0)
        
        total_docs += count
        total_size += size
        
        print(f"\n{collection_name}:")
        print(f"  Documents: {count:,}")
        print(f"  Size: {size / 1024 / 1024:.2f} MB")
        print(f"  Avg Doc Size: {avg_size:,} bytes")
        print(f"  Indexes: {indexes}")
    
    print("\n" + "=" * 80)
    print(f"TOTAL: {total_docs:,} documents, {total_size / 1024 / 1024:.2f} MB")
    print("=" * 80)
    
    client.close()

if __name__ == "__main__":
    get_collection_stats()
