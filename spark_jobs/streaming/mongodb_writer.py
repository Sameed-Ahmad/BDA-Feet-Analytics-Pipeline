"""
MongoDB Writer for Spark Streaming
Step 5.3: Write processed data to MongoDB
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pymongo import MongoClient, UpdateOne
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.spark_config import *


def write_to_mongodb_foreachBatch(df, collection_name):
    """
    Write DataFrame to MongoDB using foreachBatch
    
    Args:
        df: DataFrame to write
        collection_name: Target MongoDB collection
    """
    
    def process_batch(batch_df, batch_id):
        """Process each micro-batch"""
        
        print(f"\n🔄 Processing batch {batch_id}...")
        
        row_count = batch_df.count()
        print(f"   Rows in batch: {row_count}")
        
        if row_count == 0:
            print(f"   ⚠️  Batch {batch_id} is empty, skipping...")
            return
        
        # Convert to list of Row objects (no pandas needed!)
        records = batch_df.collect()
        
        # Connect to MongoDB
        try:
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DATABASE]
            collection = db[collection_name]
            
            # Insert records (handle duplicates)
            if len(records) > 0:
                operations = []
                
                for row in records:
                    # Convert Row to dictionary
                    clean_record = row.asDict()
                    
                    # Remove None values
                    clean_record = {k: v for k, v in clean_record.items() if v is not None}
                    
                    # Determine unique key based on collection
                    if collection_name == 'telemetry_events':
                        filter_key = {
                            'vehicle_id': clean_record.get('vehicle_id'),
                            'timestamp': clean_record.get('timestamp')
                        }
                    elif collection_name == 'deliveries':
                        filter_key = {'delivery_id': clean_record.get('delivery_id')}
                    elif collection_name == 'incidents':
                        filter_key = {'incident_id': clean_record.get('incident_id')}
                    else:
                        filter_key = clean_record
                    
                    operations.append(
                        UpdateOne(filter_key, {'$set': clean_record}, upsert=True)
                    )
                
                result = collection.bulk_write(operations)
                print(f"✅ Batch {batch_id}: Inserted/Updated {result.upserted_count + result.modified_count} documents in {collection_name}")
            
            client.close()
            
        except Exception as e:
            print(f"❌ Error writing batch {batch_id} to MongoDB: {e}")
            
    return process_batch


def write_stream_to_mongodb(df, collection_name, query_name, checkpoint_location):
    """
    Write streaming DataFrame to MongoDB
    
    Args:
        df: Streaming DataFrame
        collection_name: MongoDB collection name
        query_name: Name for the streaming query
        checkpoint_location: Checkpoint directory
    
    Returns:
        StreamingQuery object
    """
    
    query = df \
        .writeStream \
        .foreachBatch(write_to_mongodb_foreachBatch(df, collection_name)) \
        .option("checkpointLocation", f"{checkpoint_location}/{query_name}") \
        .queryName(query_name) \
        .start()
    
    print(f"✅ Started writing to MongoDB collection: {collection_name}")
    return query