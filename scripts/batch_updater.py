#!/usr/bin/env python3
"""
Batch Updater for Delta Lake Demo.
This script performs batch updates on the Delta table for the Delta Lake demo.
"""

import os
import sys
import time
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add scripts directory to path
sys.path.append('/opt/spark/scripts')
try:
    import utils
except Exception as e:
    logger.error(f"Error importing utils: {e}")
    utils = None

# Constants
SPARK_HOME = '/opt/spark'
DATA_DIR = os.path.join(SPARK_HOME, 'data')
DELTA_TABLE_PATH = os.path.join(DATA_DIR, 'processed/global_superstore_delta')

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Batch Updater for Delta Lake Demo')
    parser.add_argument('--interval', type=int, default=30,
                        help='Interval between batches in seconds')
    parser.add_argument('--max-batches', type=int, default=1,
                        help='Maximum number of batches to generate')
    parser.add_argument('--update-pct', type=float, default=0.1,
                        help='Percentage of records to update in each batch')
    return parser.parse_args()

def update_batch(spark, batch_num=1, update_pct=0.1):
    """Update a batch of data in the Delta table."""
    try:
        # Check if Delta table exists
        if not os.path.exists(DELTA_TABLE_PATH):
            logger.error(f"Delta table not found at {DELTA_TABLE_PATH}")
            return False

        # Read the Delta table
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        total_records = df.count()
        
        # Calculate number of records to update
        num_records = int(total_records * update_pct)
        if num_records < 1:
            num_records = 1
        
        logger.info(f"Updating {num_records} records ({update_pct * 100:.1f}% of {total_records})")
        
        # Sample records to update
        records_to_update = df.sample(fraction=update_pct).limit(num_records)
        
        # Get the IDs of records to update
        order_ids = [row["Order ID"] for row in records_to_update.select("Order ID").collect()]
        
        # Generate updated values
        for order_id in order_ids:
            # Update sales and profit values
            new_sales = float(np.random.uniform(10, 1000).round(2))
            new_profit = float(np.random.uniform(-100, 500).round(2))
            
            # Update the record
            spark.sql(f"""
                UPDATE delta.`{DELTA_TABLE_PATH}`
                SET Sales = {new_sales}, Profit = {new_profit}, Last_Updated = '{datetime.now()}'
                WHERE `Order ID` = '{order_id}'
            """)
            
            logger.info(f"Updated Order ID {order_id}: Sales = {new_sales}, Profit = {new_profit}")
        
        return True
    except Exception as e:
        logger.error(f"Error updating batch: {e}")
        return False

def main():
    """Main function."""
    args = parse_args()
    logger.info(f"Starting batch updater with interval={args.interval}s, max_batches={args.max_batches}, update_pct={args.update_pct}")

    # Create Spark session
    if utils:
        spark = utils.create_spark_session("Batch Updater")
    else:
        # Fallback if utils is not available
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Batch Updater") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .getOrCreate()

    # Run batch updates
    batch_num = 1
    try:
        while True:
            # Check if max batches reached
            if args.max_batches > 0 and batch_num > args.max_batches:
                logger.info(f"Reached maximum number of batches ({args.max_batches})")
                break

            # Update batch
            logger.info(f"Running batch update {batch_num}")
            success = update_batch(spark, batch_num, args.update_pct)
            
            if success:
                logger.info(f"Batch update {batch_num} completed successfully")
            else:
                logger.error(f"Batch update {batch_num} failed")

            # Increment batch number
            batch_num += 1

            # Wait for next batch if not the last one
            if args.max_batches <= 0 or batch_num <= args.max_batches:
                logger.info(f"Waiting {args.interval} seconds for next batch")
                time.sleep(args.interval)
            
    except KeyboardInterrupt:
        logger.info("Batch updater stopped by user")
    except Exception as e:
        logger.error(f"Error in batch updater: {e}")
    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Batch updater finished")

if __name__ == '__main__':
    main()
