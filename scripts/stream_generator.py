#!/usr/bin/env python3
"""
Stream Generator for Delta Lake Demo.
This script generates streaming data for the Delta Lake demo.
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
STREAM_DIR = os.path.join(DATA_DIR, 'stream')

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Stream Generator for Delta Lake Demo')
    parser.add_argument('--interval', type=int, default=5,
                        help='Interval between batches in seconds')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Number of records per batch')
    parser.add_argument('--max-batches', type=int, default=0,
                        help='Maximum number of batches to generate (0 for infinite)')
    return parser.parse_args()

def generate_batch(batch_size=10, batch_num=1):
    """Generate a batch of data."""
    # Generate test data
    if utils:
        df = utils.generate_test_data(num_records=batch_size)
    else:
        # Fallback if utils is not available
        data = {
            'Row ID': range(1, batch_size + 1),
            'Order ID': [f"STREAM-{batch_num}-{i}" for i in range(batch_size)],
            'Order Date': [
                (datetime.now() - timedelta(days=np.random.randint(0, 30))).strftime('%Y-%m-%d')
                for _ in range(batch_size)
            ],
            'Ship Date': [
                (datetime.now() + timedelta(days=np.random.randint(1, 10))).strftime('%Y-%m-%d')
                for _ in range(batch_size)
            ],
            'Ship Mode': np.random.choice(['Standard Class', 'First Class', 'Second Class', 'Same Day'], batch_size),
            'Customer ID': [f"CG-STREAM-{np.random.randint(10000, 99999)}" for _ in range(batch_size)],
            'Customer Name': [f"Stream-Customer-{i}" for i in range(batch_size)],
            'Segment': np.random.choice(['Consumer', 'Corporate', 'Home Office'], batch_size),
            'Category': np.random.choice(['Furniture', 'Office Supplies', 'Technology'], batch_size),
            'Sub-Category': np.random.choice(['Chairs', 'Tables', 'Phones', 'Storage', 'Binders'], batch_size),
            'Sales': np.random.uniform(10, 1000, batch_size).round(2),
            'Quantity': np.random.randint(1, 10, batch_size),
            'Discount': np.random.choice([0, 0.1, 0.2, 0.3, 0.4, 0.5], batch_size),
            'Profit': np.random.uniform(-100, 500, batch_size).round(2)
        }
        df = pd.DataFrame(data)

    # Add batch metadata
    df['Batch'] = batch_num
    df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['Source'] = 'stream'

    return df

def save_batch(df, batch_num):
    """Save a batch of data to the stream directory."""
    # Create stream directory if it doesn't exist
    os.makedirs(STREAM_DIR, exist_ok=True)

    # Save as CSV
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    file_path = os.path.join(STREAM_DIR, f"batch_{batch_num}_{timestamp}.csv")
    df.to_csv(file_path, index=False)
    logger.info(f"Saved batch {batch_num} with {len(df)} records to {file_path}")

    return file_path

def main():
    """Main function."""
    args = parse_args()
    logger.info(f"Starting stream generator with interval={args.interval}s, batch_size={args.batch_size}")

    # Create stream directory if it doesn't exist
    os.makedirs(STREAM_DIR, exist_ok=True)

    # Generate and save batches
    batch_num = 1
    try:
        while True:
            # Check if max batches reached
            if args.max_batches > 0 and batch_num > args.max_batches:
                logger.info(f"Reached maximum number of batches ({args.max_batches})")
                break

            # Generate and save batch
            df = generate_batch(args.batch_size, batch_num)
            save_batch(df, batch_num)

            # Increment batch number
            batch_num += 1

            # Wait for next batch
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stream generator stopped by user")
    except Exception as e:
        logger.error(f"Error in stream generator: {e}")

if __name__ == '__main__':
    main()
