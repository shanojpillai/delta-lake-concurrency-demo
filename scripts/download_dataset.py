#!/usr/bin/env python3
"""
Download and prepare the Global Superstore dataset for the Delta Lake demo.
If the dataset is not available, generate a sample dataset.
"""

import os
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

# Constants
DATA_DIR = "/opt/spark/data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
RAW_DATA_PATH = os.path.join(RAW_DATA_DIR, "Global_Superstore.csv")

def generate_sample_data(num_records=1000):
    """
    Generate sample data for the Global Superstore dataset.
    
    Parameters:
    - num_records: Number of records to generate
    
    Returns:
    - Pandas DataFrame with generated data
    """
    logger.info(f"Generating sample data with {num_records} records")
    
    # Generate random dates within the last year
    start_date = datetime.now() - timedelta(days=365)
    
    # Base data generation
    data = {
        'Row ID': range(1, num_records + 1),
        'Order ID': [f"ORD-{i:05d}" for i in range(1, num_records + 1)],
        'Order Date': [(start_date + timedelta(days=np.random.randint(1, 365))).strftime('%Y-%m-%d') for _ in range(num_records)],
        'Ship Date': [(start_date + timedelta(days=np.random.randint(5, 375))).strftime('%Y-%m-%d') for _ in range(num_records)],
        'Ship Mode': np.random.choice(['Standard Class', 'First Class', 'Second Class', 'Same Day'], num_records),
        'Customer ID': [f"CUST-{i:05d}" for i in range(1, num_records + 1)],
        'Customer Name': [f"Customer {i}" for i in range(1, num_records + 1)],
        'Segment': np.random.choice(['Consumer', 'Corporate', 'Home Office'], num_records),
        'Country': np.random.choice(['United States', 'Canada', 'Mexico', 'United Kingdom', 'France', 'Germany', 'Australia', 'China', 'India'], num_records),
        'City': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Toronto', 'London', 'Paris', 'Berlin', 'Sydney', 'Beijing', 'Mumbai'], num_records),
        'State': np.random.choice(['New York', 'California', 'Illinois', 'Texas', 'Arizona', 'Ontario', 'England', 'Ile-de-France', 'Berlin', 'New South Wales', 'Beijing', 'Maharashtra'], num_records),
        'Postal Code': np.random.choice(['10001', '90001', '60601', '77001', '85001', 'M5V 2A8', 'SW1A 1AA', '75001', '10115', '2000', '100000', '400001'], num_records),
        'Region': np.random.choice(['East', 'West', 'Central', 'South', 'North', 'Europe', 'Asia Pacific'], num_records),
        'Product ID': [f"PROD-{i:05d}" for i in range(1, num_records + 1)],
        'Category': np.random.choice(['Furniture', 'Office Supplies', 'Technology'], num_records),
        'Sub-Category': np.random.choice(['Chairs', 'Tables', 'Phones', 'Storage', 'Binders', 'Appliances', 'Accessories', 'Copiers', 'Bookcases'], num_records),
        'Product Name': [f"Product {i}" for i in range(1, num_records + 1)],
        'Sales': np.random.uniform(10, 1000, num_records).round(2),
        'Quantity': np.random.randint(1, 10, num_records),
        'Discount': np.random.choice([0, 0.1, 0.2, 0.3, 0.4, 0.5], num_records),
        'Profit': np.random.uniform(-100, 500, num_records).round(2)
    }
    
    logger.info(f"Sample data generation completed")
    return pd.DataFrame(data)

def main():
    """Main function to download or generate the dataset."""
    # Create data directory
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    logger.info(f"Data directory: {RAW_DATA_DIR}")
    
    # Check if the dataset already exists
    if os.path.exists(RAW_DATA_PATH):
        logger.info(f"Dataset already exists at {RAW_DATA_PATH}")
        try:
            # Load the dataset to verify it
            df = pd.read_csv(RAW_DATA_PATH)
            logger.info(f"Loaded existing dataset with {len(df)} records")
            return
        except Exception as e:
            logger.error(f"Error loading existing dataset: {e}")
            logger.info("Will generate a new dataset")
    
    # Generate sample data
    df = generate_sample_data(1000)
    
    # Save the dataset
    df.to_csv(RAW_DATA_PATH, index=False)
    logger.info(f"Saved dataset to {RAW_DATA_PATH}")

if __name__ == "__main__":
    main()
