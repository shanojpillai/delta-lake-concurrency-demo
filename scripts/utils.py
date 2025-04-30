#!/usr/bin/env python3
"""
Utility functions for Delta Lake operations.
This module provides:
1. Functions to generate test data for different scenarios
2. Helper functions for logging Delta table metrics and statistics
3. Utilities for benchmarking query performance
4. Functions to visualize Delta Lake transactions and versions
5. Helpers for monitoring file counts and sizes in Delta tables
"""

import os
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set SPARK_HOME environment variable
os.environ['SPARK_HOME'] = '/usr/local/lib/python3.10/site-packages/pyspark'

# Constants
DELTA_TABLE_PATH = "/opt/spark/data/processed/global_superstore_delta"

def create_spark_session(app_name="Delta Lake Utils"):
    """Create a Spark session with Delta Lake configuration."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession
        
        # Create a Spark session with Delta Lake support
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .getOrCreate()

        logger.info(f"Created Spark session for {app_name}")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session with Delta Lake support: {e}")
        logger.info("Creating basic Spark session without Delta Lake support...")

        # Import PySpark
        from pyspark.sql import SparkSession
        
        # Create a basic Spark session without Delta Lake support
        spark = SparkSession.builder \
            .appName(f"{app_name} (Basic)") \
            .getOrCreate()

        logger.info(f"Created basic Spark session for {app_name}")
        return spark

def generate_test_data(num_records=1000, scenario='normal'):
    """
    Generate test data for different scenarios.

    Parameters:
    - num_records: Number of records to generate
    - scenario: Type of data to generate ('normal', 'corrupted', 'schema_change')

    Returns:
    - Pandas DataFrame with generated data
    """
    # Base data generation with underscores instead of spaces in column names
    data = {
        'Row_ID': range(1, num_records + 1),
        'Order_ID': [f"TEST-{np.random.randint(10000, 99999)}" for _ in range(num_records)],
        'Order_Date': [
            (datetime.now() - timedelta(days=np.random.randint(0, 365))).strftime('%Y-%m-%d')
            for _ in range(num_records)
        ],
        'Ship_Date': [
            (datetime.now() - timedelta(days=np.random.randint(0, 30))).strftime('%Y-%m-%d')
            for _ in range(num_records)
        ],
        'Ship_Mode': np.random.choice(['Standard Class', 'First Class', 'Second Class', 'Same Day'], num_records),
        'Customer_ID': [f"CG-{np.random.randint(10000, 99999)}" for _ in range(num_records)],
        'Customer_Name': [f"Test-Customer-{i}" for i in range(num_records)],
        'Segment': np.random.choice(['Consumer', 'Corporate', 'Home Office'], num_records),
        'Country': ['United States'] * num_records,
        'City': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], num_records),
        'State': np.random.choice(['New York', 'California', 'Illinois', 'Texas', 'Arizona'], num_records),
        'Postal_Code': np.random.choice(['10001', '90001', '60601', '77001', '85001'], num_records),
        'Region': np.random.choice(['East', 'West', 'Central', 'South'], num_records),
        'Product_ID': [f"PROD-{np.random.randint(10000, 99999)}" for _ in range(num_records)],
        'Category': np.random.choice(['Furniture', 'Office Supplies', 'Technology'], num_records),
        'Sub_Category': np.random.choice(['Chairs', 'Tables', 'Phones', 'Storage', 'Binders'], num_records),
        'Product_Name': [f"Product {i}" for i in range(num_records)],
        'Sales': np.random.uniform(10, 1000, num_records).round(2),
        'Quantity': np.random.randint(1, 10, num_records),
        'Discount': np.random.choice([0, 0.1, 0.2, 0.3, 0.4, 0.5], num_records),
        'Profit': np.random.uniform(-100, 500, num_records).round(2)
    }

    # Modify data based on scenario
    if scenario == 'corrupted':
        # Add negative sales and quantities
        corrupt_indices = np.random.choice(range(num_records), size=int(num_records * 0.2), replace=False)
        for idx in corrupt_indices:
            data['Sales'][idx] = -abs(data['Sales'][idx])
            data['Quantity'][idx] = -abs(data['Quantity'][idx])
        logger.info(f"Generated corrupted test data with {len(corrupt_indices)} corrupted records")

    elif scenario == 'schema_change':
        # Add new columns
        data['Customer_Feedback'] = np.random.choice(['Excellent', 'Good', 'Average', 'Poor'], num_records)
        data['Delivery_Rating'] = np.random.randint(1, 6, num_records)
        logger.info("Generated test data with schema changes (added Customer_Feedback and Delivery_Rating)")

    else:
        logger.info(f"Generated normal test data with {num_records} records")

    return pd.DataFrame(data)

def log_delta_table_metrics(spark, table_path=DELTA_TABLE_PATH):
    """
    Log metrics about a Delta table.

    Parameters:
    - spark: SparkSession
    - table_path: Path to the Delta table

    Returns:
    - Dictionary with table metrics
    """
    try:
        # Check if table exists
        if not os.path.exists(table_path):
            logger.error(f"Delta table not found at {table_path}")
            return {
                "table_path": table_path,
                "current_version": -1,
                "record_count": 0,
                "partition_columns": [],
                "num_files": 0,
                "size_in_bytes": 0,
                "size_in_mb": 0
            }

        # Load the Delta table
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(spark, table_path)

            # Get table details
            history = delta_table.history()
            current_version = history.select("version").orderBy("version").first()[0]

            # Count records
            df = spark.read.format("delta").load(table_path)
            record_count = df.count()

            # Get partition information
            partition_cols = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").select("partitionColumns").first()[0]

            # Get file information
            files_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
            num_files = files_df.select("numFiles").first()[0]
            size_in_bytes = files_df.select("sizeInBytes").first()[0]

            # Compile metrics
            metrics = {
                "table_path": table_path,
                "current_version": current_version,
                "record_count": record_count,
                "partition_columns": partition_cols,
                "num_files": num_files,
                "size_in_bytes": size_in_bytes,
                "size_in_mb": size_in_bytes / (1024 * 1024)
            }

            # Log the metrics
            logger.info(f"Delta table metrics: version={current_version}, records={record_count}, "
                       f"files={num_files}, size={metrics['size_in_mb']:.2f} MB")

            return metrics
        except Exception as e:
            logger.error(f"Error loading Delta table: {e}")
            return {
                "table_path": table_path,
                "current_version": -1,
                "record_count": 0,
                "partition_columns": [],
                "num_files": 0,
                "size_in_bytes": 0,
                "size_in_mb": 0
            }

    except Exception as e:
        logger.error(f"Error getting Delta table metrics: {e}")
        return {
            "table_path": table_path,
            "current_version": -1,
            "record_count": 0,
            "partition_columns": [],
            "num_files": 0,
            "size_in_bytes": 0,
            "size_in_mb": 0
        }

def monitor_file_metrics(spark, table_path=DELTA_TABLE_PATH, before_after=None):
    """
    Monitor file counts and sizes in Delta tables.

    Parameters:
    - spark: SparkSession
    - table_path: Path to the Delta table
    - before_after: Label for the metrics ('before' or 'after')

    Returns:
    - Dictionary with file metrics
    """
    try:
        # Check if table exists
        if not os.path.exists(table_path):
            logger.error(f"Delta table not found at {table_path}")
            return {
                "num_files": 0,
                "size_in_bytes": 0,
                "size_in_mb": 0,
                "timestamp": datetime.now()
            }

        # Get file information
        try:
            files_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
            num_files = files_df.select("numFiles").first()[0]
            size_in_bytes = files_df.select("sizeInBytes").first()[0]

            # Compile metrics
            metrics = {
                "num_files": num_files,
                "size_in_bytes": size_in_bytes,
                "size_in_mb": size_in_bytes / (1024 * 1024),
                "timestamp": datetime.now()
            }

            # Log with before/after label if provided
            label = f" ({before_after})" if before_after else ""
            logger.info(f"Delta table file metrics{label}: files={num_files}, "
                       f"size={metrics['size_in_mb']:.2f} MB")

            return metrics
        except Exception as e:
            logger.error(f"Error getting file metrics: {e}")
            return {
                "num_files": 0,
                "size_in_bytes": 0,
                "size_in_mb": 0,
                "timestamp": datetime.now()
            }

    except Exception as e:
        logger.error(f"Error monitoring file metrics: {e}")
        return {
            "num_files": 0,
            "size_in_bytes": 0,
            "size_in_mb": 0,
            "timestamp": datetime.now()
        }
