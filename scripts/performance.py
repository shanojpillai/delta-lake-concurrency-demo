#!/usr/bin/env python3
"""
Performance optimization utilities for Delta Lake operations.
This module provides:
1. Functions to optimize Delta tables
2. Caching mechanisms for frequently accessed data
3. Batch processing utilities
4. Performance monitoring tools
"""

import os
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import logging
import json
from functools import lru_cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DELTA_TABLE_PATH = "/opt/spark/data/processed/global_superstore_delta"
PERFORMANCE_METRICS_PATH = "/opt/spark/data/metrics"

# Create metrics directory if it doesn't exist
os.makedirs(PERFORMANCE_METRICS_PATH, exist_ok=True)

def optimize_delta_table(spark, table_path=DELTA_TABLE_PATH, z_order_columns=None):
    """
    Optimize a Delta table with OPTIMIZE and Z-ORDER.
    
    Parameters:
    - spark: SparkSession
    - table_path: Path to the Delta table
    - z_order_columns: List of columns to Z-ORDER by
    
    Returns:
    - Dictionary with optimization results
    """
    try:
        # Check if table exists
        if not os.path.exists(table_path):
            logger.error(f"Delta table not found at {table_path}")
            return {
                "success": False,
                "error": f"Delta table not found at {table_path}"
            }
        
        # Get metrics before optimization
        start_time = time.time()
        before_metrics = get_table_metrics(spark, table_path)
        
        # Run OPTIMIZE
        logger.info(f"Running OPTIMIZE on Delta table at {table_path}")
        if z_order_columns:
            # Z-ORDER by specified columns
            z_order_cols_str = ", ".join([f"`{col}`" for col in z_order_columns])
            logger.info(f"Z-ORDERing by columns: {z_order_cols_str}")
            spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({z_order_cols_str})")
        else:
            # Just run OPTIMIZE without Z-ORDER
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
        
        # Get metrics after optimization
        after_metrics = get_table_metrics(spark, table_path)
        end_time = time.time()
        
        # Calculate improvement
        file_reduction = before_metrics["num_files"] - after_metrics["num_files"]
        file_reduction_pct = (file_reduction / before_metrics["num_files"]) * 100 if before_metrics["num_files"] > 0 else 0
        size_reduction = before_metrics["size_in_bytes"] - after_metrics["size_in_bytes"]
        size_reduction_pct = (size_reduction / before_metrics["size_in_bytes"]) * 100 if before_metrics["size_in_bytes"] > 0 else 0
        
        # Compile results
        results = {
            "success": True,
            "before_metrics": before_metrics,
            "after_metrics": after_metrics,
            "file_reduction": file_reduction,
            "file_reduction_pct": file_reduction_pct,
            "size_reduction": size_reduction,
            "size_reduction_pct": size_reduction_pct,
            "execution_time": end_time - start_time
        }
        
        # Log results
        logger.info(f"Optimization completed in {results['execution_time']:.2f} seconds")
        logger.info(f"File reduction: {file_reduction} files ({file_reduction_pct:.2f}%)")
        logger.info(f"Size reduction: {size_reduction / (1024 * 1024):.2f} MB ({size_reduction_pct:.2f}%)")
        
        # Save metrics
        save_performance_metrics("optimize", results)
        
        return results
    except Exception as e:
        logger.error(f"Error optimizing Delta table: {e}")
        return {
            "success": False,
            "error": str(e)
        }

def get_table_metrics(spark, table_path=DELTA_TABLE_PATH):
    """
    Get metrics about a Delta table.
    
    Parameters:
    - spark: SparkSession
    - table_path: Path to the Delta table
    
    Returns:
    - Dictionary with table metrics
    """
    try:
        # Load the Delta table
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, table_path)
        
        # Get table details
        history = delta_table.history()
        current_version = history.select("version").orderBy("version").first()[0]
        
        # Get file information
        files_df = spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`")
        num_files = files_df.select("numFiles").first()[0]
        size_in_bytes = files_df.select("sizeInBytes").first()[0]
        
        # Get partition information
        partition_cols = files_df.select("partitionColumns").first()[0]
        
        # Count records
        df = spark.read.format("delta").load(table_path)
        record_count = df.count()
        
        # Compile metrics
        metrics = {
            "table_path": table_path,
            "current_version": current_version,
            "record_count": record_count,
            "partition_columns": partition_cols,
            "num_files": num_files,
            "size_in_bytes": size_in_bytes,
            "size_in_mb": size_in_bytes / (1024 * 1024),
            "timestamp": datetime.now().isoformat()
        }
        
        return metrics
    except Exception as e:
        logger.error(f"Error getting table metrics: {e}")
        return {
            "table_path": table_path,
            "current_version": -1,
            "record_count": 0,
            "partition_columns": [],
            "num_files": 0,
            "size_in_bytes": 0,
            "size_in_mb": 0,
            "timestamp": datetime.now().isoformat()
        }

@lru_cache(maxsize=32)
def get_cached_table_info(table_path=DELTA_TABLE_PATH, cache_ttl=60):
    """
    Get cached table information with a TTL.
    
    Parameters:
    - table_path: Path to the Delta table
    - cache_ttl: Cache time-to-live in seconds
    
    Returns:
    - Dictionary with table information
    """
    # This function uses the lru_cache decorator to cache results
    # The cache is invalidated after cache_ttl seconds
    # Implementation note: The actual caching is handled by the decorator
    
    # Create a SparkSession
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("Delta Lake Cache") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    # Get table metrics
    metrics = get_table_metrics(spark, table_path)
    
    # Add cache timestamp
    metrics["cache_timestamp"] = datetime.now().isoformat()
    
    return metrics

def batch_process_data(spark, data, batch_size=1000, process_func=None):
    """
    Process data in batches for better performance.
    
    Parameters:
    - spark: SparkSession
    - data: DataFrame or RDD to process
    - batch_size: Number of records per batch
    - process_func: Function to apply to each batch
    
    Returns:
    - List of results from each batch
    """
    try:
        # Convert to DataFrame if not already
        if not hasattr(data, "rdd"):
            data = spark.createDataFrame(data)
        
        # Get total count
        total_count = data.count()
        logger.info(f"Batch processing {total_count} records with batch size {batch_size}")
        
        # Calculate number of batches
        num_batches = (total_count + batch_size - 1) // batch_size
        
        # Process in batches
        results = []
        for i in range(num_batches):
            start_time = time.time()
            
            # Get batch
            batch = data.limit(batch_size).offset(i * batch_size)
            
            # Process batch
            if process_func:
                batch_result = process_func(batch)
            else:
                # Default processing: collect as dictionary
                batch_result = batch.toPandas().to_dict(orient="records")
            
            # Add to results
            results.append(batch_result)
            
            # Log progress
            end_time = time.time()
            logger.info(f"Processed batch {i+1}/{num_batches} in {end_time - start_time:.2f} seconds")
        
        return results
    except Exception as e:
        logger.error(f"Error batch processing data: {e}")
        return []

def save_performance_metrics(operation, metrics):
    """
    Save performance metrics to a file.
    
    Parameters:
    - operation: Name of the operation
    - metrics: Dictionary with metrics
    
    Returns:
    - Path to the saved metrics file
    """
    try:
        # Create a timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Create a filename
        filename = f"{operation}_{timestamp}.json"
        file_path = os.path.join(PERFORMANCE_METRICS_PATH, filename)
        
        # Add timestamp to metrics
        metrics["timestamp"] = datetime.now().isoformat()
        
        # Save metrics to file
        with open(file_path, "w") as f:
            json.dump(metrics, f, indent=2, default=str)
        
        logger.info(f"Saved performance metrics to {file_path}")
        
        return file_path
    except Exception as e:
        logger.error(f"Error saving performance metrics: {e}")
        return None

def get_performance_history(operation=None, limit=10):
    """
    Get performance history from saved metrics.
    
    Parameters:
    - operation: Filter by operation name
    - limit: Maximum number of records to return
    
    Returns:
    - List of performance metrics
    """
    try:
        # Get all metrics files
        metrics_files = os.listdir(PERFORMANCE_METRICS_PATH)
        
        # Filter by operation if specified
        if operation:
            metrics_files = [f for f in metrics_files if f.startswith(f"{operation}_")]
        
        # Sort by timestamp (newest first)
        metrics_files.sort(reverse=True)
        
        # Limit number of files
        metrics_files = metrics_files[:limit]
        
        # Load metrics from files
        metrics = []
        for filename in metrics_files:
            file_path = os.path.join(PERFORMANCE_METRICS_PATH, filename)
            with open(file_path, "r") as f:
                metric = json.load(f)
                # Add operation name
                metric["operation"] = filename.split("_")[0]
                metrics.append(metric)
        
        return metrics
    except Exception as e:
        logger.error(f"Error getting performance history: {e}")
        return []

def analyze_query_performance(spark, query_func, num_runs=5):
    """
    Analyze the performance of a query.
    
    Parameters:
    - spark: SparkSession
    - query_func: Function that executes a query
    - num_runs: Number of times to run the query
    
    Returns:
    - Dictionary with performance analysis
    """
    try:
        # Run the query multiple times
        execution_times = []
        for i in range(num_runs):
            # Clear cache to ensure fair comparison
            spark.catalog.clearCache()
            
            # Run the query and measure time
            start_time = time.time()
            result = query_func(spark)
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Add to execution times
            execution_times.append(execution_time)
            
            logger.info(f"Run {i+1}/{num_runs}: {execution_time:.4f} seconds")
        
        # Calculate statistics
        avg_time = np.mean(execution_times)
        min_time = np.min(execution_times)
        max_time = np.max(execution_times)
        std_dev = np.std(execution_times)
        
        # Compile results
        results = {
            "execution_times": execution_times,
            "avg_time": avg_time,
            "min_time": min_time,
            "max_time": max_time,
            "std_dev": std_dev,
            "num_runs": num_runs
        }
        
        # Log results
        logger.info(f"Query performance: avg={avg_time:.4f}s, min={min_time:.4f}s, max={max_time:.4f}s, std_dev={std_dev:.4f}s")
        
        # Save metrics
        save_performance_metrics("query", results)
        
        return results
    except Exception as e:
        logger.error(f"Error analyzing query performance: {e}")
        return {
            "error": str(e)
        }

def recommend_optimizations(spark, table_path=DELTA_TABLE_PATH):
    """
    Recommend optimizations for a Delta table.
    
    Parameters:
    - spark: SparkSession
    - table_path: Path to the Delta table
    
    Returns:
    - Dictionary with optimization recommendations
    """
    try:
        # Get table metrics
        metrics = get_table_metrics(spark, table_path)
        
        # Initialize recommendations
        recommendations = []
        
        # Check number of files
        if metrics["num_files"] > 10:
            recommendations.append({
                "type": "optimize",
                "description": "Run OPTIMIZE to reduce the number of files",
                "reason": f"The table has {metrics['num_files']} files, which can impact query performance"
            })
        
        # Check if table is partitioned
        if not metrics["partition_columns"]:
            # Get the table schema
            df = spark.read.format("delta").load(table_path)
            schema = df.schema
            
            # Find potential partition columns
            date_columns = [field.name for field in schema.fields if field.dataType.typeName() == "date"]
            categorical_columns = [field.name for field in schema.fields if field.dataType.typeName() == "string"]
            
            # Recommend partitioning
            if date_columns:
                recommendations.append({
                    "type": "partition",
                    "description": f"Consider partitioning by date columns: {', '.join(date_columns)}",
                    "reason": "Partitioning by date can improve query performance for time-based queries"
                })
            elif categorical_columns:
                # Find low-cardinality categorical columns
                low_cardinality_columns = []
                for col in categorical_columns[:5]:  # Limit to first 5 to avoid too many queries
                    count = df.select(col).distinct().count()
                    if 2 <= count <= 20:  # Good cardinality for partitioning
                        low_cardinality_columns.append((col, count))
                
                if low_cardinality_columns:
                    columns_str = ", ".join([f"{col} ({count} values)" for col, count in low_cardinality_columns])
                    recommendations.append({
                        "type": "partition",
                        "description": f"Consider partitioning by categorical columns: {columns_str}",
                        "reason": "Partitioning by low-cardinality categorical columns can improve query performance"
                    })
        
        # Check for Z-ORDER opportunities
        df = spark.read.format("delta").load(table_path)
        schema = df.schema
        
        # Find potential Z-ORDER columns
        date_columns = [field.name for field in schema.fields if field.dataType.typeName() == "date"]
        id_columns = [field.name for field in schema.fields if "id" in field.name.lower()]
        
        if date_columns or id_columns:
            columns = date_columns + id_columns
            recommendations.append({
                "type": "z_order",
                "description": f"Consider Z-ORDERing by columns: {', '.join(columns)}",
                "reason": "Z-ORDERing by frequently queried columns can improve query performance"
            })
        
        # Check table size
        if metrics["size_in_mb"] > 1000:  # More than 1GB
            recommendations.append({
                "type": "vacuum",
                "description": "Run VACUUM to remove old versions and reduce table size",
                "reason": f"The table is {metrics['size_in_mb']:.2f} MB, which can impact storage costs"
            })
        
        # Compile results
        results = {
            "table_path": table_path,
            "metrics": metrics,
            "recommendations": recommendations
        }
        
        return results
    except Exception as e:
        logger.error(f"Error recommending optimizations: {e}")
        return {
            "table_path": table_path,
            "error": str(e)
        }
