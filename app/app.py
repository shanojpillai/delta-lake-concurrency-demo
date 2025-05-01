#!/usr/bin/env python3
"""
Flask web application for Delta Lake Concurrency Demo.
This app provides a UI for interacting with the Delta Lake demo and visualizing the results.
"""

import os
import sys
import json
import time
import subprocess
import pandas as pd
import numpy as np
from flask import Flask, render_template, request, jsonify, redirect, url_for
from werkzeug.serving import run_simple

# Add scripts directory to path
try:
    sys.path.append('/opt/spark/scripts')
    import utils
except Exception as e:
    print(f"Error importing utils: {e}")
    utils = None

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'delta-lake-demo-secret-key'

# Constants
SPARK_HOME = '/opt/spark'
NOTEBOOKS_DIR = os.path.join(SPARK_HOME, 'notebooks')
SCRIPTS_DIR = os.path.join(SPARK_HOME, 'scripts')
DATA_DIR = os.path.join(SPARK_HOME, 'data')
DELTA_TABLE_PATH = os.path.join(DATA_DIR, 'processed/global_superstore_delta')

# Notebook mapping
NOTEBOOK_MAPPING = {
    'setup': '01_setup.ipynb',
    'streaming': '02_streaming.ipynb',
    'concurrency': '03_concurrency.ipynb',
    'merge': '04_merge.ipynb',
    'optimize': '05_optimize.ipynb',
    'schema': '06_schema.ipynb',
    'timetravel': '07_timetravel.ipynb'
}

# Helper function to run a notebook
def run_notebook(notebook_path, parameters=None):
    """
    Run a Jupyter notebook using papermill and return the output path.

    Parameters:
    - notebook_path: Path to the notebook to run
    - parameters: Dictionary of parameters to pass to the notebook

    Returns:
    - Path to the output notebook
    """
    try:
        import papermill as pm

        # Check if the notebook exists
        if not os.path.exists(notebook_path):
            raise FileNotFoundError(f"Notebook not found at {notebook_path}")

        # Create output path
        output_dir = os.path.join(SPARK_HOME, 'notebook_outputs')
        os.makedirs(output_dir, exist_ok=True)

        # Generate output path
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        notebook_name = os.path.basename(notebook_path)
        output_path = os.path.join(output_dir, f"{notebook_name.split('.')[0]}_{timestamp}.ipynb")

        # Run the notebook
        print(f"Running notebook {notebook_path} with papermill...")
        pm.execute_notebook(
            notebook_path,
            output_path,
            parameters=parameters or {}
        )
        print(f"Notebook execution completed, output saved to {output_path}")

        return output_path
    except Exception as e:
        print(f"Error running notebook: {e}")
        raise

# Helper function to run a script
def run_script(script_path, args=None):
    """
    Run a Python script and return the output.

    Parameters:
    - script_path: Path to the script to run
    - args: List of arguments to pass to the script

    Returns:
    - Script output
    """
    try:
        # Check if the script exists
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script not found at {script_path}")

        # Make the script executable
        os.chmod(script_path, 0o755)

        cmd = [sys.executable, script_path]
        if args:
            cmd.extend(args)

        print(f"Running script {script_path} with command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Script execution failed with return code {result.returncode}")
            print(f"Error output: {result.stderr}")
            raise RuntimeError(f"Script execution failed: {result.stderr}")

        print(f"Script execution completed successfully")
        return result.stdout
    except Exception as e:
        print(f"Error running script: {e}")
        raise

# Routes
@app.route('/')
def index():
    """Render the main dashboard."""
    return render_template('index.html')

@app.route('/setup')
def setup():
    """Render the setup page."""
    return render_template('setup.html', notebook_path=NOTEBOOK_MAPPING.get('setup'))

@app.route('/streaming')
def streaming():
    """Render the streaming page."""
    return render_template('streaming.html', notebook_path=NOTEBOOK_MAPPING.get('streaming'))

@app.route('/concurrency')
def concurrency():
    """Render the concurrency page."""
    return render_template('concurrency.html', notebook_path=NOTEBOOK_MAPPING.get('concurrency'))

@app.route('/merge')
def merge():
    """Render the merge page."""
    return render_template('merge.html', notebook_path=NOTEBOOK_MAPPING.get('merge'))

@app.route('/optimize')
def optimize():
    """Render the optimize page."""
    return render_template('optimize.html', notebook_path=NOTEBOOK_MAPPING.get('optimize'))

@app.route('/schema')
def schema():
    """Render the schema evolution page."""
    return render_template('schema.html', notebook_path=NOTEBOOK_MAPPING.get('schema'))

@app.route('/timetravel')
def timetravel():
    """Render the time travel page."""
    return render_template('timetravel.html', notebook_path=NOTEBOOK_MAPPING.get('timetravel'))

@app.route('/readme')
def readme():
    """Render the README page."""
    return render_template('readme.html')

@app.route('/performance')
def performance():
    """Render the performance optimization page."""
    return render_template('performance.html')

@app.route('/notebook/<path:notebook_name>')
def open_notebook(notebook_name):
    """Redirect to the Jupyter notebook."""
    notebook_path = os.path.join(NOTEBOOKS_DIR, notebook_name)
    if os.path.exists(notebook_path):
        # Redirect to the Jupyter notebook server
        return redirect(f"http://localhost:8888/notebooks/{notebook_name}")
    else:
        return jsonify({
            'success': False,
            'error': f'Notebook not found: {notebook_name}'
        })

# API endpoints
@app.route('/api/table_info')
def get_table_info():
    """Get information about the Delta table."""
    try:
        # Import PySpark and Delta Lake
        from pyspark.sql import SparkSession

        # Create Spark session
        try:
            spark = utils.create_spark_session("Delta Lake UI")
        except Exception as e:
            print(f"Error creating Spark session: {e}")
            # Create a basic Spark session
            spark = SparkSession.builder \
                .appName("Delta Lake UI") \
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
                .getOrCreate()

        # Check if Delta table exists
        if not os.path.exists(DELTA_TABLE_PATH):
            return jsonify({
                'success': True,
                'metrics': {
                    'current_version': -1,
                    'record_count': 0,
                    'num_files': 0,
                    'size_in_bytes': 0,
                    'size_in_mb': 0
                },
                'history': []
            })

        # Get table metrics
        metrics = utils.log_delta_table_metrics(spark, DELTA_TABLE_PATH)

        # Get table history
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
            history = delta_table.history(10).toPandas().to_dict(orient='records')
        except Exception as e:
            print(f"Error getting table history: {e}")
            history = []

        return jsonify({
            'success': True,
            'metrics': metrics,
            'history': history
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/run_setup', methods=['POST'])
def run_setup_api():
    """Run the setup notebook."""
    try:
        # Check if download_dataset.py exists
        download_script = os.path.join(SCRIPTS_DIR, 'download_dataset.py')
        if not os.path.exists(download_script):
            print(f"Download script not found at {download_script}")
            return jsonify({
                'success': False,
                'error': f'Download script not found at {download_script}'
            })

        # Check if the setup notebook exists
        notebook_path = os.path.join(NOTEBOOKS_DIR, NOTEBOOK_MAPPING.get('setup'))
        if not os.path.exists(notebook_path):
            print(f"Setup notebook not found at {notebook_path}")
            return jsonify({
                'success': False,
                'error': f'Setup notebook not found at {notebook_path}'
            })

        # Run the download script
        print(f"Running download script: {download_script}")
        download_output = run_script(download_script)

        # Run the setup notebook
        print(f"Running setup notebook: {notebook_path}")
        output_path = run_notebook(notebook_path)

        return jsonify({
            'success': True,
            'message': 'Setup completed successfully',
            'download_output': download_output,
            'notebook_output': output_path
        })
    except Exception as e:
        print(f"Error in run_setup_api: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/start_streaming', methods=['POST'])
def start_streaming_api():
    """Start the streaming data generator."""
    try:
        # Get parameters from request
        data = request.json
        interval = data.get('interval', 5)
        batch_size = data.get('batch_size', 10)

        # Start the stream generator in the background
        script_path = os.path.join(SCRIPTS_DIR, 'stream_generator.py')

        # Make sure the script exists and is executable
        if not os.path.exists(script_path):
            return jsonify({
                'success': False,
                'error': f'Stream generator script not found at {script_path}'
            })

        # Make the script executable
        os.chmod(script_path, 0o755)

        # Use the full path to python and the script
        cmd = [
            '/usr/local/bin/python',
            script_path,
            '--interval', str(interval),
            '--batch-size', str(batch_size)
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Wait a moment for the process to start
        time.sleep(2)

        # Check if the process is running
        if process.poll() is None:
            return jsonify({
                'success': True,
                'message': f'Streaming data generator started with interval={interval}s, batch_size={batch_size}',
                'pid': process.pid
            })
        else:
            stdout, stderr = process.communicate()
            return jsonify({
                'success': False,
                'error': f'Failed to start streaming data generator: {stderr}'
            })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error starting streaming data generator: {str(e)}'
        })

@app.route('/api/stop_streaming', methods=['POST'])
def stop_streaming_api():
    """Stop the streaming data generator."""
    try:
        # Kill the stream generator process
        subprocess.run(['pkill', '-f', 'stream_generator.py'])

        return jsonify({
            'success': True,
            'message': 'Streaming data generator stopped'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/run_merge', methods=['POST'])
def run_merge_api():
    """Run a MERGE operation on the Delta table."""
    try:
        # Import PySpark and Delta Lake
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, rand, expr, current_timestamp
        from delta.tables import DeltaTable

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Get parameters from request
        data = request.json
        update_count = int(data.get('update_count', 50))
        insert_count = int(data.get('insert_count', 50))
        new_column = data.get('new_column', 'Customer_Satisfaction')
        new_column_value = data.get('new_column_value', '5')

        # Check if Delta table exists
        if not os.path.exists(DELTA_TABLE_PATH):
            return jsonify({
                'success': False,
                'error': 'Delta table does not exist. Please run the setup first.'
            })

        # Load the Delta table
        delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
        existing_df = delta_table.toDF()

        # Get a sample of records to update
        records_to_update = existing_df.limit(update_count)

        # Create a template for new records
        template_df = existing_df.limit(insert_count)

        # Modify the records to update
        records_to_update = records_to_update \
            .withColumn("Sales", col("Sales") * (rand() * 0.4 + 0.8)) \
            .withColumn("Profit", col("Profit") * (rand() * 0.4 + 0.8)) \
            .withColumn("Update_Source", lit("merge_update")) \
            .withColumn("Last_Updated", current_timestamp()) \
            .withColumn(new_column, lit(new_column_value))

        # Create new records
        new_records = template_df \
            .withColumn("Order_ID", expr("concat('NEW-', uuid())")) \
            .withColumn("Update_Source", lit("merge_insert")) \
            .withColumn("Last_Updated", current_timestamp()) \
            .withColumn(new_column, lit(new_column_value))

        # Combine updates and inserts into a single source DataFrame
        source_df = records_to_update.union(new_records)

        # Perform MERGE operation
        merge_result = delta_table.alias("target") \
            .merge(
                source_df.alias("source"),
                "target.Order_ID = source.Order_ID"
            ) \
            .whenMatchedUpdate(set={
                "Sales": "source.Sales",
                "Profit": "source.Profit",
                "Quantity": "source.Quantity",
                "Update_Source": "source.Update_Source",
                "Last_Updated": "source.Last_Updated",
                new_column: f"source.{new_column}"
            }) \
            .whenNotMatchedInsertAll() \
            .execute()

        # Get metrics
        metrics = {
            "updates": update_count,
            "inserts": insert_count,
            "unchanged": existing_df.count() - update_count
        }

        return jsonify({
            'success': True,
            'message': 'MERGE operation completed successfully',
            'results': metrics
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/run_batch_update', methods=['POST'])
def run_batch_update_api():
    """Run a batch update."""
    try:
        # Get parameters from request
        data = request.json
        interval = data.get('interval', 30)
        max_batches = data.get('max_batches', 1)
        update_pct = data.get('update_pct', 0.1)

        # Run the batch updater script
        script_path = os.path.join(SCRIPTS_DIR, 'batch_updater.py')

        # Make sure the script exists and is executable
        if not os.path.exists(script_path):
            return jsonify({
                'success': False,
                'error': f'Batch updater script not found at {script_path}'
            })

        # Make the script executable
        os.chmod(script_path, 0o755)

        cmd = [
            sys.executable, script_path,
            '--interval', str(interval),
            '--max-batches', str(max_batches),
            '--update-pct', str(update_pct)
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        return jsonify({
            'success': True,
            'message': 'Batch update completed',
            'output': result.stdout
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/run_optimize', methods=['POST'])
def run_optimize_api():
    """Run OPTIMIZE on the Delta table."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession

        # Create Spark session
        try:
            spark = utils.create_spark_session("Delta Lake UI")
        except Exception as e:
            print(f"Error creating Spark session: {e}")
            # Create a basic Spark session
            spark = SparkSession.builder \
                .appName("Delta Lake UI") \
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
                .getOrCreate()

        # Check if Delta table exists
        if not os.path.exists(DELTA_TABLE_PATH):
            return jsonify({
                'success': False,
                'error': 'Delta table does not exist. Please run the setup first.'
            })

        # Get metrics before optimization
        before_metrics = utils.monitor_file_metrics(spark, DELTA_TABLE_PATH, "before")

        # Run OPTIMIZE
        spark.sql(f"OPTIMIZE delta.`{DELTA_TABLE_PATH}`").show()

        # Get metrics after optimization
        after_metrics = utils.monitor_file_metrics(spark, DELTA_TABLE_PATH, "after")

        # Calculate improvement
        file_reduction = before_metrics["num_files"] - after_metrics["num_files"]
        file_reduction_pct = (file_reduction / before_metrics["num_files"]) * 100 if before_metrics["num_files"] > 0 else 0

        return jsonify({
            'success': True,
            'message': 'OPTIMIZE completed successfully',
            'before_metrics': before_metrics,
            'after_metrics': after_metrics,
            'file_reduction': file_reduction,
            'file_reduction_pct': file_reduction_pct
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/run_zorder', methods=['POST'])
def run_zorder_api():
    """Run Z-ORDER BY on the Delta table."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession

        # Check if utils is available
        if utils is None:
            return jsonify({
                'success': False,
                'error': 'Utils module not available. Please check the server logs.'
            })

        # Create Spark session
        try:
            spark = utils.create_spark_session("Delta Lake UI")
        except Exception as e:
            print(f"Error creating Spark session: {e}")
            # Create a basic Spark session
            spark = SparkSession.builder \
                .appName("Delta Lake UI") \
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
                .getOrCreate()

        # Check if Delta table exists
        if not os.path.exists(DELTA_TABLE_PATH):
            return jsonify({
                'success': False,
                'error': 'Delta table does not exist. Please run the setup first.'
            })

        # Get columns to Z-ORDER by from request
        data = request.json
        columns = data.get('columns', ['Order Date', 'Customer ID', 'Product ID'])
        columns_str = '`' + '`, `'.join(columns) + '`'

        # Run Z-ORDER
        spark.sql(f"OPTIMIZE delta.`{DELTA_TABLE_PATH}` ZORDER BY ({columns_str})").show()

        return jsonify({
            'success': True,
            'message': f'Z-ORDER BY {", ".join(columns)} completed successfully'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/add_column', methods=['POST'])
def add_column_api():
    """Add a new column to the Delta table."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import lit

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Get column details from request
        data = request.json
        column_name = data.get('column_name')
        column_value = data.get('column_value')

        if not column_name:
            return jsonify({
                'success': False,
                'error': 'Column name is required'
            })

        # Read the Delta table
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)

        # Add the new column
        df_with_column = df.withColumn(column_name, lit(column_value))

        # Write back to the Delta table
        df_with_column.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(DELTA_TABLE_PATH)

        return jsonify({
            'success': True,
            'message': f'Added column {column_name} with value {column_value}'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/widen_data_types', methods=['POST'])
def widen_data_types_api():
    """Widen data types in the Delta table."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit
        from pyspark.sql.types import LongType, DoubleType, DecimalType

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Read the Delta table
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)

        # Get the schema
        schema = df.schema

        # Track which columns were widened
        widened_columns = []

        # Create a new DataFrame with widened columns
        widened_df = df

        # Check each column for potential widening
        for field in schema.fields:
            # Check if the column is a numeric type that can be widened
            if field.dataType.typeName() == "integer":
                # Widen integer to long
                widened_df = widened_df.withColumn(field.name, col(field.name).cast(LongType()))
                widened_columns.append(f"{field.name} (integer → long)")
            elif field.dataType.typeName() == "float":
                # Widen float to double
                widened_df = widened_df.withColumn(field.name, col(field.name).cast(DoubleType()))
                widened_columns.append(f"{field.name} (float → double)")
            elif field.dataType.typeName() == "decimal":
                # Widen decimal precision
                current_precision = field.dataType.precision
                current_scale = field.dataType.scale
                if current_precision < 18:
                    new_precision = 18
                    new_scale = min(current_scale + 2, 6)  # Increase scale but cap at 6
                    widened_df = widened_df.withColumn(field.name, col(field.name).cast(DecimalType(new_precision, new_scale)))
                    widened_columns.append(f"{field.name} (decimal({current_precision},{current_scale}) → decimal({new_precision},{new_scale}))")

        # Add a marker column to indicate the schema version
        widened_df = widened_df.withColumn("Schema_Version", lit("widened"))

        # If no columns were widened, return a message
        if not widened_columns:
            return jsonify({
                'success': True,
                'message': 'No columns were eligible for widening'
            })

        # Write back to the Delta table
        widened_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(DELTA_TABLE_PATH)

        return jsonify({
            'success': True,
            'message': f'Widened data types for columns: {", ".join(widened_columns)}'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/add_nested_structure', methods=['POST'])
def add_nested_structure_api():
    """Add a nested structure to the Delta table."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, struct, lit

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Read the Delta table
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)

        # Check if required columns exist
        required_columns = ["Customer_ID", "Segment", "City", "State", "Country", "Postal_Code"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            return jsonify({
                'success': False,
                'error': f'Missing required columns: {", ".join(missing_columns)}'
            })

        # Add nested structure
        nested_df = df.withColumn(
            "Customer_Details",
            struct(
                col("Customer_ID").alias("ID"),
                col("Segment").alias("Segment"),
                struct(
                    col("City").alias("City"),
                    col("State").alias("State"),
                    col("Country").alias("Country"),
                    col("Postal_Code").alias("Postal_Code")
                ).alias("Address")
            )
        ).withColumn("Schema_Version", lit("nested"))

        # Write back to the Delta table
        nested_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(DELTA_TABLE_PATH)

        return jsonify({
            'success': True,
            'message': 'Added nested Customer_Details structure with Address sub-structure'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/get_schema', methods=['GET'])
def get_schema_api():
    """Get the schema of the Delta table."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Check if Delta table exists
        if not os.path.exists(DELTA_TABLE_PATH):
            return jsonify({
                'success': False,
                'error': 'Delta table does not exist. Please run the setup first.'
            })

        # Read the Delta table
        df = spark.read.format("delta").load(DELTA_TABLE_PATH)

        # Get the schema
        schema = []
        for field in df.schema.fields:
            schema.append({
                'name': field.name,
                'type': field.dataType.simpleString()
            })

        return jsonify({
            'success': True,
            'schema': schema
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/write_bad_data', methods=['POST'])
def write_bad_data_api():
    """Write bad data to the Delta table for time travel demo."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import lit

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Generate corrupted data
        corrupted_data = utils.generate_test_data(num_records=100, scenario='corrupted')
        corrupted_df = spark.createDataFrame(corrupted_data)

        # Add a marker to identify the corrupted data
        corrupted_df = corrupted_df.withColumn("Data_Quality", lit("corrupted"))

        # Write the corrupted data to the Delta table
        corrupted_df.write.format("delta").mode("append").save(DELTA_TABLE_PATH)

        return jsonify({
            'success': True,
            'message': 'Wrote 100 corrupted records to the Delta table'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/rollback', methods=['POST'])
def rollback_api():
    """Rollback the Delta table to a previous version."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Get version to rollback to from request
        data = request.json
        version = data.get('version')

        if version is None:
            return jsonify({
                'success': False,
                'error': 'Version is required'
            })

        # Perform the rollback
        spark.sql(f"RESTORE TABLE delta.`{DELTA_TABLE_PATH}` VERSION AS OF {version}")

        return jsonify({
            'success': True,
            'message': f'Rolled back to version {version}'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/query_version', methods=['POST'])
def query_version_api():
    """Query the Delta table at a specific version."""
    try:
        # Import PySpark
        from pyspark.sql import SparkSession

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Get version to query from request
        data = request.json
        version = data.get('version')

        if version is None:
            return jsonify({
                'success': False,
                'error': 'Version is required'
            })

        # Query the table at the specific version
        df = spark.read.format("delta").option("versionAsOf", version).load(DELTA_TABLE_PATH)

        # Get basic stats
        count = df.count()
        schema = [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]

        # Get a sample of the data
        sample = df.limit(10).toPandas().to_dict(orient='records')

        return jsonify({
            'success': True,
            'version': version,
            'count': count,
            'schema': schema,
            'sample': sample
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/performance_history', methods=['GET'])
def performance_history_api():
    """Get performance history."""
    try:
        # Import performance module
        sys.path.append('/opt/spark/scripts')
        import performance

        # Get performance history
        history = performance.get_performance_history()

        return jsonify({
            'success': True,
            'history': history
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/recommend_optimizations', methods=['GET'])
def recommend_optimizations_api():
    """Get optimization recommendations."""
    try:
        # Import PySpark and performance module
        from pyspark.sql import SparkSession
        sys.path.append('/opt/spark/scripts')
        import performance

        # Create Spark session
        spark = utils.create_spark_session("Delta Lake UI")

        # Get recommendations
        results = performance.recommend_optimizations(spark, DELTA_TABLE_PATH)

        return jsonify({
            'success': True,
            'recommendations': results.get('recommendations', []),
            'metrics': results.get('metrics', {})
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        })

if __name__ == '__main__':
    # Run the app
    run_simple('0.0.0.0', 5000, app, use_reloader=True, use_debugger=True)
