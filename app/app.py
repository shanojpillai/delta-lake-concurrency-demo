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

if __name__ == '__main__':
    # Run the app
    run_simple('0.0.0.0', 5000, app, use_reloader=True, use_debugger=True)
