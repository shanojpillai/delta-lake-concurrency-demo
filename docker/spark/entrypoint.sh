#!/bin/bash

# Create necessary directories if they don't exist
mkdir -p /opt/spark/logs
mkdir -p /opt/spark/data/raw
mkdir -p /opt/spark/data/processed
mkdir -p /opt/spark/data/streaming
mkdir -p /opt/spark/data/stream_simulator
mkdir -p /opt/spark/data/batch_updates
mkdir -p /opt/spark/data/checkpoints

# Set permissions
chmod -R 777 /opt/spark/data
chmod -R 777 /opt/spark/logs

# Set Spark environment variables
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

# Find the py4j zip file and add it to PYTHONPATH
PY4J_ZIP=$(find $SPARK_HOME/python/lib -name "py4j-*.zip" | head -n 1)
if [ -n "$PY4J_ZIP" ]; then
    export PYTHONPATH=$PYTHONPATH:$PY4J_ZIP
fi

# Verify Spark installation
echo "Verifying Spark installation..."
ls -la $SPARK_HOME/bin
echo "Spark version:"
$SPARK_HOME/bin/spark-submit --version

# Start Jupyter notebook server
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' &

# Start Flask application
cd /opt/spark/app
python app.py &

# Print URLs for services
echo "Jupyter Notebook URL: http://localhost:8888"
echo "Spark UI URL: http://localhost:4040"
echo "Flask Web UI URL: http://localhost:5000"

# Keep container running
tail -f /dev/null
