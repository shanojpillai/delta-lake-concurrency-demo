"""
Delta Lake Concurrency Demo Architecture Diagram Generator

This script generates an architecture diagram for the Delta Lake Concurrency Demo
using the diagrams Python package.

Requirements:
- Python 3.6+
- diagrams package (pip install diagrams)
- Graphviz (https://graphviz.org/download/)

To generate the diagram:
$ python architecture_diagram.py
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.analytics import Spark, Dbt
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.queue import Kafka
from diagrams.onprem.container import Docker
from diagrams.programming.framework import Flask, React
from diagrams.programming.language import Python
from diagrams.onprem.client import User
from diagrams.generic.storage import Storage
from diagrams.generic.compute import Rack
from diagrams.generic.network import Firewall
from diagrams.generic.place import Datacenter

# Define custom icons for Delta Lake components
from diagrams.custom import Custom

# Set the output file path
output_path = "delta_lake_architecture"

# Create the diagram
with Diagram("Delta Lake Concurrency Demo Architecture", show=False, filename=output_path, direction="TB"):

    # User Interface Layer
    with Cluster("User Interface Layer"):
        web_ui = Flask("Web UI\n(Port 5000)")
        jupyter = User("Jupyter Notebooks\n(Port 8888)")
        spark_ui = Custom("Spark UI\n(Port 4040)", "./custom_icons/spark.png")

        user_interfaces = [web_ui, jupyter, spark_ui]

    # Application Layer
    with Cluster("Application Layer"):
        flask_api = Flask("Flask API Server")
        jupyter_app = User("Jupyter Notebooks")
        delta_api = Custom("Delta Lake API", "./custom_icons/delta.png")

        application_components = [flask_api, jupyter_app, delta_api]

    # Processing Layer
    with Cluster("Processing Layer"):
        with Cluster("Data Processing"):
            batch = Spark("Batch Processing")
            streaming = Spark("Streaming Processing")
            merge_ops = Custom("MERGE Operations", "./custom_icons/delta.png")
            schema_evolution = Custom("Schema Evolution", "./custom_icons/delta.png")

        with Cluster("Optimization"):
            optimize = Custom("Optimization\n(Z-Order)", "./custom_icons/delta.png")
            concurrency = Custom("Concurrency Control", "./custom_icons/delta.png")
            time_travel = Custom("Time Travel\n& Rollback", "./custom_icons/delta.png")
            vacuum = Custom("Vacuum", "./custom_icons/delta.png")

        processing_components = [batch, streaming, merge_ops, schema_evolution,
                                optimize, concurrency, time_travel, vacuum]

    # Storage Layer
    with Cluster("Storage Layer"):
        with Cluster("Delta Lake Tables"):
            transaction_log = Custom("Transaction Log", "./custom_icons/delta.png")
            parquet_files = Storage("Parquet Files")
            checkpoint_files = Storage("Checkpoint Files")

        file_system = Storage("File System\n(HDFS/Local)")

        storage_components = [transaction_log, parquet_files, checkpoint_files, file_system]

    # Connect the layers with cleaner edges
    # Create a single edge from each layer to the next
    web_ui >> Edge(color="darkblue", style="solid", penwidth="2.0") >> flask_api
    jupyter >> Edge(color="darkgreen", style="solid", penwidth="2.0") >> jupyter_app
    spark_ui >> Edge(color="darkred", style="solid", penwidth="2.0") >> delta_api

    # Connect application layer to processing layer
    flask_api >> Edge(color="darkblue", style="solid", penwidth="2.0") >> batch
    jupyter_app >> Edge(color="darkgreen", style="solid", penwidth="2.0") >> streaming
    delta_api >> Edge(color="darkred", style="solid", penwidth="2.0") >> merge_ops

    # Connect processing layer to storage layer
    batch >> Edge(color="darkblue", style="solid", penwidth="2.0") >> transaction_log
    streaming >> Edge(color="darkgreen", style="solid", penwidth="2.0") >> parquet_files
    merge_ops >> Edge(color="darkred", style="solid", penwidth="2.0") >> checkpoint_files

    # Connect to file system
    transaction_log >> Edge(color="black", style="dashed", penwidth="1.5") >> file_system
    parquet_files >> Edge(color="black", style="dashed", penwidth="1.5") >> file_system
    checkpoint_files >> Edge(color="black", style="dashed", penwidth="1.5") >> file_system

# Note: You'll need to create a 'custom_icons' directory and add delta.png and spark.png files
# or modify the paths to use existing icons from the diagrams library
