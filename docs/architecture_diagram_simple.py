"""
Delta Lake Concurrency Demo Architecture Diagram Generator (Simplified Version)

This script generates an architecture diagram for the Delta Lake Concurrency Demo
using the diagrams Python package.

Requirements:
- Python 3.6+
- diagrams package (pip install diagrams)
- Graphviz (https://graphviz.org/download/)

To generate the diagram:
$ python architecture_diagram_simple.py
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.analytics import Spark
from diagrams.onprem.container import Docker
from diagrams.programming.framework import Flask
from diagrams.programming.language import Python, Jupyter
from diagrams.generic.storage import Storage
from diagrams.generic.database import SQL

# Set the output file path
output_path = "delta_lake_architecture_simple"

# Create the diagram
with Diagram("Delta Lake Concurrency Demo Architecture", show=False, filename=output_path, direction="TB"):
    
    # User Interface Layer
    with Cluster("User Interface Layer"):
        web_ui = Flask("Web UI\n(Port 5000)")
        jupyter = Jupyter("Jupyter Notebooks\n(Port 8888)")
        spark_ui = Spark("Spark UI\n(Port 4040)")
        
        user_interfaces = [web_ui, jupyter, spark_ui]
    
    # Application Layer
    with Cluster("Application Layer"):
        flask_api = Flask("Flask API Server")
        jupyter_app = Jupyter("Jupyter Notebooks")
        delta_api = Spark("Delta Lake API")
        
        application_components = [flask_api, jupyter_app, delta_api]
    
    # Processing Layer
    with Cluster("Processing Layer"):
        with Cluster("Data Processing"):
            batch = Spark("Batch Processing")
            streaming = Spark("Streaming Processing")
            merge_ops = Spark("MERGE Operations")
            schema_evolution = Spark("Schema Evolution")
        
        with Cluster("Optimization"):
            optimize = Spark("Optimization\n(Z-Order)")
            concurrency = Spark("Concurrency Control")
            time_travel = Spark("Time Travel\n& Rollback")
            vacuum = Spark("Vacuum")
        
        processing_components = [batch, streaming, merge_ops, schema_evolution, 
                                optimize, concurrency, time_travel, vacuum]
    
    # Storage Layer
    with Cluster("Storage Layer"):
        with Cluster("Delta Lake Tables"):
            transaction_log = SQL("Transaction Log")
            parquet_files = Storage("Parquet Files")
            checkpoint_files = Storage("Checkpoint Files")
        
        file_system = Storage("File System\n(HDFS/Local)")
        
        storage_components = [transaction_log, parquet_files, checkpoint_files, file_system]
    
    # Connect the layers
    for ui in user_interfaces:
        ui >> Edge(color="black") >> application_components
    
    for app in application_components:
        app >> Edge(color="black") >> processing_components
    
    for proc in processing_components:
        proc >> Edge(color="black") >> storage_components
