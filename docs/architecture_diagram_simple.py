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
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.programming.framework import Flask
from diagrams.onprem.client import Users
from diagrams.generic.storage import Storage
from diagrams.generic.database import SQL
from diagrams.onprem.container import Docker
from diagrams.programming.language import Python

# Set the output file path
output_path = "delta_lake_architecture_simple"

# Create the diagram
with Diagram("Delta Lake Concurrency Demo Architecture", show=False, filename=output_path, direction="TB"):

    # User Layer
    users = Users("Data Scientists\nand Analysts")

    # User Interface Layer
    with Cluster("User Interface Layer"):
        web_ui = Flask("Web UI\n(Port 5000)")
        jupyter = Python("Jupyter Notebooks\n(Port 8888)")
        spark_ui = Server("Spark UI\n(Port 4040)")

    # Application Layer
    with Cluster("Application Layer"):
        with Cluster("Services"):
            flask_api = Flask("Flask API Server")
            docker = Docker("Docker Container")

        with Cluster("Processing Engines"):
            spark = Spark("Apache Spark")
            delta_lake = Spark("Delta Lake")

    # Data Processing Layer
    with Cluster("Data Processing Features"):
        with Cluster("Core Features"):
            batch = Spark("Batch Processing")
            streaming = Spark("Streaming\nProcessing")
            merge_ops = Spark("MERGE\nOperations")

        with Cluster("Advanced Features"):
            schema_evolution = PostgreSQL("Schema\nEvolution")
            optimize = Spark("Optimization\n(Z-Order)")
            time_travel = Spark("Time Travel\n& Rollback")
            vacuum = Spark("Vacuum")

    # Storage Layer
    with Cluster("Storage Layer"):
        with Cluster("Delta Lake Tables"):
            transaction_log = SQL("Transaction Log")
            parquet_files = Storage("Parquet Files")
            checkpoint_files = Storage("Checkpoint Files")

        file_system = Storage("File System\n(HDFS/Local)")

    # Connect the layers with logical flow
    # User to interfaces
    users >> Edge(color="gray", style="dashed") >> web_ui
    users >> Edge(color="gray", style="dashed") >> jupyter
    users >> Edge(color="gray", style="dashed") >> spark_ui

    # Interfaces to application layer
    web_ui >> Edge(color="darkblue", style="solid", penwidth="1.5") >> flask_api
    jupyter >> Edge(color="darkgreen", style="solid", penwidth="1.5") >> spark
    spark_ui >> Edge(color="darkred", style="solid", penwidth="1.5") >> spark

    # Application components relationships
    flask_api >> Edge(color="black", style="solid") >> docker
    docker >> Edge(color="black", style="solid") >> spark
    spark >> Edge(color="black", style="solid") >> delta_lake

    # Connect application to processing features
    delta_lake >> Edge(color="darkblue") >> batch
    delta_lake >> Edge(color="darkgreen") >> streaming
    delta_lake >> Edge(color="darkred") >> merge_ops
    delta_lake >> Edge(color="purple") >> schema_evolution
    delta_lake >> Edge(color="orange") >> optimize
    delta_lake >> Edge(color="brown") >> time_travel
    delta_lake >> Edge(color="gray") >> vacuum

    # Connect processing to storage
    batch >> Edge(color="darkblue") >> transaction_log
    streaming >> Edge(color="darkgreen") >> transaction_log
    merge_ops >> Edge(color="darkred") >> transaction_log

    # Storage relationships
    transaction_log >> Edge(color="black") >> parquet_files
    transaction_log >> Edge(color="black") >> checkpoint_files
    parquet_files >> Edge(color="black", style="dashed") >> file_system
    checkpoint_files >> Edge(color="black", style="dashed") >> file_system
