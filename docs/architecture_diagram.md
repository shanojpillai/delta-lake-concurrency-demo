# Delta Lake Concurrency Demo - Architecture

## Architecture Overview

The Delta Lake Concurrency Demo is built with a modular architecture that showcases Delta Lake's capabilities in a real-world scenario. The architecture consists of several key components:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           User Interfaces                                │
│                                                                         │
│  ┌───────────────┐   ┌───────────────┐   ┌───────────────────────────┐  │
│  │  Web UI       │   │  Jupyter      │   │  Spark UI                 │  │
│  │  (Port 5000)  │   │  (Port 8888)  │   │  (Port 4040)              │  │
│  └───────┬───────┘   └───────┬───────┘   └───────────────────────────┘  │
└──────────┼─────────────────────────────────────────────────────────────┘
           │                     │
           ▼                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                           Application Layer                               │
│                                                                          │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────┐  │
│  │ Flask API      │  │ Jupyter        │  │ Delta Lake API             │  │
│  │ Server         │  │ Notebooks      │  │ Integration                │  │
│  └────────┬───────┘  └────────┬───────┘  └─────────────┬──────────────┘  │
└───────────┼────────────────────────────────────────────────────────────┘
            │                    │                        │
            ▼                    ▼                        ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                           Processing Layer                                 │
│                                                                           │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌──────────┐ │
│  │ Batch          │  │ Streaming      │  │ MERGE          │  │ Schema    │ │
│  │ Processing     │  │ Processing     │  │ Operations     │  │ Evolution │ │
│  └────────┬───────┘  └────────┬───────┘  └────────┬───────┘  └─────┬────┘ │
│           │                   │                   │                │      │
│  ┌────────▼───────┐  ┌────────▼───────┐  ┌────────▼───────┐  ┌────▼─────┐ │
│  │ Optimization   │  │ Concurrency    │  │ Time Travel    │  │ Vacuum   │ │
│  │ (Z-Order)      │  │ Control        │  │ & Rollback     │  │          │ │
│  └────────────────┘  └────────────────┘  └────────────────┘  └──────────┘ │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                           Storage Layer                                    │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                      Delta Lake Tables                              │   │
│  │                                                                     │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐ │   │
│  │  │ Transaction Log │  │ Parquet Files   │  │ Checkpoint Files    │ │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────────┘ │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │                      File System (HDFS/Local)                       │   │
│  └────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
```

## Component Description

### User Interfaces
- **Web UI (Port 5000)**: A Flask-based web application that provides a user-friendly interface to interact with Delta Lake features
- **Jupyter Notebooks (Port 8888)**: Interactive notebooks for exploring and demonstrating Delta Lake capabilities
- **Spark UI (Port 4040)**: Monitoring interface for Apache Spark jobs and tasks

### Application Layer
- **Flask API Server**: Provides RESTful APIs to interact with Delta Lake operations
- **Jupyter Notebooks**: Contains step-by-step demonstrations of Delta Lake features
- **Delta Lake API Integration**: Connects the application to Delta Lake functionality

### Processing Layer
- **Batch Processing**: Handles large-scale data processing operations
- **Streaming Processing**: Processes real-time data streams
- **MERGE Operations**: Implements upserts and conditional updates
- **Schema Evolution**: Manages schema changes and compatibility
- **Optimization (Z-Order)**: Improves query performance through data organization
- **Concurrency Control**: Manages multiple readers and writers
- **Time Travel & Rollback**: Provides historical data access and recovery
- **Vacuum**: Manages file retention and cleanup

### Storage Layer
- **Delta Lake Tables**: The core storage format combining transaction log and data files
- **Transaction Log**: Records all changes to the table
- **Parquet Files**: Stores the actual data in columnar format
- **Checkpoint Files**: Provides optimization for faster table loading
- **File System**: Underlying storage system (local file system in Docker)

## Data Flow

1. **Data Ingestion**:
   - Batch data is loaded from the Global Superstore dataset
   - Streaming data is generated by simulation scripts

2. **Processing**:
   - Data undergoes transformation and validation
   - Concurrent operations are managed by Delta Lake's transaction protocol

3. **Storage**:
   - Data is stored in Delta Lake format (transaction log + Parquet files)
   - File management operations optimize storage and performance

4. **Access**:
   - Users interact with the data through the Web UI or Jupyter notebooks
   - API endpoints provide programmatic access to Delta Lake operations

## Deployment Architecture

The entire system is containerized using Docker, with the following components:

- **delta-spark**: Main container running Spark, Delta Lake, Jupyter, and the Flask web server
- **Shared volumes**: For persisting data between container restarts

This architecture enables a self-contained demonstration environment that showcases Delta Lake's capabilities for handling concurrency, schema evolution, and data quality challenges in a realistic data pipeline scenario.
