# Delta Lake Concurrency Demo

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)]()
[![Spark](https://img.shields.io/badge/Spark-3.5+-orange.svg)]()
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Latest-green.svg)]()

A comprehensive Docker-based project demonstrating Delta Lake's advanced features including concurrency control, streaming integration, schema evolution, and time travel capabilities using the Global Superstore dataset.
![image](https://github.com/user-attachments/assets/c2c20f04-da28-41ba-8891-dd84b4f2ac19)

## Project Overview

This project provides hands-on experience with Delta Lake's key features in a real-world scenario. It simulates a retail data pipeline that combines batch and streaming data processing, demonstrating how Delta Lake handles concurrency, schema evolution, and data quality challenges.

### Key Questions Addressed

This project addresses several critical questions about Delta Lake's capabilities:

1. **What concurrency control problems might occur with batch and stream jobs writing to the same table?**
   - How Delta Lake prevents write conflicts and ensures consistency
   - Strategies for managing concurrent readers and writers
   - Handling race conditions in mixed workloads

2. **How would you use Delta's MERGE, OPTIMIZE, and VACUUM without risking state corruption?**
   - Safe implementation patterns for MERGE operations
   - Best practices for compaction and file management
   - Proper retention periods and maintenance windows

3. **Would you use replaceWhere, upserts with hash keys, or Z-order clustering?**
   - Tradeoffs between different data organization strategies
   - When to use each approach based on workload characteristics
   - Performance implications of different strategies

4. **How would schema evolution affect stream ingestion if a new column is added in batch only?**
   - Managing schema compatibility between batch and streaming
   - Handling null values and defaults in evolving schemas
   - Ensuring data quality during schema transitions

5. **How would you implement time travel rollback in the event of upstream corruption?**
   - Detecting data quality issues across versions
   - Safe rollback procedures without data loss
   - Recovery strategies for different corruption scenarios

### Key Features Demonstrated

- **Concurrent Read/Write Operations**: Managing multiple writers to the same Delta table
- **Streaming + Batch Integration**: Combining real-time and batch processing
- **MERGE Operations**: Implementing upserts and conditional updates
- **File Management**: OPTIMIZE and VACUUM operations for performance
- **Schema Evolution**: Adding and modifying columns without disruption
- **Time Travel**: Querying historical data and rollback capabilities

## Prerequisites

- Docker and Docker Compose (latest version recommended)
- Git
- 8GB+ RAM available for Docker
- Kaggle account (for dataset download) or internet connection for sample data generation

## Quick Start

1. Clone this repository:
```bash
git clone https://github.com/shanojpillai/delta-lake-concurrency-demo.git
cd delta-lake-concurrency-demo
```

2. Set up Kaggle credentials for dataset download:
```bash
export KAGGLE_USERNAME=your_kaggle_username
export KAGGLE_KEY=your_kaggle_api_key
```

3. Start the Docker environment:
```bash
docker-compose up -d
```

4. Access Jupyter Notebook:
Open your browser and navigate to `http://localhost:8888`
The token will be displayed in the docker-compose logs.

5. Run through the notebooks in sequence:
   - `01_setup.ipynb`: Initial Delta table setup
   - `02_streaming.ipynb`: Streaming integration
   - `03_concurrency.ipynb`: Concurrency handling
   - ... and so on

## Architecture

The Delta Lake Concurrency Demo is built with a modular architecture that showcases Delta Lake's capabilities in a real-world scenario.

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

For more details on the architecture, see [Architecture Documentation](docs/architecture_diagram.md).

## Project Structure

```
delta-lake-concurrency-demo/
├── data/                    # Data directories
├── docker/                  # Docker configurations
├── notebooks/               # Jupyter notebooks
├── scripts/                 # Python scripts
├── docs/                    # Documentation
├── docker-compose.yml       # Docker composition
├── Dockerfile               # Main Dockerfile
└── README.md                # This file
```

## Implementation Details

### Delta Table Setup

The project begins by creating a properly partitioned Delta table using the Global Superstore dataset, with optimized settings for performance and concurrency.

### Streaming Simulation

A Python script simulates real-time data by generating new sales records and writing them to a directory monitored by a Structured Streaming job.

### Concurrency Demonstration

The project runs concurrent operations (streaming appends, batch updates, and maintenance) to demonstrate Delta Lake's handling of concurrency conflicts.

### MERGE Operations

Demonstrates how to properly implement MERGE operations for upserts, with conditional logic based on business rules.

### Optimization and Maintenance

Shows the impact of OPTIMIZE and VACUUM commands on performance and storage, with best practices for implementation.

### Schema Evolution

Demonstrates adding and modifying columns while maintaining compatibility with existing processes.

### Time Travel and Rollback

Intentionally introduces problematic data, then uses time travel capabilities to identify and roll back the changes.

## Running the Components

### 1. Data Generator

Start the streaming data generator:

```bash
docker exec -it jupyter python /home/jovyan/scripts/stream_generator.py
```

### 2. Batch Operations

Run the batch update script:

```bash
docker exec -it jupyter python /home/jovyan/scripts/batch_updater.py
```

### 3. Notebooks

Follow the numbered notebooks in sequence through the Jupyter interface.

## Monitoring

- Web UI: http://localhost:5000
- Spark UI: http://localhost:4040
- Jupyter: http://localhost:8888

## Troubleshooting

### Common Issues

#### 1. Container fails to start

Check Docker logs:
```bash
docker-compose logs
```

Ensure you have sufficient memory allocated to Docker.

#### 2. Notebook kernel dies during operations

Increase the memory allocation for Docker/containers and restart.

#### 3. Concurrency exceptions

These are expected! The notebooks include instructions for handling them.

#### 4. Schema evolution issues

Ensure `spark.databricks.delta.schema.autoMerge.enabled` is set to `true`.

## Learning Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake: The Definitive Guide](https://databricks.com/p/ebook/delta-lake-the-definitive-guide)
- [Additional documentation in the `/docs` directory]

## Contribution

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Delta Lake Community
- Apache Spark Community
- Global Superstore dataset creators
- All contributors to this project
