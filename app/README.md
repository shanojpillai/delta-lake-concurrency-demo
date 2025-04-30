# Delta Lake Concurrency Demo - Web Application

This web application provides a user-friendly interface to interact with the Delta Lake Concurrency Demo. It allows users to explore various Delta Lake features through a web UI and Jupyter notebooks.

## Features

- **Setup**: Initialize the Delta Lake table with sample data
- **Streaming**: Demonstrate Delta Lake's streaming capabilities
- **Concurrency**: Explore how Delta Lake handles concurrent operations
- **MERGE Operations**: Learn about Delta Lake's MERGE command
- **Optimization**: Understand Delta Lake's optimization features (OPTIMIZE, Z-ORDER)
- **Schema Evolution**: See how Delta Lake handles schema changes
- **Time Travel**: Explore Delta Lake's time travel capabilities

## Architecture

The application consists of:

1. **Flask Web Server**: Serves the UI and handles API requests
2. **PySpark Backend**: Executes Delta Lake operations
3. **Jupyter Notebook Integration**: Links to detailed notebooks for each feature

## Getting Started

The application is automatically started when you run the Docker container. You can access it at:

```
http://localhost:5000
```

## Usage

1. **Setup**: Start by running the setup process to create the Delta table
2. **Explore Features**: Navigate through the different tabs to explore Delta Lake features
3. **View Notebooks**: Click on the "Open Notebook" button to see detailed code examples

## API Endpoints

The application provides several API endpoints:

- `/api/table_info`: Get information about the Delta table
- `/api/run_setup`: Run the setup process
- `/api/start_streaming`: Start the streaming data generator
- `/api/stop_streaming`: Stop the streaming data generator
- `/api/run_batch_update`: Run a batch update
- `/api/run_optimize`: Run OPTIMIZE on the Delta table
- `/api/run_zorder`: Run Z-ORDER BY on the Delta table
- `/api/add_column`: Add a new column to the Delta table
- `/api/write_bad_data`: Write bad data to the Delta table for time travel demo
- `/api/rollback`: Rollback the Delta table to a previous version
- `/api/query_version`: Query the Delta table at a specific version

## Performance Considerations

- The application is designed to work with moderate-sized datasets
- For large datasets, consider adjusting the batch size and interval settings
- The UI updates in real-time to show the current state of the Delta table

## Troubleshooting

If you encounter issues:

1. Check the Docker container logs
2. Verify that the Delta table was created successfully
3. Ensure that the PySpark backend is running properly

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
