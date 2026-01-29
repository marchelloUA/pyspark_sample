# PySpark Sample Pipeline

This repository contains a comprehensive PySpark sample pipeline that demonstrates various PySpark capabilities and data processing patterns.

## Pipeline Overview

The pipeline consists of the following main components:

### Step 0: Data Generation
- Generates synthetic dataset in Parquet format
- Includes columns with all possible data types (string, integer, float, boolean, timestamp, decimal, array, map, struct)
- Dataset size is optimized to demonstrate PySpark capabilities on a local machine

### Step 1: Data Processing Pipeline
- Reads data from Parquet files
- Performs various transformations using PySpark DataFrame and RDD operations
- Demonstrates common PySpark patterns and best practices
- Handles data quality checks and validation

### Step 2: Output Generation
- Writes processed results to Avro format
- Generates journal status records in JSONL format
- Maintains data lineage and processing metadata

## Key Features

- **Comprehensive Data Types**: Demonstrates handling of all PySpark-supported data types
- **Real-world Transformations**: Includes filtering, aggregation, joins, and complex operations
- **Error Handling**: Implements robust error handling and data validation
- **Performance Considerations**: Optimized for local execution while demonstrating scalability patterns
- **Data Quality**: Includes data validation and quality checks
- **Journaling**: Maintains processing status and metadata for auditability

## Local Execution

The pipeline is designed to run on a single machine (laptop/desktop) without requiring distributed infrastructure. All PySpark nuances and patterns are demonstrated in a sandbox environment that can be executed locally.

## File Structure

- `data/`: Input and output data files
  - `input/`: Generated synthetic Parquet files
  - `output/`: Processed Avro files and JSONL journal records
- `src/`: PySpark pipeline source code
- `tests/`: Unit tests and validation scripts
