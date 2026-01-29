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

```
pyspark_sample/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── src/                     # Source code directory
│   ├── data_generator.py    # Synthetic data generation script
│   ├── pipeline.py          # Core pipeline processing code
│   └── main.py              # Main orchestration script
├── data/                    # Data directory
│   ├── input/               # Generated synthetic Parquet files
│   └── output/              # Processed Avro files and JSONL journal records
└── tests/                   # Unit tests and validation scripts
    └── test_pipeline.py     # Pipeline validation tests
```

## Installation and Setup

### Prerequisites

1. **Python 3.7+**
2. **Java 8+** (required for PySpark)
3. **Apache Spark 3.3+** (will be installed via pip)

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Verify Installation

```bash
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
```

## Running the Pipeline

### Option 1: Run Complete Pipeline

Execute the main orchestration script to run the entire pipeline:

```bash
python src/main.py
```

This will:
1. Check dependencies and setup directories
2. Generate synthetic data
3. Run the main pipeline
4. Run validation tests
5. Generate output reports

### Option 2: Run Individual Components

#### Generate Data Only

```bash
python src/data_generator.py
```

#### Run Pipeline Only (requires data to be generated first)

```bash
python src/pipeline.py
```

#### Run Tests Only

```bash
python tests/test_pipeline.py
```

## Pipeline Details

### Data Generation (`data_generator.py`)

- Generates 10,000 synthetic employee records
- Includes all PySpark data types:
  - Basic: string, integer, float, boolean, date, timestamp, decimal
  - Complex: array, map, struct
- Saves data as Parquet files in `data/input/`

### Core Pipeline (`pipeline.py`)

The pipeline performs the following operations:

1. **Data Quality Checks**
   - Removes records with critical null values
   - Validates data integrity

2. **Basic Transformations**
   - Adds computed columns (age groups, salary categories)
   - Creates performance indicators

3. **Aggregations and Grouping**
   - Department-level statistics
   - Average salary calculations
   - Project completion metrics

4. **Window Functions**
   - Department ranking
   - Salary percentiles
   - Comparative analysis

5. **Complex Transformations**
   - Array operations (skill analysis)
   - Map operations (metadata flattening)
   - Struct field extraction

6. **Filtering and Joins**
   - High performer identification
   - Underperformer analysis
   - Mentor relationship modeling

7. **Time Series Analysis**
   - Hiring trends by month/year
   - Login pattern analysis
   - Temporal aggregations

### Output Generation

- **Avro Files**: Processed data in optimized format
- **JSONL Journal**: Processing metadata and status records
- **Multiple Output Files**:
  - `department_aggregations.avro`
  - `skill_statistics.avro`
  - `flattened_employee_data.avro`
  - `high_performers.avro`
  - `underperformers.avro`
  - `employee_with_mentor_flags.avro`
  - `hiring_trends.avro`
  - `login_patterns.avro`
  - `processing_journal.jsonl`

## Testing

The pipeline includes comprehensive unit tests:

```bash
python tests/test_pipeline.py
```

Test coverage includes:
- Data loading and validation
- Transformation operations
- Aggregation functions
- Filtering operations
- File I/O operations
- Schema validation

## Performance Considerations

- **Memory**: Configured for 2-4GB local execution
- **Adaptive Query Execution**: Enabled for optimization
- **Partitioning**: Automatically managed by Spark
- **Serialization**: Kryo serializer for better performance

## Troubleshooting

### Common Issues

1. **Java Not Found**
   ```
   Error: JAVA_HOME is not set and no 'java' command could be found
   ```
   Solution: Install Java 8+ and set JAVA_HOME environment variable.

2. **PySpark Import Error**
   ```
   ImportError: No module named 'pyspark'
   ```
   Solution: Install dependencies with `pip install -r requirements.txt`

3. **Memory Issues**
   ```
   SparkException: Could not create executor
   ```
   Solution: Reduce memory configuration in the scripts or increase available memory.

4. **File Not Found**
   ```
   FileNotFoundError: [Errno 2] No such file or directory
   ```
   Solution: Ensure data generation has been run before pipeline execution.

### Debug Mode

For debugging, you can modify the Spark configuration in the scripts to enable verbose logging:

```python
.config("spark.log.level", "DEBUG")
.config("spark.sql.adaptive.enabled", "false")  # Disable for detailed query plans
```

## Extension Points

The pipeline is designed to be easily extensible:

1. **Add New Data Types**: Extend the schema in `data_generator.py`
2. **Custom Transformations**: Add methods to the `PySparkPipeline` class
3. **Additional Output Formats**: Modify the `save_outputs` method
4. **Enhanced Validation**: Add more data quality checks
5. **Real-time Processing**: Adapt for streaming scenarios

## License

This project is for educational and demonstration purposes.
