#!/usr/bin/env python3
"""
Unit Tests for PySpark Sample Pipeline

This script contains basic tests to validate the pipeline functionality.
"""

import os
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType, ArrayType, MapType, DecimalType
import sys
import tempfile
import shutil
import datetime
from decimal import Decimal


class TestPySparkPipeline(unittest.TestCase):
    """Test cases for PySpark pipeline functionality."""
    
    def setUp(self):
        """Set up Spark session for each test."""
        self.spark = SparkSession.builder \
            .appName("PySparkPipelineTests") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Create test data
        self.test_data = [
            (1, "Alice", 25, 50000.0, True, "2020-01-15", "Engineering"),
            (2, "Bob", 30, 60000.0, True, "2019-03-20", "Sales"),
            (3, "Charlie", 35, 70000.0, False, "2018-07-10", "Marketing"),
            (4, "Diana", 28, 55000.0, True, "2021-02-28", "Engineering"),
            (5, "Eve", 32, 65000.0, True, "2020-11-05", "HR")
        ]
        
        self.test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("join_date", StringType(), True),
            StructField("department", StringType(), True)
        ])
        
        self.test_df = self.spark.createDataFrame(self.test_data, schema=self.test_schema)
    
    def tearDown(self):
        """Clean up Spark session after each test."""
        if hasattr(self, 'spark') and self.spark:
            self.spark.stop()
    
    def test_data_loading(self):
        """Test that test data is loaded correctly."""
        self.assertEqual(self.test_df.count(), 5)
        self.assertEqual(len(self.test_df.columns), 7)
        
        # Check specific values
        first_row = self.test_df.first()
        self.assertEqual(first_row["id"], 1)
        self.assertEqual(first_row["name"], "Alice")
        self.assertEqual(first_row["age"], 25)
    
    def test_data_quality_checks(self):
        """Test data quality validation."""
        # Test with clean data
        clean_count = self.test_df.na.drop(subset=["id", "name", "age", "salary"]).count()
        self.assertEqual(clean_count, 5)
        
        # Test with null values
        test_data_with_nulls = self.test_data + [(6, None, 40, 80000.0, True, "2017-05-15", "Engineering")]
        df_with_nulls = self.spark.createDataFrame(test_data_with_nulls, schema=self.test_schema)
        
        clean_count_with_nulls = df_with_nulls.na.drop(subset=["id", "name", "age", "salary"]).count()
        self.assertEqual(clean_count_with_nulls, 5)  # Should exclude the record with null name
    
    def test_basic_transformations(self):
        """Test basic DataFrame transformations."""
        from pyspark.sql.functions import col, when, concat, lit
        
        # Add computed columns
        df_transformed = self.test_df.withColumn(
            "age_group",
            when(col("age") < 30, "Young")
            .when(col("age") < 40, "Adult")
            .otherwise("Senior")
        ).withColumn(
            "salary_category",
            when(col("salary") < 55000, "Low")
            .otherwise("High")
        )
        
        # Check transformations
        self.assertEqual(df_transformed.count(), 5)
        self.assertIn("age_group", df_transformed.columns)
        self.assertIn("salary_category", df_transformed.columns)
        
        # Check specific transformations
        young_employees = df_transformed.filter(col("age_group") == "Young").count()
        self.assertEqual(young_employees, 2)  # Alice (25) and Diana (28)
    
    def test_aggregations(self):
        """Test aggregation operations."""
        from pyspark.sql.functions import count, avg, max, min
        
        # Department-level aggregations
        dept_agg = self.test_df.groupBy("department").agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary"),
            max("salary").alias("max_salary"),
            min("salary").alias("min_salary")
        )
        
        self.assertEqual(dept_agg.count(), 4)  # Engineering, Sales, Marketing, HR
        
        # Check specific aggregations
        engineering_agg = dept_agg.filter(col("department") == "Engineering").first()
        self.assertEqual(engineering_agg["employee_count"], 2)
        self.assertAlmostEqual(engineering_agg["avg_salary"], 52500.0, places=1)
    
    def test_filtering(self):
        """Test data filtering operations."""
        from pyspark.sql.functions import col
        
        # Create test data with metadata structure to match pipeline expectations
        test_data = [
            (1, "Alice", 25, 50000.0, True, "2020-01-15", {"department": "Engineering", "level": 3, "location": "NYC"}),
            (2, "Bob", 30, 60000.0, True, "2019-03-20", {"department": "Sales", "level": 2, "location": "LA"}),
            (3, "Charlie", 35, 70000.0, False, "2018-07-10", {"department": "Marketing", "level": 4, "location": "Chicago"}),
            (4, "Diana", 28, 55000.0, True, "2021-02-28", {"department": "Engineering", "level": 2, "location": "NYC"}),
            (5, "Eve", 32, 65000.0, True, "2020-11-05", {"department": "HR", "level": 3, "location": "SF"})
        ]
        
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("join_date", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema=test_schema)
        
        # Filter active employees
        active_employees = df.filter(col("is_active") == True)
        self.assertEqual(active_employees.count(), 4)
        
        # Filter high salary employees
        high_salary_employees = df.filter(col("salary") > 60000)
        self.assertEqual(high_salary_employees.count(), 2)
        
        # Filter by multiple conditions
        engineering_high_salary = df.filter(
            (col("metadata")["department"] == "Engineering") & (col("salary") > 50000)
        )
        self.assertEqual(engineering_high_salary.count(), 1)  # Only Diana meets both criteria (Alice has salary 50000 which is not > 50000)
    
    def test_file_operations(self):
        """Test file input/output operations."""
        import tempfile
        import os
        
        # Test writing to Parquet
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_path = os.path.join(temp_dir, "test_data.parquet")
            self.test_df.write.mode("overwrite").parquet(parquet_path)
            
            # Test reading from Parquet
            df_from_parquet = self.spark.read.parquet(parquet_path)
            self.assertEqual(df_from_parquet.count(), 5)
            self.assertEqual(len(df_from_parquet.columns), 7)
    
    def test_schema_validation(self):
        """Test schema validation."""
        expected_columns = {"id", "name", "age", "salary", "is_active", "join_date", "department"}
        actual_columns = set(self.test_df.columns)
        
        self.assertEqual(expected_columns, actual_columns)
        
        # Check data types
        schema_dict = {field.name: field.dataType for field in self.test_df.schema}
        self.assertIsInstance(schema_dict["id"], IntegerType)
        self.assertIsInstance(schema_dict["name"], StringType)
        self.assertIsInstance(schema_dict["salary"], FloatType)
        self.assertIsInstance(schema_dict["is_active"], BooleanType)
    
    def test_data_generator(self):
        """Test data generation functionality."""
        import sys
        import os
        import datetime
        from decimal import Decimal
        sys.path.append('/home/ymarkiv/git/pyspark_sample/pyspark_sample/src')
        
        from data_generator import create_spark_session, generate_synthetic_data
        
        # Create Spark session for data generation
        spark = create_spark_session()
        
        try:
            # Test data generation
            data = generate_synthetic_data(spark, num_records=100)
            self.assertEqual(len(data), 100)
            
            # Test data structure
            required_fields = ['id', 'name', 'age', 'salary', 'is_active', 'join_date', 'last_login', 'rating']
            for record in data:
                for field in required_fields:
                    self.assertIn(field, record)
            
            # Test data types and ranges
            for record in data:
                self.assertIsInstance(record['id'], int)
                self.assertIsInstance(record['name'], str)
                self.assertIsInstance(record['age'], int)
                self.assertGreaterEqual(record['age'], 18)
                self.assertLessEqual(record['age'], 80)
                self.assertIsInstance(record['salary'], (int, float))
                self.assertGreaterEqual(record['salary'], 30000)
                self.assertLessEqual(record['salary'], 200000)
                self.assertIsInstance(record['is_active'], bool)
                self.assertIsInstance(record['join_date'], datetime.date)
                self.assertIsInstance(record['last_login'], datetime.datetime)
                self.assertIsInstance(record['rating'], Decimal)
                
        finally:
            spark.stop()
    
    def test_pipeline_class_initialization(self):
        """Test PySparkPipeline class initialization."""
        from src.pipeline import PySparkPipeline
        
        # Test pipeline initialization
        pipeline = PySparkPipeline(self.spark)
        self.assertIsNotNone(pipeline.spark)
        self.assertEqual(pipeline.journal_records, [])
    
    def test_pipeline_journal_logging(self):
        """Test pipeline journal logging functionality."""
        from src.pipeline import PySparkPipeline
        
        pipeline = PySparkPipeline(self.spark)
        
        # Test journal logging
        pipeline.log_journal("test_operation", "success", {"test": "data"})
        self.assertEqual(len(pipeline.journal_records), 1)
        
        journal_entry = pipeline.journal_records[0]
        self.assertEqual(journal_entry["operation"], "test_operation")
        self.assertEqual(journal_entry["status"], "success")
        self.assertEqual(journal_entry["details"]["test"], "data")
        self.assertIn("timestamp", journal_entry)
    
    def test_error_handling_in_pipeline(self):
        """Test error handling in pipeline operations."""
        from src.pipeline import PySparkPipeline
        
        pipeline = PySparkPipeline(self.spark)
        
        # Test error handling for reading non-existent file
        with self.assertRaises(Exception):
            pipeline.read_input_data("/non/existent/path.parquet")
    
    def test_spark_session_creation(self):
        """Test Spark session creation and configuration."""
        from src.pipeline import create_spark_session
        
        # Test Spark session creation
        spark = create_spark_session()
        self.assertIsNotNone(spark)
        
        # Test Spark session configuration
        app_name = spark.conf.get("spark.app.name")
        self.assertEqual(app_name, "PySparkSamplePipeline")
        
        spark.stop()
    
    def test_data_quality_with_edge_cases(self):
        """Test data quality checks with edge cases."""
        from src.pipeline import PySparkPipeline
        
        pipeline = PySparkPipeline(self.spark)
        
        # Test with data containing nulls in critical columns
        test_data_with_nulls = [
            (1, "Alice", 25, 50000.0, True, "2020-01-15", "Engineering"),
            (2, None, 30, 60000.0, True, "2019-03-20", "Sales"),  # Null name
            (3, "Charlie", None, 70000.0, False, "2018-07-10", "Marketing"),  # Null age
            (4, "Diana", 28, None, True, "2021-02-28", "Engineering"),  # Null salary
            (5, "Eve", 32, 65000.0, True, "2020-11-05", "HR")
        ]
        
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("join_date", StringType(), True),
            StructField("department", StringType(), True)
        ])
        
        df_with_nulls = self.spark.createDataFrame(test_data_with_nulls, schema=test_schema)
        
        # Test data quality checks
        df_clean = pipeline.data_quality_checks(df_with_nulls)
        
        # Should remove records with nulls in critical columns
        self.assertEqual(df_clean.count(), 2)  # Only Alice and Eve should remain
        
        # Check that remaining records don't have nulls in critical columns
        for row in df_clean.collect():
            self.assertIsNotNone(row["name"])
            self.assertIsNotNone(row["age"])
            self.assertIsNotNone(row["salary"])
    
    def test_complex_transformations(self):
        """Test complex transformations including array and map operations."""
        from src.pipeline import PySparkPipeline
        
        pipeline = PySparkPipeline(self.spark)
        
        # Create test data with arrays and maps
        test_data = [
            (1, "Alice", 25, 50000.0, True, "2020-01-15", "Engineering", 
             ["Python", "SQL", "Spark"], {"level": 5, "location": "NYC"}),
            (2, "Bob", 30, 60000.0, True, "2019-03-20", "Sales", 
             ["Java", "Excel"], {"level": 3, "location": "LA"}),
            (3, "Charlie", 35, 70000.0, False, "2018-07-10", "Marketing", 
             ["Python", "R", "Tableau"], {"level": 4, "location": "Chicago"})
        ]
        
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("join_date", StringType(), True),
            StructField("department", StringType(), True),
            StructField("skills", ArrayType(StringType()), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema=test_schema)
        
        # Test complex transformations
        skill_stats, df_flattened = pipeline.complex_transformations(df)
        
        # Check skill statistics
        self.assertGreater(skill_stats.count(), 0)
        self.assertIn("skill", skill_stats.columns)
        self.assertIn("skill_count", skill_stats.columns)
        
        # Check flattened DataFrame
        self.assertIn("dept", df_flattened.columns)
        self.assertIn("level", df_flattened.columns)
        self.assertIn("location", df_flattened.columns)
        self.assertNotIn("metadata", df_flattened.columns)
    
    def test_window_functions(self):
        """Test window functions functionality."""
        from src.pipeline import PySparkPipeline
        
        pipeline = PySparkPipeline(self.spark)
        
        # Create test data with metadata structure
        test_data = [
            (1, "Alice", 25, 50000.0, True, "2020-01-15", {"department": "Engineering", "level": "5"}, datetime.datetime(2023, 1, 15, 9, 0, 0)),
            (2, "Bob", 30, 60000.0, True, "2019-03-20", {"department": "Sales", "level": "3"}, datetime.datetime(2023, 1, 15, 10, 0, 0)),
            (3, "Charlie", 35, 70000.0, False, "2018-07-10", {"department": "Marketing", "level": "4"}, datetime.datetime(2023, 1, 15, 11, 0, 0)),
            (4, "Diana", 28, 55000.0, True, "2021-02-28", {"department": "Engineering", "level": "4"}, datetime.datetime(2023, 1, 15, 8, 0, 0)),
            (5, "Eve", 32, 65000.0, True, "2020-11-05", {"department": "HR", "level": "3"}, datetime.datetime(2023, 1, 15, 9, 30, 0))
        ]
        
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("join_date", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("last_login", TimestampType(), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema=test_schema)
        
        # Test window functions
        df_window = pipeline.window_functions_example(df)
        
        # Check that window function columns are added
        self.assertIn("rank_in_dept", df_window.columns)
        self.assertIn("salary_percentile", df_window.columns)
        self.assertIn("dept_avg_salary", df_window.columns)
        self.assertIn("salary_vs_dept_avg", df_window.columns)
        
        # Check that ranks are calculated correctly
        engineering_employees = df_window.filter(col("metadata")["department"] == "Engineering")
        self.assertEqual(engineering_employees.count(), 2)
        
        # Check that ranks are within expected range
        ranks = [row["rank_in_dept"] for row in engineering_employees.collect()]
        self.assertIn(1, ranks)
        self.assertIn(2, ranks)
    
    def test_time_series_analysis(self):
        """Test time series analysis functionality."""
        from src.pipeline import PySparkPipeline
        
        pipeline = PySparkPipeline(self.spark)
        
        # Create test data with metadata structure to match pipeline expectations
        test_data = [
            (1, "Alice", 25, 50000.0, True, "2020-01-15", {"department": "Engineering", "level": 3, "location": "NYC"}, datetime.datetime(2023, 1, 15, 9, 0, 0), 8.5),
            (2, "Bob", 30, 60000.0, True, "2019-03-20", {"department": "Sales", "level": 2, "location": "LA"}, datetime.datetime(2023, 1, 15, 10, 0, 0), 6.0),
            (3, "Charlie", 35, 70000.0, False, "2018-07-10", {"department": "Marketing", "level": 4, "location": "Chicago"}, datetime.datetime(2023, 1, 15, 11, 0, 0), 9.2),
            (4, "Diana", 28, 55000.0, True, "2021-02-28", {"department": "Engineering", "level": 2, "location": "NYC"}, datetime.datetime(2023, 1, 15, 8, 0, 0), 3.5),
            (5, "Eve", 32, 65000.0, True, "2020-11-05", {"department": "HR", "level": 3, "location": "SF"}, datetime.datetime(2023, 1, 15, 9, 30, 0), 8.8)
        ]
        
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("join_date", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("last_login", TimestampType(), True),
            StructField("satisfaction_score", FloatType(), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema=test_schema)
        
        # Test time series analysis
        hiring_trends, login_patterns = pipeline.time_series_analysis(df)
        
        # Check hiring trends
        self.assertIn("join_year", hiring_trends.columns)
        self.assertIn("join_month", hiring_trends.columns)
        self.assertIn("hires_that_month", hiring_trends.columns)
        
        # Check login patterns
        self.assertIn("login_hour", login_patterns.columns)
        self.assertIn("login_count", login_patterns.columns)
        self.assertIn("avg_satisfaction_during_hour", login_patterns.columns)
        
        # Check that we get reasonable results
        self.assertGreater(hiring_trends.count(), 0)
        self.assertGreater(login_patterns.count(), 0)
    
    def test_filtering_and_joins(self):
        """Test filtering and join operations."""
        from src.pipeline import PySparkPipeline
        
        pipeline = PySparkPipeline(self.spark)
        
        # Create test data with metadata structure to match pipeline expectations
        test_data = [
            (1, "Alice", 25, 50000.0, True, "2020-01-15", {"department": "Engineering", "level": 3, "location": "NYC"}, ["Python", "SQL"], 30, 8.5),
            (2, "Bob", 30, 60000.0, True, "2019-03-20", {"department": "Sales", "level": 2, "location": "LA"}, ["Java", "Excel"], 15, 6.0),
            (3, "Charlie", 35, 70000.0, False, "2018-07-10", {"department": "Marketing", "level": 4, "location": "Chicago"}, ["R", "Tableau"], 40, 9.2),
            (4, "Diana", 28, 55000.0, True, "2021-02-28", {"department": "Engineering", "level": 2, "location": "NYC"}, ["Python", "Spark"], 5, 3.5),
            (5, "Eve", 32, 65000.0, True, "2020-11-05", {"department": "HR", "level": 3, "location": "SF"}, ["Python", "HRIS"], 28, 8.8)
        ]
        
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("join_date", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("skills", ArrayType(StringType()), True),
            StructField("projects_completed", IntegerType(), True),
            StructField("satisfaction_score", FloatType(), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema=test_schema)
        
        # Test filtering and joins
        high_performers, underperformers, df_mentor = pipeline.filtering_and_joins(df)
        
        # Check high performers (should be Alice and Charlie)
        self.assertEqual(high_performers.count(), 2)
        
        # Check underperformers (should be Bob and Diana)  
        # Bob: low projects (15) but high satisfaction (6.0) - not underperformer by satisfaction
        # Diana: low projects (5) and medium satisfaction (3.5) - underperformer by projects
        # Charlie: inactive but high satisfaction - not underperformer
        # Only Diana should be underperformer
        self.assertEqual(underperformers.count(), 1)
        
        # Check mentor flags
        self.assertIn("is_senior_employee", df_mentor.columns)
        senior_employees = df_mentor.filter(col("is_senior_employee") == True)
        self.assertEqual(senior_employees.count(), 2)  # Charlie and Alice should be senior (both have >25 projects)
    
    def test_aggregations_and_grouping(self):
        """Test aggregations and grouping operations."""
        from src.pipeline import PySparkPipeline
        
        pipeline = PySparkPipeline(self.spark)
        
        # Create test data with metadata structure to match pipeline expectations
        test_data = [
            (1, "Alice", 25, 50000.0, True, "2020-01-15", {"department": "Engineering", "level": 3, "location": "NYC"}, ["Python", "SQL"], 30, 8.5),
            (2, "Bob", 30, 60000.0, True, "2019-03-20", {"department": "Sales", "level": 2, "location": "LA"}, ["Java", "Excel"], 15, 6.0),
            (3, "Charlie", 35, 70000.0, False, "2018-07-10", {"department": "Marketing", "level": 4, "location": "Chicago"}, ["R", "Tableau"], 40, 9.2),
            (4, "Diana", 28, 55000.0, True, "2021-02-28", {"department": "Engineering", "level": 2, "location": "NYC"}, ["Python", "Spark"], 5, 3.5),
            (5, "Eve", 32, 65000.0, True, "2020-11-05", {"department": "HR", "level": 3, "location": "SF"}, ["Python", "HRIS"], 28, 8.8)
        ]
        
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", FloatType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("join_date", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("skills", ArrayType(StringType()), True),
            StructField("projects_completed", IntegerType(), True),
            StructField("satisfaction_score", FloatType(), True)
        ])
        
        df = self.spark.createDataFrame(test_data, schema=test_schema)
        
        # Test complex transformations first to get flattened data
        skill_stats, df_flattened = pipeline.complex_transformations(df)
        
        # Test aggregations on flattened data
        dept_agg = pipeline.aggregations_and_grouping(df_flattened)
        
        # Check aggregation results
        self.assertIn("employee_count", dept_agg.columns)
        self.assertIn("avg_salary", dept_agg.columns)
        self.assertIn("max_salary", dept_agg.columns)
        self.assertIn("min_salary", dept_agg.columns)
        self.assertIn("total_projects", dept_agg.columns)
        self.assertIn("avg_satisfaction", dept_agg.columns)
        
        # Check that we have data for all departments
        self.assertEqual(dept_agg.count(), 4)  # Engineering, Sales, Marketing, HR
        
        # Check specific aggregations
        engineering_agg = dept_agg.filter(col("department") == "Engineering").first()
        self.assertEqual(engineering_agg["employee_count"], 2)
        self.assertAlmostEqual(engineering_agg["avg_salary"], 52500.0, places=1)
    
    
    
    