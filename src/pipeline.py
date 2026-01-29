#!/usr/bin/env python3
"""
Core PySpark Pipeline Code

This script implements the main data processing pipeline that reads from Parquet files,
performs various transformations, and writes results to Avro format with journaling.
"""

import os
import json
import datetime
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum, avg, max, min, when, lit, explode, concat,
    year, month, dayofmonth, hour, minute, second, date_format,
    collect_list, collect_set, map_from_arrays, struct, array_contains,
    row_number, rank, dense_rank, lead, lag, ntile, cume_dist, percent_rank,
    isnull
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, TimestampType


class PySparkPipeline:
    """Main pipeline class for data processing operations."""
    
    def __init__(self, spark: SparkSession):
        """Initialize pipeline with Spark session."""
        self.spark = spark
        self.journal_records = []
        
    def log_journal(self, operation: str, status: str, details: Dict[str, Any] = None) -> None:
        """Log processing status to journal."""
        journal_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "operation": operation,
            "status": status,
            "details": details or {}
        }
        self.journal_records.append(journal_entry)
        print(f"Journal: {operation} - {status}")
    
    def read_input_data(self, input_path: str) -> DataFrame:
        """Read input Parquet data."""
        try:
            df = self.spark.read.parquet(input_path)
            self.log_journal("read_input_data", "success", {"records": df.count()})
            return df
        except Exception as e:
            self.log_journal("read_input_data", "failed", {"error": str(e)})
            raise
    
    def data_quality_checks(self, df: DataFrame) -> DataFrame:
        """Perform data quality checks and validation."""
        original_count = df.count()
        
        # Check for null values in critical columns
        null_counts = df.select([
            (count(when(isnull(c), c)) / original_count).alias(c)
            for c in ["id", "name", "age", "salary"]
        ]).collect()[0]
        
        # Remove records with critical null values
        df_clean = df.na.drop(subset=["id", "name", "age", "salary"])
        
        cleaned_count = df_clean.count()
        removed_count = original_count - cleaned_count
        
        self.log_journal("data_quality_checks", "success", {
            "original_count": original_count,
            "cleaned_count": cleaned_count,
            "removed_count": removed_count,
            "null_percentages": dict(null_counts.asDict())
        })
        
        return df_clean
    
    def basic_transformations(self, df: DataFrame) -> DataFrame:
        """Apply basic DataFrame transformations."""
        try:
            # Add computed columns
            df_transformed = df.withColumn(
                "full_name", 
                concat(col("name"), lit(" "), lit("User"))
            ).withColumn(
                "age_group",
                when(col("age") < 25, "Young")
                .when(col("age") < 40, "Adult")
                .when(col("age") < 60, "Middle-aged")
                .otherwise("Senior")
            ).withColumn(
                "salary_category",
                when(col("salary") < 50000, "Low")
                .when(col("salary") < 100000, "Medium")
                .otherwise("High")
            ).withColumn(
                "is_high_performer",
                (col("projects_completed") > 20) & (col("satisfaction_score") > 7.0)
            )
            
            self.log_journal("basic_transformations", "success")
            return df_transformed
        except Exception as e:
            self.log_journal("basic_transformations", "failed", {"error": str(e)})
            raise
    
    def aggregations_and_grouping(self, df: DataFrame) -> DataFrame:
        """Perform aggregations and grouping operations."""
        try:
            # Check if we have flattened metadata (dept column) or original metadata
            if "dept" in df.columns:
                # Use flattened metadata
                dept_agg = df.groupBy("dept").agg(
                    count("*").alias("employee_count"),
                    avg("salary").alias("avg_salary"),
                    max("salary").alias("max_salary"),
                    min("salary").alias("min_salary"),
                    sum("projects_completed").alias("total_projects"),
                    avg("satisfaction_score").alias("avg_satisfaction")
                ).orderBy("dept")
                # Add department column for compatibility
                dept_agg = dept_agg.withColumnRenamed("dept", "department")
            else:
                # Use original metadata
                dept_agg = df.groupBy("metadata.department").agg(
                    count("*").alias("employee_count"),
                    avg("salary").alias("avg_salary"),
                    max("salary").alias("max_salary"),
                    min("salary").alias("min_salary"),
                    sum("projects_completed").alias("total_projects"),
                    avg("satisfaction_score").alias("avg_satisfaction")
                ).orderBy("metadata.department")
                # Add department column for compatibility
                dept_agg = dept_agg.withColumnRenamed("metadata.department", "department")
            
            self.log_journal("aggregations_and_grouping", "success", {"records": dept_agg.count()})
            return dept_agg
        except Exception as e:
            self.log_journal("aggregations_and_grouping", "failed", {"error": str(e)})
            raise
    
    def window_functions_example(self, df: DataFrame) -> DataFrame:
        """Demonstrate window functions for advanced analytics."""
        try:
            window_spec = Window.partitionBy("metadata.department").orderBy(col("salary").desc())
            
            df_with_window = df.withColumn(
                "rank_in_dept", 
                rank().over(window_spec)
            ).withColumn(
                "salary_percentile",
                percent_rank().over(window_spec)
            ).withColumn(
                "dept_avg_salary",
                avg(col("salary")).over(Window.partitionBy("metadata.department"))
            ).withColumn(
                "salary_vs_dept_avg",
                (col("salary") - avg(col("salary")).over(Window.partitionBy("metadata.department"))) /
                avg(col("salary")).over(Window.partitionBy("metadata.department"))
            )
            
            self.log_journal("window_functions_example", "success")
            return df_with_window
        except Exception as e:
            self.log_journal("window_functions_example", "failed", {"error": str(e)})
            raise
    
    def complex_transformations(self, df: DataFrame) -> DataFrame:
        """Apply complex transformations including array and map operations."""
        try:
            # Explode skills array
            df_exploded = df.withColumn("skill", explode(col("skills")))
            
            # Create skill-level aggregations
            skill_stats = df_exploded.groupBy("skill").agg(
                count("*").alias("skill_count"),
                avg("salary").alias("avg_salary_for_skill"),
                collect_set("metadata.department").alias("departments_using_skill")
            ).orderBy("skill_count", ascending=False)
            
            # Map operations - create flattened metadata
            df_flattened = df.withColumn(
                "dept", col("metadata.department")
            ).withColumn(
                "level", col("metadata.level").cast("integer")
            ).withColumn(
                "location", col("metadata.location")
            ).drop("metadata")
            
            self.log_journal("complex_transformations", "success", {
                "skill_stats_count": skill_stats.count(),
                "records_flattened": df_flattened.count()
            })
            
            return skill_stats, df_flattened
        except Exception as e:
            self.log_journal("complex_transformations", "failed", {"error": str(e)})
            raise
    
    def filtering_and_joins(self, df: DataFrame) -> DataFrame:
        """Demonstrate filtering and join operations."""
        try:
            # Filter high performers
            high_performers = df.filter(
                (col("projects_completed") > 25) & 
                (col("satisfaction_score") > 8.0) &
                col("is_active")
            )
            
            # Filter underperformers
            underperformers = df.filter(
                (col("projects_completed") < 5) | 
                (col("satisfaction_score") < 4.0)
            )
            
            # Self-join to find mentor relationships (simplified)
            mentor_window = Window.orderBy(col("projects_completed").desc())
            df_with_mentor_flag = df.withColumn(
                "is_senior_employee",
                (row_number().over(mentor_window) <= 2) & (col("projects_completed") > 25)  # Only top 2 employees with >25 projects are senior
            )
            
            self.log_journal("filtering_and_joins", "success", {
                "high_performers_count": high_performers.count(),
                "underperformers_count": underperformers.count(),
                "senior_employees_count": df_with_mentor_flag.filter(col("is_senior_employee")).count()
            })
            
            return high_performers, underperformers, df_with_mentor_flag
        except Exception as e:
            self.log_journal("filtering_and_joins", "failed", {"error": str(e)})
            raise
    
    def time_series_analysis(self, df: DataFrame) -> DataFrame:
        """Perform time-based analysis."""
        try:
            # Extract time components
            df_time = df.withColumn(
                "join_year", year(col("join_date"))
            ).withColumn(
                "join_month", month(col("join_date"))
            ).withColumn(
                "join_day", dayofmonth(col("join_date"))
            ).withColumn(
                "login_hour", hour(col("last_login"))
            ).withColumn(
                "login_day_of_week", date_format(col("last_login"), "E")
            )
            
            # Time-based aggregations
            hiring_trends = df_time.groupBy("join_year", "join_month").agg(
                count("*").alias("hires_that_month"),
                avg("salary").alias("avg_salary_for_hires")
            ).orderBy("join_year", "join_month")
            
            login_patterns = df_time.groupBy("login_hour").agg(
                count("*").alias("login_count"),
                avg("satisfaction_score").alias("avg_satisfaction_during_hour")
            ).orderBy("login_hour")
            
            self.log_journal("time_series_analysis", "success", {
                "hiring_trends_records": hiring_trends.count(),
                "login_patterns_records": login_patterns.count()
            })
            
            return hiring_trends, login_patterns
        except Exception as e:
            self.log_journal("time_series_analysis", "failed", {"error": str(e)})
            raise
    
    def save_outputs(self, 
                    dept_agg: DataFrame, 
                    skill_stats: DataFrame, 
                    df_flattened: DataFrame,
                    high_performers: DataFrame,
                    underperformers: DataFrame,
                    df_with_mentor_flag: DataFrame,
                    hiring_trends: DataFrame,
                    login_patterns: DataFrame,
                    output_path: str) -> None:
        """Save all processed outputs to Avro format."""
        try:
            # Create output directory
            os.makedirs(output_path, exist_ok=True)
            
            # Save each DataFrame to Avro
            outputs = [
                (dept_agg, "department_aggregations"),
                (skill_stats, "skill_statistics"),
                (df_flattened, "flattened_employee_data"),
                (high_performers, "high_performers"),
                (underperformers, "underperformers"),
                (df_with_mentor_flag, "employee_with_mentor_flags"),
                (hiring_trends, "hiring_trends"),
                (login_patterns, "login_patterns")
            ]
            
            for df, name in outputs:
                avro_path = os.path.join(output_path, f"{name}.avro")
                df.write.format("avro").mode("overwrite").save(avro_path)
                print(f"Saved {name} to {avro_path}")
            
            self.log_journal("save_outputs", "success", {
                "output_files": len(outputs),
                "output_path": output_path
            })
            
        except Exception as e:
            self.log_journal("save_outputs", "failed", {"error": str(e)})
            raise
    
    def save_journal(self, output_path: str) -> None:
        """Save processing journal to JSONL format."""
        try:
            journal_path = os.path.join(output_path, "processing_journal.jsonl")
            with open(journal_path, 'w') as f:
                for record in self.journal_records:
                    f.write(json.dumps(record) + '\n')
            
            print(f"Journal saved to {journal_path}")
            self.log_journal("save_journal", "success", {"journal_path": journal_path})
            
        except Exception as e:
            self.log_journal("save_journal", "failed", {"error": str(e)})
            raise
    
    def run_pipeline(self, input_path: str, output_path: str) -> None:
        """Execute the complete pipeline."""
        print("Starting PySpark pipeline execution...")
        
        try:
            # Step 1: Read input data
            df = self.read_input_data(input_path)
            
            # Step 2: Data quality checks
            df_clean = self.data_quality_checks(df)
            
            # Step 3: Basic transformations
            df_transformed = self.basic_transformations(df_clean)
            
            # Step 4: Aggregations and grouping
            dept_agg = self.aggregations_and_grouping(df_transformed)
            
            # Step 5: Window functions
            df_window = self.window_functions_example(df_transformed)
            
            # Step 6: Complex transformations
            skill_stats, df_flattened = self.complex_transformations(df_transformed)
            
            # Step 7: Filtering and joins
            high_performers, underperformers, df_mentor = self.filtering_and_joins(df_transformed)
            
            # Step 8: Time series analysis
            hiring_trends, login_patterns = self.time_series_analysis(df_transformed)
            
            # Step 9: Save outputs
            self.save_outputs(
                dept_agg, skill_stats, df_flattened,
                high_performers, underperformers, df_mentor,
                hiring_trends, login_patterns,
                output_path
            )
            
            # Step 10: Save journal
            self.save_journal(output_path)
            
            print("Pipeline execution completed successfully!")
            
        except Exception as e:
            print(f"Pipeline execution failed: {str(e)}")
            self.log_journal("pipeline_execution", "failed", {"error": str(e)})
            raise


def create_spark_session() -> SparkSession:
    """Create and configure Spark session for local execution."""
    return SparkSession.builder \
        .appName("PySparkSamplePipeline") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def main():
    """Main function to run the pipeline."""
    # Create output directory
    output_dir = "/home/ymarkiv/git/pyspark_sample/pyspark_sample/data/output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Initialize and run pipeline
        pipeline = PySparkPipeline(spark)
        input_path = "/home/ymarkiv/git/pyspark_sample/pyspark_sample/data/input/synthetic_data.parquet"
        pipeline.run_pipeline(input_path, output_dir)
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        raise
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()