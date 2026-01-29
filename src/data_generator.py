#!/usr/bin/env python3
"""
Data Generation Script for PySpark Sample Pipeline

This script generates synthetic datasets with all possible PySpark data types
and saves them in Parquet format for the pipeline to process.
"""

import os
import random
import datetime
from decimal import Decimal
from typing import List, Dict, Any
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, 
    BooleanType, TimestampType, DecimalType, ArrayType, MapType,
    DateType
)


def create_spark_session() -> SparkSession:
    """Create and configure Spark session for local execution."""
    return SparkSession.builder \
        .appName("DataGenerator") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def generate_synthetic_data(spark: SparkSession, num_records: int = 10000) -> List[Dict[str, Any]]:
    """Generate synthetic data with all PySpark-supported data types."""
    data = []
    
    for i in range(num_records):
        record = {
            # Basic data types
            "id": i + 1,
            "name": f"User_{i}",
            "age": random.randint(18, 80),
            "salary": round(random.uniform(30000, 200000), 2),
            "is_active": random.choice([True, False]),
            "join_date": datetime.date(2020, 1, 1) + datetime.timedelta(days=random.randint(0, 1825)),
            "last_login": datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365)),
            "rating": Decimal(str(round(random.uniform(1.0, 5.0), 2))),
            
            # Array type
            "skills": random.choice([
                ["Python", "SQL", "Spark"],
                ["Java", "Scala", "Hadoop"],
                ["R", "Statistics", "Machine Learning"],
                ["JavaScript", "React", "Node.js"]
            ]),
            
            # Map type
            "metadata": {
                "department": random.choice(["Engineering", "Sales", "Marketing", "HR"]),
                "level": random.randint(1, 10),
                "location": random.choice(["NYC", "SF", "London", "Tokyo"])
            },
            
            # Struct type
            "address": {
                "street": f"{random.randint(100, 999)} Main St",
                "city": random.choice(["New York", "San Francisco", "London", "Tokyo"]),
                "zip_code": str(random.randint(10000, 99999))
            },
            
            # Additional fields for variety
            "bonus": round(random.uniform(0, 50000), 2) if random.random() > 0.3 else None,
            "projects_completed": random.randint(0, 50),
            "satisfaction_score": round(random.uniform(1.0, 10.0), 1)
        }
        data.append(record)
    
    return data


def create_schema() -> StructType:
    """Define the schema for the synthetic data."""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", FloatType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("join_date", DateType(), True),
        StructField("last_login", TimestampType(), True),
        StructField("rating", DecimalType(10, 2), True),
        StructField("skills", ArrayType(StringType()), True),
        StructField("metadata", MapType(StringType(), StringType()), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip_code", StringType(), True)
        ]), True),
        StructField("bonus", FloatType(), True),
        StructField("projects_completed", IntegerType(), True),
        StructField("satisfaction_score", FloatType(), True)
    ])


def save_data_to_parquet(spark: SparkSession, data: List[Dict[str, Any]], output_path: str) -> None:
    """Save synthetic data to Parquet format."""
    # Convert list of dictionaries to DataFrame
    df = spark.createDataFrame(data, schema=create_schema())
    
    # Save to Parquet
    df.write.mode("overwrite").parquet(output_path)
    print(f"Data saved to {output_path}")
    print(f"Total records generated: {df.count()}")
    
    # Show schema and sample data
    print("\nSchema:")
    df.printSchema()
    
    print("\nSample data:")
    df.show(5, truncate=False)


def main():
    """Main function to generate synthetic data."""
    print("Starting synthetic data generation...")
    
    # Create output directory if it doesn't exist
    output_dir = "/home/ymarkiv/git/pyspark_sample/pyspark_sample/data/input"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Generate synthetic data
        num_records = 10000
        print(f"Generating {num_records} synthetic records...")
        synthetic_data = generate_synthetic_data(spark, num_records)
        
        # Save to Parquet
        output_path = os.path.join(output_dir, "synthetic_data.parquet")
        save_data_to_parquet(spark, synthetic_data, output_path)
        
        print("Data generation completed successfully!")
        
    except Exception as e:
        print(f"Error during data generation: {str(e)}")
        raise
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()