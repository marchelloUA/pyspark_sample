#!/usr/bin/env python3
"""
Main Orchestration Script for PySpark Sample Pipeline

This script coordinates the execution of data generation and pipeline processing.
It ensures synthetic data is generated before running the core pipeline.
"""

import os
import sys
import subprocess
from pathlib import Path


def run_command(command: str, description: str) -> bool:
    """Run a shell command and return success status."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {command}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=True, 
            capture_output=True, 
            text=True
        )
        print(f"✓ {description} completed successfully")
        if result.stdout:
            print("Output:")
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed")
        print(f"Error: {e.stderr}")
        return False


def check_dependencies() -> bool:
    """Check if required dependencies are available."""
    print("Checking dependencies...")
    
    # Check Python
    try:
        import pyspark
        print(f"✓ PySpark available: {pyspark.__version__}")
    except ImportError:
        print("✗ PySpark not found. Please install it with: pip install pyspark")
        return False
    
    # Check Java (required for PySpark)
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        print("✓ Java available")
    except FileNotFoundError:
        print("✗ Java not found. Please install Java.")
        return False
    
    return True


def setup_directories() -> bool:
    """Create necessary directories."""
    print("Setting up directories...")
    
    directories = [
        "/home/ymarkiv/git/pyspark_sample/pyspark_sample/data/input",
        "/home/ymarkiv/git/pyspark_sample/pyspark_sample/data/output",
        "/home/ymarkiv/git/pyspark_sample/pyspark_sample/src"
    ]
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"✓ Directory created/verified: {directory}")
        except Exception as e:
            print(f"✗ Failed to create directory {directory}: {e}")
            return False
    
    return True


def generate_data() -> bool:
    """Generate synthetic data using the data generator script."""
    print("Starting data generation...")
    
    script_path = "/home/ymarkiv/git/pyspark_sample/pyspark_sample/src/data_generator.py"
    
    if not os.path.exists(script_path):
        print(f"✗ Data generator script not found: {script_path}")
        return False
    
    command = f"python {script_path}"
    return run_command(command, "Data Generation")


def run_pipeline() -> bool:
    """Run the main PySpark pipeline."""
    print("Starting pipeline execution...")
    
    script_path = "/home/ymarkiv/git/pyspark_sample/pyspark_sample/src/pipeline.py"
    
    if not os.path.exists(script_path):
        print(f"✗ Pipeline script not found: {script_path}")
        return False
    
    command = f"python {script_path}"
    return run_command(command, "Pipeline Execution")


def run_tests() -> bool:
    """Run unit tests if they exist."""
    test_path = "/home/ymarkiv/git/pyspark_sample/pyspark_sample/tests"
    
    if not os.path.exists(test_path):
        print("No tests directory found, skipping tests")
        return True
    
    # Look for test files
    test_files = list(Path(test_path).glob("test_*.py"))
    
    if not test_files:
        print("No test files found, skipping tests")
        return True
    
    print("Running tests...")
    success_count = 0
    
    for test_file in test_files:
        command = f"python {test_file}"
        if run_command(command, f"Test: {test_file.name}"):
            success_count += 1
    
    print(f"\nTest Results: {success_count}/{len(test_files)} passed")
    return success_count == len(test_files)


def main():
    """Main orchestration function."""
    print("PySpark Sample Pipeline - Main Orchestration")
    print("=" * 60)
    
    # Step 1: Check dependencies
    if not check_dependencies():
        print("Dependency check failed. Exiting.")
        sys.exit(1)
    
    # Step 2: Setup directories
    if not setup_directories():
        print("Directory setup failed. Exiting.")
        sys.exit(1)
    
    # Step 3: Generate synthetic data
    if not generate_data():
        print("Data generation failed. Exiting.")
        sys.exit(1)
    
    # Step 4: Run main pipeline
    if not run_pipeline():
        print("Pipeline execution failed. Exiting.")
        sys.exit(1)
    
    # Step 5: Run tests (optional)
    run_tests()
    
    print("\n" + "=" * 60)
    print("✓ All pipeline steps completed successfully!")
    print("=" * 60)
    
    # Print summary of outputs
    print("\nOutput Files:")
    output_dir = "/home/ymarkiv/git/pyspark_sample/pyspark_sample/data/output"
    if os.path.exists(output_dir):
        for file in os.listdir(output_dir):
            print(f"  - {file}")
    
    print("\nProcessing complete!")


if __name__ == "__main__":
    main()