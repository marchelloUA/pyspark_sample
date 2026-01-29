.PHONY: help install test clean data pipeline all

# Default target
help:
	@echo "PySpark Sample Pipeline - Available Commands:"
	@echo "  install     - Install Python dependencies"
	@echo "  data        - Generate synthetic data"
	@echo "  pipeline    - Run the main pipeline"
	@echo "  test        - Run unit tests"
	@echo "  all         - Run complete pipeline (data + pipeline + test)"
	@echo "  clean       - Clean generated data and outputs"
	@echo "  help        - Show this help message"

# Install Python dependencies
install:
	pip install -r requirements.txt

# Generate synthetic data
data:
	python src/data_generator.py

# Run the main pipeline
pipeline:
	python src/pipeline.py

# Run unit tests
test:
	python tests/test_pipeline.py

# Run complete pipeline
all: data pipeline test

# Clean generated data and outputs
clean:
	rm -rf data/input/*
	rm -rf data/output/*
	@echo "Cleaned generated data and outputs"

# Quick verification
verify:
	@echo "Verifying installation..."
	@python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')" || echo "PySpark not installed. Run 'make install'"
	@echo "Checking required directories..."
	@mkdir -p data/input data/output
	@echo "Setup complete!"