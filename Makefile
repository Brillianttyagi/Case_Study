# Makefile for building and running Case Analysis

# Target: build - Builds the Python egg distribution
build:
	@echo "Building Python egg distribution..."
	@python setup.py bdist_egg
	@echo "Build complete."

# Target: run - Submits the Spark job with the generated egg file
run:
	@echo "Submitting Spark job..."
	@spark-submit --master "local[*]" --py-files dist/BCG_Analysis-0.0.1-py3.10.egg main.py
	@echo "Spark job submitted successfully."

# Target: help - Displays available targets and their descriptions
help:
	@echo "Available targets:"
	@echo "  - build: Build the Python egg distribution."
	@echo "  - run:   Submit the Spark job with the generated egg file."
	@echo "  - help:  Display this help message."

# Ensure that targets without dependencies are always executed
.PHONY: build run help
