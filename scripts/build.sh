#!/bin/bash

# Your Python module source code directory
MODULE_DIR="src"

# Navigate to the module directory
cd "$MODULE_DIR" || exit

# Build and install the Python module
python setup.py build
python setup.py install