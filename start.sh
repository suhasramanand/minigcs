#!/bin/bash

# MiniGCS Startup Script

echo "Starting MiniGCS..."

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Check if required packages are installed
echo "Checking dependencies..."
python3 -c "import fastapi, uvicorn, aiofiles, yaml, prometheus_client" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Installing dependencies..."
    pip3 install -r requirements.txt
fi

# Create data directories
echo "Creating data directories..."
mkdir -p data/node-1 data/node-2 data/node-3

# Test installation
echo "Testing installation..."
python3 test_installation.py
if [ $? -ne 0 ]; then
    echo "Installation test failed"
    exit 1
fi

# Start the server
echo "Starting MiniGCS API server..."
python3 -m api.server
