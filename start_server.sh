#!/bin/bash

echo "Starting KIMBALL v2.0 FastAPI Server..."
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed or not in PATH"
    echo "Please install Python 3.8+ and try again"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1)
echo "Python found: $PYTHON_VERSION"

# Check if virtual environment exists
VENV_PATH="/opt/tomcat/.venv"
if [ ! -d "$VENV_PATH" ]; then
    echo "WARNING: Virtual environment not found at $VENV_PATH"
    echo "Continuing without virtual environment activation..."
    USE_VENV=false
else
    echo "Virtual environment found at $VENV_PATH"
    USE_VENV=true
fi

echo ""
echo "Starting FastAPI server with hot reloading..."
echo "Server: http://localhost:8000"
echo "API Docs: http://localhost:8000/docs"
echo "Alternative Docs: http://localhost:8000/redoc"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Activate virtual environment if it exists
if [ "$USE_VENV" = true ]; then
    source "$VENV_PATH/bin/activate"
    echo "Virtual environment activated"
    echo ""
fi

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Start server
uvicorn kimball.api.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir kimball --log-level info

