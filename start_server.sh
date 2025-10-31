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

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Create logs directory if it doesn't exist
mkdir -p logs

# Log file path
LOG_FILE="$SCRIPT_DIR/logs/kimball_server.log"
PID_FILE="$SCRIPT_DIR/logs/kimball_server.pid"

# Check if server is already running
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "WARNING: Server appears to already be running (PID: $OLD_PID)"
        echo "Use ./stop_server.sh to stop it first, or check the process manually"
        exit 1
    else
        # Stale PID file, remove it
        rm -f "$PID_FILE"
    fi
fi

echo ""
echo "Starting FastAPI server in background with nohup..."
echo "Server: http://localhost:8000"
echo "API Docs: http://localhost:8000/docs"
echo "Alternative Docs: http://localhost:8000/redoc"
echo ""

# Activate virtual environment if it exists
if [ "$USE_VENV" = true ]; then
    source "$VENV_PATH/bin/activate"
    echo "Virtual environment activated"
fi

echo "Starting server in background with nohup..."
echo "Log file: $LOG_FILE"
echo "PID file: $PID_FILE"
echo ""

# Start server in background with nohup
nohup uvicorn kimball.api.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir kimball --log-level info > "$LOG_FILE" 2>&1 &

# Get the PID
SERVER_PID=$!

# Save PID to file
echo $SERVER_PID > "$PID_FILE"

echo "Server started successfully!"
echo "Process ID (PID): $SERVER_PID"
echo "Log file: $LOG_FILE"
echo ""
echo "To view logs in real-time, run:"
echo "  tail -f $LOG_FILE"
echo ""
echo "To stop the server, run:"
echo "  ./stop_server.sh"
echo ""
