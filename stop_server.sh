#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PID_FILE="$SCRIPT_DIR/logs/kimball_server.pid"

echo "Stopping KIMBALL v2.0 FastAPI Server..."
echo ""

# Method 0: Try to stop using PID file first (cleanest method)
if [ -f "$PID_FILE" ]; then
    SERVER_PID=$(cat "$PID_FILE")
    if ps -p "$SERVER_PID" > /dev/null 2>&1; then
        echo "Found server process from PID file: $SERVER_PID"
        echo "Stopping process $SERVER_PID..."
        kill "$SERVER_PID" 2>/dev/null
        # Wait a bit for graceful shutdown
        sleep 2
        # Force kill if still running
        if ps -p "$SERVER_PID" > /dev/null 2>&1; then
            echo "Force stopping process $SERVER_PID..."
            kill -9 "$SERVER_PID" 2>/dev/null
        fi
        # Remove PID file
        rm -f "$PID_FILE"
        echo "Server stopped using PID file"
    else
        echo "PID file exists but process not found. Cleaning up PID file..."
        rm -f "$PID_FILE"
    fi
fi

echo ""

# Method 1: Find and kill processes on port 8000
echo "Checking for processes on port 8000..."

# Try using lsof first (more common on Linux)
if command -v lsof &> /dev/null; then
    PIDS=$(lsof -ti:8000 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo "$PIDS" | while read pid; do
            echo "Stopping process $pid on port 8000..."
            kill -9 "$pid" 2>/dev/null
        done
        echo "Processes on port 8000 stopped"
    else
        echo "No processes found on port 8000"
    fi
# Fallback to netstat/fuser if lsof is not available
elif command -v fuser &> /dev/null; then
    fuser -k 8000/tcp 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "Processes on port 8000 stopped"
    else
        echo "No processes found on port 8000"
    fi
# Fallback to netstat + awk
elif command -v netstat &> /dev/null; then
    PIDS=$(netstat -tlnp 2>/dev/null | grep :8000 | awk '{print $7}' | cut -d'/' -f1 | grep -v '-' | sort -u)
    if [ -n "$PIDS" ]; then
        echo "$PIDS" | while read pid; do
            if [ -n "$pid" ] && [ "$pid" != "-" ]; then
                echo "Stopping process $pid on port 8000..."
                kill -9 "$pid" 2>/dev/null
            fi
        done
        echo "Processes on port 8000 stopped"
    else
        echo "No processes found on port 8000"
    fi
else
    echo "WARNING: Could not find lsof, fuser, or netstat. Trying generic approach..."
    # Generic fallback: try to find uvicorn processes
    PIDS=$(ps aux | grep "[u]vicorn kimball" | awk '{print $2}')
    if [ -n "$PIDS" ]; then
        echo "$PIDS" | while read pid; do
            echo "Stopping uvicorn process $pid..."
            kill -9 "$pid" 2>/dev/null
        done
    fi
fi

# Method 2: Kill specific uvicorn processes for kimball (more targeted)
echo ""
echo "Checking for KIMBALL uvicorn processes..."
PIDS=$(ps aux | grep "[u]vicorn kimball.api.main" | awk '{print $2}')
if [ -n "$PIDS" ]; then
    echo "$PIDS" | while read pid; do
        echo "Stopping KIMBALL server process $pid..."
        kill -9 "$pid" 2>/dev/null
    done
    echo "KIMBALL server processes stopped"
else
    echo "No KIMBALL server processes found"
fi

echo ""
echo "Server stopped successfully!"
echo ""

