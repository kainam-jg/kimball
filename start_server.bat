@echo off
echo Starting KIMBALL v2.0 FastAPI Server...
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ and try again
    pause
    exit /b 1
)

echo.
echo Starting FastAPI server with hot reloading...
echo Server: http://localhost:8000
echo API Docs: http://localhost:8000/docs
echo Alternative Docs: http://localhost:8000/redoc
echo.
echo Press Ctrl+C to stop the server
echo.

REM Start server directly (simple approach)
uvicorn kimball.api.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir kimball --log-level info
