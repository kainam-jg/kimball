@echo off
echo Starting KIMBALL v2.0 FastAPI Server (Production Mode)...
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ and try again
    pause
    exit /b 1
)

REM Check if virtual environment exists
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo ERROR: Failed to create virtual environment
        pause
        exit /b 1
    )
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

REM Install dependencies
echo Installing dependencies...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

REM Start FastAPI server in production mode
echo.
echo ========================================
echo KIMBALL v2.0 FastAPI Production Server
echo ========================================
echo.
echo Server URLs:
echo - Main Server: http://localhost:8000
echo - API Documentation: http://localhost:8000/docs
echo - Health Check: http://localhost:8000/health
echo.
echo Production Features:
echo - Optimized for performance
echo - No hot reloading (stable)
echo - Production logging level
echo - Error handling optimized
echo - Ready for deployment
echo.
echo Press Ctrl+C to stop the server
echo ========================================
echo.
uvicorn kimball.api.main:app --host 0.0.0.0 --port 8000 --workers 4 --log-level info

pause
