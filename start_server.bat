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

REM Start FastAPI server with development options
echo.
echo Starting FastAPI server with hot reloading...
echo Server: http://localhost:8000
echo API Docs: http://localhost:8000/docs
echo Alternative Docs: http://localhost:8000/redoc
echo.
echo Development Features:
echo - Hot reloading enabled (auto-restart on code changes)
echo - Debug mode enabled
echo - Detailed error messages
echo - File watching for all Python files in 'kimball' and 'frontend'
echo.
echo Press Ctrl+C to stop the server
echo.
uvicorn kimball.api.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir kimball --reload-dir frontend --log-level debug

pause
