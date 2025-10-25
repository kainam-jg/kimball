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
echo Starting server in background...
echo Use 'stop_server.bat' to stop the server
echo.

REM Start server in background (Windows equivalent of nohup)
start /B uvicorn kimball.api.main:app --host 0.0.0.0 --port 8000 --reload --reload-dir kimball --reload-dir frontend --log-level debug

echo FastAPI server started in background!
echo.
echo To check if server is running:
echo   curl http://localhost:8000/health
echo.
echo To stop the server:
echo   stop_server.bat
echo.
echo To view server logs, check the terminal window that opened
echo.

pause
