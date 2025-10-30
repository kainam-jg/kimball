@echo off
echo Stopping KIMBALL v2.0 FastAPI Server...
echo.

REM Method 1: Kill processes on port 8000
echo Checking for processes on port 8000...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8000') do (
    echo Stopping process %%a on port 8000...
    taskkill /pid %%a /f >nul 2>&1
)

REM Method 2: Kill all Python processes (more aggressive)
echo Stopping all Python processes...
taskkill /f /im python.exe >nul 2>&1

echo.
echo Server stopped successfully!
echo.
