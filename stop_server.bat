@echo off
echo Stopping KIMBALL v2.0 FastAPI Server...
echo.

REM Find and kill uvicorn processes
for /f "tokens=2" %%i in ('tasklist /fi "imagename eq python.exe" /fo table /nh ^| findstr uvicorn') do (
    echo Stopping process %%i...
    taskkill /pid %%i /f
)

REM Alternative method - kill by port
echo Checking for processes on port 8000...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8000') do (
    echo Stopping process on port 8000...
    taskkill /pid %%a /f
)

echo.
echo Server stopped successfully!
echo.
pause
