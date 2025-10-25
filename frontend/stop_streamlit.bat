@echo off
echo Stopping KIMBALL v2.0 Streamlit Frontend...
echo.

REM Find and kill streamlit processes
for /f "tokens=2" %%i in ('tasklist /fi "imagename eq python.exe" /fo table /nh ^| findstr streamlit') do (
    echo Stopping Streamlit process %%i...
    taskkill /pid %%i /f
)

REM Alternative method - kill by port
echo Checking for processes on port 8501...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8501') do (
    echo Stopping process on port 8501...
    taskkill /pid %%a /f
)

REM Also kill any remaining streamlit processes
taskkill /f /im streamlit.exe 2>nul

echo.
echo Streamlit frontend stopped successfully!
echo.
pause
