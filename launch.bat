@echo off
echo ===============================================
echo Data Engineering Interactive App Launcher
echo ===============================================

echo Checking and killing processes on common Streamlit ports...

REM Check and kill processes on port 8501 (default Streamlit port)
echo Checking port 8501...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8501 ^| findstr LISTENING') do (
    echo Killing process %%a on port 8501
    taskkill /f /pid %%a >nul 2>&1
)

REM Check and kill processes on port 8502 (alternative Streamlit port)
echo Checking port 8502...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8502 ^| findstr LISTENING') do (
    echo Killing process %%a on port 8502
    taskkill /f /pid %%a >nul 2>&1
)

REM Check and kill processes on port 8503 (another alternative)
echo Checking port 8503...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8503 ^| findstr LISTENING') do (
    echo Killing process %%a on port 8503
    taskkill /f /pid %%a >nul 2>&1
)

echo Port cleanup completed.
echo.

REM Wait a moment for cleanup to complete
timeout /t 2 /nobreak >nul

echo Checking dependencies...

REM Check if required packages are installed
python -c "import streamlit, pandas, numpy, plotly, PIL, requests" >nul 2>&1
if %errorlevel% neq 0 (
    echo Missing dependencies detected. Installing required packages...
    pip install -r requirements.txt
) else (
    echo All dependencies are already installed.
)

echo.
echo Launching Data Engineering Interactive App...
echo The app will open in your default browser.
echo Press Ctrl+C to stop the application.
echo.

REM Launch the Streamlit app
streamlit run app.py

pause