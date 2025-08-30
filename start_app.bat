@echo off
cls
echo ============================================
echo Data Engineering App Launcher (Simple)
echo ============================================
echo.

REM Kill any existing streamlit processes
echo Stopping any running Streamlit processes...
taskkill /F /IM python.exe /FI "WINDOWTITLE eq streamlit*" >nul 2>&1
taskkill /F /IM python.exe /FI "COMMANDLINE eq *streamlit*" >nul 2>&1
timeout /t 2 >nul

REM Check if Python is installed
echo Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python from https://python.org
    pause
    exit /b 1
)

REM Check if app.py exists
if not exist "app.py" (
    echo ERROR: app.py not found in current directory
    echo Current directory: %CD%
    pause
    exit /b 1
)

REM Install streamlit if needed
echo Checking Streamlit installation...
python -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo Installing Streamlit...
    pip install streamlit pandas numpy plotly pillow requests
    if errorlevel 1 (
        echo ERROR: Failed to install Streamlit
        pause
        exit /b 1
    )
)

echo.
echo Starting the Data Engineering App...
echo The app will open in your browser automatically.
echo.
echo IMPORTANT: Do NOT close this window while using the app
echo To stop the app, press Ctrl+C or close this window
echo.
echo ============================================

REM Start streamlit with basic settings
streamlit run app.py --server.headless false --server.runOnSave false

echo.
echo App has stopped.
pause