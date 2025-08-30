@echo off
setlocal enabledelayedexpansion

echo ===============================================
echo Data Engineering Interactive App Launcher
echo ===============================================
echo.

REM Function to kill processes on a specific port
call :kill_port_process 8501
call :kill_port_process 8502
call :kill_port_process 8503
call :kill_port_process 8504
call :kill_port_process 8505

echo Port cleanup completed.
echo Waiting for ports to be fully released...
timeout /t 3 /nobreak >nul

REM Find available port
call :find_available_port

echo Will use port: !AVAILABLE_PORT!
echo.

echo Checking dependencies...

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Python not found. Please install Python.
    pause
    exit /b 1
)

REM Check if required packages are installed
python -c "import streamlit, pandas, numpy, plotly, PIL, requests" >nul 2>&1
if %errorlevel% neq 0 (
    echo Missing dependencies detected. Installing required packages...
    if exist requirements.txt (
        pip install -r requirements.txt
    ) else (
        echo requirements.txt not found. Installing common dependencies...
        pip install streamlit pandas numpy plotly pillow requests matplotlib seaborn
    )
) else (
    echo All dependencies are already installed.
)

echo.
echo Launching Data Engineering Interactive App...
echo The app will be available at http://localhost:!AVAILABLE_PORT!
echo Press Ctrl+C to stop the application.
echo ===============================================
echo.

REM Check if app.py exists
if not exist app.py (
    echo Error: app.py not found in current directory
    echo Current directory: %CD%
    echo Files in directory:
    dir /b
    pause
    exit /b 1
)

REM Launch the Streamlit app with specific port
streamlit run app.py --server.port !AVAILABLE_PORT! --server.headless true 2>&1

pause
goto :eof

REM Function to kill process on a specific port
:kill_port_process
set port=%1
echo Checking port %port%...
for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr :%port% ^| findstr LISTENING 2^>nul') do (
    echo Killing process %%a on port %port%
    taskkill /f /pid %%a >nul 2>&1
    timeout /t 1 /nobreak >nul
)
goto :eof

REM Function to find available port
:find_available_port
set /a test_port=8501
:port_loop
netstat -an 2>nul | findstr :!test_port! >nul 2>&1
if !errorlevel! neq 0 (
    set AVAILABLE_PORT=!test_port!
    goto :eof
)
set /a test_port+=1
if !test_port! leq 8510 goto port_loop
set AVAILABLE_PORT=8501
goto :eof