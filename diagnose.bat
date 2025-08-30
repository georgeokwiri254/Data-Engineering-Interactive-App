@echo off
cls
echo ============================================
echo Data Engineering App Diagnostics
echo ============================================
echo.

echo [1/6] Checking current directory...
echo Current directory: %CD%
if exist "app.py" (
    echo ✓ app.py found
) else (
    echo ✗ app.py NOT found
    echo ERROR: Please run this from the correct directory
    pause
    exit /b 1
)
echo.

echo [2/6] Checking Python installation...
python --version 2>nul
if errorlevel 1 (
    echo ✗ Python NOT found in PATH
    echo ERROR: Install Python from https://python.org
    pause
    exit /b 1
) else (
    echo ✓ Python found
)
echo.

echo [3/6] Checking pip...
pip --version 2>nul
if errorlevel 1 (
    echo ✗ pip NOT found
) else (
    echo ✓ pip found
)
echo.

echo [4/6] Testing Python imports...
python -c "import sys; print('Python path:', sys.executable)" 2>nul
python -c "import streamlit; print('✓ streamlit version:', streamlit.__version__)" 2>nul || echo "✗ streamlit not available"
python -c "import pandas; print('✓ pandas available')" 2>nul || echo "✗ pandas not available"
python -c "import numpy; print('✓ numpy available')" 2>nul || echo "✗ numpy not available"
python -c "import plotly; print('✓ plotly available')" 2>nul || echo "✗ plotly not available"
echo.

echo [5/6] Testing app.py syntax...
python -m py_compile app.py 2>nul
if errorlevel 1 (
    echo ✗ app.py has syntax errors
    python -c "import ast; ast.parse(open('app.py').read())"
) else (
    echo ✓ app.py syntax is valid
)
echo.

echo [6/6] Testing streamlit command...
where streamlit 2>nul
if errorlevel 1 (
    echo ✗ streamlit command not found
    echo Trying: python -m streamlit
    python -m streamlit --version 2>nul || echo "✗ streamlit module not found"
) else (
    echo ✓ streamlit command available
    streamlit --version 2>nul
)
echo.

echo ============================================
echo Diagnostics complete. 
echo If all items show ✓, try the simple launcher:
echo   run.bat
echo ============================================
pause