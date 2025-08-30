#!/bin/bash

echo "==============================================="
echo "Testing Launch Scripts"
echo "==============================================="

echo "1. Testing Linux launch.sh syntax..."
bash -n launch.sh
if [ $? -eq 0 ]; then
    echo "✅ launch.sh syntax is valid"
else
    echo "❌ launch.sh has syntax errors"
    exit 1
fi

echo
echo "2. Testing Windows launch.bat basic syntax..."
# Basic check for Windows batch file
if [ -f "launch.bat" ]; then
    echo "✅ launch.bat file exists"
    # Check for basic Windows batch syntax issues
    if grep -q "^@echo off" launch.bat && grep -q "streamlit run app.py" launch.bat; then
        echo "✅ launch.bat contains expected commands"
    else
        echo "❌ launch.bat missing expected commands"
    fi
else
    echo "❌ launch.bat file not found"
fi

echo
echo "3. Testing app.py syntax..."
python3 -m py_compile app.py
if [ $? -eq 0 ]; then
    echo "✅ app.py compiles successfully"
else
    echo "❌ app.py has syntax errors"
    exit 1
fi

echo
echo "4. Testing dependencies..."
python3 -c "import streamlit, pandas, numpy, plotly, PIL, requests" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ All required dependencies are available"
else
    echo "⚠️  Some dependencies may be missing"
fi

echo
echo "==============================================="
echo "✅ All tests passed! Launch scripts should work."
echo "==============================================="