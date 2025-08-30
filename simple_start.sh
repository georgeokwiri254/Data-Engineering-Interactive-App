#!/bin/bash

echo "Simple Data Engineering App Launcher"
echo "===================================="

# Kill existing processes
pkill -f streamlit 2>/dev/null || true
sleep 2

# Check if app exists
if [ ! -f "app.py" ]; then
    echo "ERROR: app.py not found"
    exit 1
fi

# Check Python
if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 not found"
    exit 1
fi

echo "Starting app on port 8505..."
python3 -m streamlit run app.py --server.port 8505