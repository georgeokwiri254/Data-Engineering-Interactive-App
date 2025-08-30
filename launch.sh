#!/bin/bash

echo "==============================================="
echo "Data Engineering Interactive App Launcher"
echo "==============================================="
echo

# Function to kill processes on a specific port
kill_port_process() {
    local port=$1
    echo "Checking port $port..."
    
    # Kill streamlit processes first
    pkill -f "streamlit.*$port" 2>/dev/null || true
    
    # Method 1: Using lsof
    if command -v lsof >/dev/null 2>&1; then
        local pids=$(lsof -ti:$port 2>/dev/null)
        if [ -n "$pids" ]; then
            echo "Found processes on port $port: $pids"
            for pid in $pids; do
                echo "Killing process $pid on port $port"
                kill -TERM $pid 2>/dev/null || true
                sleep 1
                # Force kill if still running
                if kill -0 $pid 2>/dev/null; then
                    kill -KILL $pid 2>/dev/null || true
                    echo "Force killed process $pid"
                fi
            done
            return 0
        fi
    fi
    
    # Method 2: Using ss (more modern)
    if command -v ss >/dev/null 2>&1; then
        local pids=$(ss -tlnp | grep ":$port " | sed 's/.*pid=\([0-9]*\).*/\1/' | grep -E '^[0-9]+$')
        if [ -n "$pids" ]; then
            echo "Found processes via ss on port $port: $pids"
            for pid in $pids; do
                if [ "$pid" -gt 0 ] 2>/dev/null; then
                    echo "Killing process $pid on port $port"
                    kill -TERM $pid 2>/dev/null || true
                    sleep 1
                    if kill -0 $pid 2>/dev/null; then
                        kill -KILL $pid 2>/dev/null || true
                    fi
                fi
            done
            return 0
        fi
    fi
    
    # Method 3: Using netstat (fallback)
    if command -v netstat >/dev/null 2>&1; then
        local netstat_pids=$(netstat -tlnp 2>/dev/null | grep ":$port " | awk '{print $7}' | cut -d'/' -f1 | grep -E '^[0-9]+$')
        if [ -n "$netstat_pids" ]; then
            echo "Found processes via netstat on port $port: $netstat_pids"
            for pid in $netstat_pids; do
                if [ "$pid" != "-" ] && [ "$pid" -gt 0 ] 2>/dev/null; then
                    echo "Killing process $pid on port $port"
                    kill -TERM $pid 2>/dev/null || true
                    sleep 1
                    if kill -0 $pid 2>/dev/null; then
                        kill -KILL $pid 2>/dev/null || true
                    fi
                fi
            done
            return 0
        fi
    fi
    
    return 1
}

# Function to check if port is available
is_port_available() {
    local port=$1
    
    # Method 1: Using ss
    if command -v ss >/dev/null 2>&1; then
        ! ss -tln | grep -q ":$port "
        return $?
    fi
    
    # Method 2: Using netstat
    if command -v netstat >/dev/null 2>&1; then
        ! netstat -tln 2>/dev/null | grep -q ":$port "
        return $?
    fi
    
    # Method 3: Using nc (netcat) - not always available
    if command -v nc >/dev/null 2>&1; then
        ! nc -z localhost $port 2>/dev/null
        return $?
    fi
    
    # Fallback - assume available
    return 0
}

# Function to find available port
find_available_port() {
    local start_port=$1
    local max_attempts=10
    
    for ((i=0; i<max_attempts; i++)); do
        local port=$((start_port + i))
        if is_port_available $port; then
            echo $port
            return 0
        fi
    done
    
    echo "8501"  # fallback
    return 1
}

echo "Cleaning up existing Streamlit processes..."

# Kill all streamlit processes first
pkill -f streamlit 2>/dev/null || true
pkill -f "python.*streamlit" 2>/dev/null || true
sleep 2

# Clean up specific ports
for port in 8501 8502 8503 8504 8505 8506 8507 8508 8509 8510; do
    kill_port_process $port
done

echo "Port cleanup completed."
echo "Waiting for ports to be fully released..."
sleep 3

# Find available port
echo "Finding available port..."
AVAILABLE_PORT=$(find_available_port 8501)
echo "Will use port: $AVAILABLE_PORT"
echo

echo "Checking dependencies..."

# Check if Python is available
PYTHON_CMD=""
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "Error: Python not found. Please install Python."
    exit 1
fi

echo "Using Python: $PYTHON_CMD"

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD --version 2>&1)
echo "Python version: $PYTHON_VERSION"

# Check if required packages are installed
$PYTHON_CMD -c "import streamlit, pandas, numpy, plotly, PIL, requests" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Missing dependencies detected. Installing required packages..."
    if [ -f "requirements.txt" ]; then
        echo "Installing from requirements.txt..."
        pip3 install -r requirements.txt || pip install -r requirements.txt
    else
        echo "requirements.txt not found. Installing common dependencies..."
        pip3 install streamlit pandas numpy plotly pillow requests matplotlib seaborn || \
        pip install streamlit pandas numpy plotly pillow requests matplotlib seaborn
    fi
    
    # Verify installation
    $PYTHON_CMD -c "import streamlit, pandas, numpy, plotly, PIL, requests" 2>/dev/null
    if [ $? -ne 0 ]; then
        echo "Error: Failed to install required dependencies."
        exit 1
    fi
else
    echo "All dependencies are already installed."
fi

echo
echo "Launching Data Engineering Interactive App..."
echo "The app will be available at http://localhost:$AVAILABLE_PORT"
echo "Press Ctrl+C to stop the application."
echo "==============================================="
echo

# Check if app.py exists
if [ ! -f "app.py" ]; then
    echo "Error: app.py not found in current directory"
    echo "Current directory: $(pwd)"
    echo "Files in directory:"
    ls -la
    exit 1
fi

echo "Found app.py, starting Streamlit..."

# Set Streamlit configuration to prevent issues
export STREAMLIT_SERVER_PORT=$AVAILABLE_PORT
export STREAMLIT_SERVER_HEADLESS=true
export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
export STREAMLIT_SERVER_ENABLE_CORS=false
export STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false

# Create a trap to clean up on exit
cleanup() {
    echo
    echo "Shutting down Streamlit server..."
    kill_port_process $AVAILABLE_PORT
    pkill -f streamlit 2>/dev/null || true
    pkill -f "python.*app.py" 2>/dev/null || true
    echo "Cleanup completed."
    exit 0
}

trap cleanup SIGINT SIGTERM EXIT

# Launch the Streamlit app with specific port and better error handling
echo "Starting Streamlit on port $AVAILABLE_PORT..."
streamlit run app.py \
    --server.port $AVAILABLE_PORT \
    --server.headless true \
    --server.runOnSave true \
    --server.enableWebsocketCompression false \
    2>&1 | tee streamlit.log

# If we reach here, streamlit exited
echo "Streamlit has stopped."