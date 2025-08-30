#!/usr/bin/env python3
"""
Simple Data Engineering App Launcher
Cross-platform Python launcher - no external dependencies required
"""

import os
import sys
import subprocess
import time
import socket
import signal
import platform
from pathlib import Path

def print_banner():
    print("=" * 55)
    print("🚀 Data Engineering Interactive App Launcher")
    print("=" * 55)
    print()

def check_python():
    """Check Python version"""
    print("🐍 Checking Python...")
    version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(f"   ✅ Python {version}")
    return True

def check_app():
    """Check if app.py exists"""
    print("📄 Checking app.py...")
    if not Path("app.py").exists():
        print("   ❌ app.py not found!")
        print(f"   📁 Current directory: {os.getcwd()}")
        return False
    print("   ✅ app.py found")
    return True

def install_streamlit():
    """Install streamlit if needed"""
    print("📦 Checking Streamlit...")
    try:
        import streamlit
        print("   ✅ Streamlit available")
        return True
    except ImportError:
        print("   📥 Installing Streamlit...")
        try:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", 
                "streamlit", "pandas", "numpy", "plotly", "pillow", "requests"
            ])
            print("   ✅ Streamlit installed")
            return True
        except Exception as e:
            print(f"   ❌ Installation failed: {e}")
            return False

def kill_streamlit():
    """Kill existing streamlit processes using basic methods"""
    print("🔄 Cleaning existing processes...")
    
    system = platform.system().lower()
    killed = False
    
    try:
        if system == "windows":
            # Windows
            result = subprocess.run([
                "taskkill", "/F", "/IM", "python.exe", "/FI", "WINDOWTITLE eq *streamlit*"
            ], capture_output=True, text=True)
            subprocess.run([
                "taskkill", "/F", "/IM", "python.exe", "/FI", "COMMANDLINE eq *streamlit*"
            ], capture_output=True, text=True)
            killed = True
        else:
            # Linux/Mac
            subprocess.run(["pkill", "-f", "streamlit"], capture_output=True)
            killed = True
            
        if killed:
            time.sleep(2)
            print("   ✅ Cleanup completed")
        
    except Exception as e:
        print(f"   ⚠️  Cleanup warning: {e}")

def find_free_port(start_port=8501):
    """Find an available port"""
    print("🔍 Finding available port...")
    
    for port in range(start_port, start_port + 10):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                print(f"   ✅ Port {port} available")
                return port
        except OSError:
            print(f"   ⚠️  Port {port} busy")
            continue
    
    print(f"   ⚠️  Using fallback port {start_port}")
    return start_port

def get_network_ip():
    """Get local network IP"""
    try:
        # Connect to a remote address to get local IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except:
        try:
            return socket.gethostbyname(socket.gethostname())
        except:
            return "localhost"

def start_app(port):
    """Start the Streamlit app"""
    print("🚀 Starting Data Engineering App...")
    print(f"   📍 Port: {port}")
    print(f"   🌐 Local: http://localhost:{port}")
    
    try:
        network_ip = get_network_ip()
        if network_ip != "localhost":
            print(f"   🌍 Network: http://{network_ip}:{port}")
    except:
        pass
    
    print()
    print("⚡ Launching Streamlit...")
    print("   The app will open in your browser")
    print("   Press Ctrl+C to stop")
    print("=" * 55)
    print()
    
    # Start Streamlit
    cmd = [
        sys.executable, "-m", "streamlit", "run", "app.py",
        "--server.port", str(port)
    ]
    
    try:
        process = subprocess.Popen(cmd)
        process.wait()
    except KeyboardInterrupt:
        print("\n🛑 Stopping app...")
        try:
            process.terminate()
            process.wait(timeout=5)
        except:
            pass
        print("   ✅ App stopped")

def main():
    """Main function"""
    try:
        print_banner()
        
        # Check requirements
        if not check_python():
            return False
        
        if not check_app():
            return False
            
        if not install_streamlit():
            return False
        
        # Prepare and launch
        kill_streamlit()
        port = find_free_port()
        start_app(port)
        
        return True
        
    except KeyboardInterrupt:
        print("\n🛑 Cancelled by user")
        return True
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        print("\n❌ Launch failed!")
        input("Press Enter to exit...")
        sys.exit(1)
    else:
        print("\n✅ Launcher finished")