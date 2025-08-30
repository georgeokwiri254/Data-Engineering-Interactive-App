#!/usr/bin/env python3
"""
Data Engineering Interactive App Launcher
A cross-platform Python launcher for the Streamlit app
"""

import os
import sys
import subprocess
import time
import socket
import signal
import platform
import psutil
from pathlib import Path

class AppLauncher:
    def __init__(self):
        self.app_name = "Data Engineering Interactive App"
        self.app_file = "app.py"
        self.default_port = 8501
        self.max_port_attempts = 10
        self.streamlit_process = None
        
    def print_banner(self):
        print("=" * 50)
        print(f"🚀 {self.app_name} Launcher")
        print("=" * 50)
        print()
    
    def check_python(self):
        """Check if Python is properly installed"""
        print("🐍 Checking Python installation...")
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        print(f"   ✅ Python {python_version} found")
        return True
    
    def check_app_file(self):
        """Check if app.py exists"""
        print(f"📄 Checking for {self.app_file}...")
        if not Path(self.app_file).exists():
            print(f"   ❌ {self.app_file} not found in current directory")
            print(f"   📁 Current directory: {os.getcwd()}")
            return False
        print(f"   ✅ {self.app_file} found")
        return True
    
    def check_dependencies(self):
        """Check and install required dependencies"""
        print("📦 Checking dependencies...")
        
        required_packages = [
            'streamlit',
            'pandas', 
            'numpy',
            'plotly',
            'pillow',
            'requests'
        ]
        
        missing_packages = []
        
        for package in required_packages:
            try:
                if package == 'pillow':
                    __import__('PIL')
                else:
                    __import__(package)
                print(f"   ✅ {package}")
            except ImportError:
                missing_packages.append(package)
                print(f"   ❌ {package} (missing)")
        
        if missing_packages:
            print(f"\n📥 Installing missing packages: {', '.join(missing_packages)}")
            try:
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install"
                ] + missing_packages)
                print("   ✅ All packages installed successfully")
                return True
            except subprocess.CalledProcessError as e:
                print(f"   ❌ Failed to install packages: {e}")
                return False
        
        print("   ✅ All dependencies available")
        return True
    
    def kill_existing_processes(self):
        """Kill any existing Streamlit processes"""
        print("🔄 Cleaning up existing Streamlit processes...")
        
        killed_count = 0
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline', [])
                if cmdline and any('streamlit' in str(arg).lower() for arg in cmdline):
                    proc.kill()
                    killed_count += 1
                    print(f"   🔫 Killed process {proc.info['pid']}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        if killed_count > 0:
            time.sleep(2)  # Wait for processes to fully terminate
            print(f"   ✅ Cleaned up {killed_count} processes")
        else:
            print("   ✅ No existing processes found")
    
    def find_free_port(self):
        """Find an available port starting from default_port"""
        print("🔍 Finding available port...")
        
        for i in range(self.max_port_attempts):
            port = self.default_port + i
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                result = sock.connect_ex(('localhost', port))
                if result != 0:  # Port is free
                    print(f"   ✅ Port {port} is available")
                    return port
                else:
                    print(f"   ⚠️  Port {port} is busy")
        
        print(f"   ❌ No free ports found in range {self.default_port}-{self.default_port + self.max_port_attempts}")
        return self.default_port  # Fallback to default
    
    def start_streamlit(self, port):
        """Start the Streamlit application"""
        print(f"🚀 Starting {self.app_name}...")
        print(f"   📍 Port: {port}")
        print(f"   🌐 Local URL: http://localhost:{port}")
        
        # Determine the correct way to get network IP
        try:
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            print(f"   🌍 Network URL: http://{local_ip}:{port}")
        except:
            pass
        
        print()
        print("⚡ Starting Streamlit server...")
        print("   Press Ctrl+C to stop the application")
        print("=" * 50)
        print()
        
        # Start Streamlit
        cmd = [
            sys.executable, "-m", "streamlit", "run", self.app_file,
            "--server.port", str(port),
            "--server.headless", "true",
            "--server.runOnSave", "true",
            "--server.enableWebsocketCompression", "false"
        ]
        
        try:
            self.streamlit_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            # Monitor the process
            while True:
                output = self.streamlit_process.stdout.readline()
                if output:
                    print(output.rstrip())
                elif self.streamlit_process.poll() is not None:
                    break
                    
        except KeyboardInterrupt:
            print("\n🛑 Stopping application...")
            self.cleanup()
        except Exception as e:
            print(f"❌ Error starting Streamlit: {e}")
            return False
            
        return True
    
    def cleanup(self):
        """Clean up processes on exit"""
        if self.streamlit_process:
            print("   🧹 Terminating Streamlit process...")
            try:
                self.streamlit_process.terminate()
                self.streamlit_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("   🔫 Force killing Streamlit process...")
                self.streamlit_process.kill()
            except:
                pass
        
        print("   ✅ Cleanup completed")
    
    def run(self):
        """Main launcher function"""
        try:
            self.print_banner()
            
            # Pre-flight checks
            if not self.check_python():
                return False
                
            if not self.check_app_file():
                return False
                
            if not self.check_dependencies():
                return False
            
            # Cleanup and prepare
            self.kill_existing_processes()
            port = self.find_free_port()
            
            # Launch the application
            return self.start_streamlit(port)
            
        except KeyboardInterrupt:
            print("\n🛑 Launcher interrupted by user")
            self.cleanup()
            return True
        except Exception as e:
            print(f"❌ Launcher error: {e}")
            return False
        finally:
            self.cleanup()

def main():
    """Entry point"""
    launcher = AppLauncher()
    success = launcher.run()
    
    if not success:
        print("\n❌ Failed to start the application")
        input("Press Enter to exit...")
        sys.exit(1)
    else:
        print("\n✅ Application stopped successfully")

if __name__ == "__main__":
    main()