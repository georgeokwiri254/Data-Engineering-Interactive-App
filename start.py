#!/usr/bin/env python3
"""
Ultra Simple Launcher for Data Engineering App
Just run: python start.py
"""

import os
import sys
import subprocess

def main():
    print("🚀 Starting Data Engineering App...")
    
    # Check if app.py exists
    if not os.path.exists("app.py"):
        print("❌ Error: app.py not found in current directory")
        input("Press Enter to exit...")
        return
    
    # Install streamlit if needed
    try:
        import streamlit
    except ImportError:
        print("📦 Installing Streamlit...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit", "pandas", "numpy", "plotly", "pillow", "requests"])
        print("✅ Streamlit installed!")
    
    # Start the app
    print("⚡ Launching app...")
    print("The app will open in your browser")
    print("Press Ctrl+C to stop")
    print("=" * 40)
    
    subprocess.call([sys.executable, "-m", "streamlit", "run", "app.py"])
    print("✅ App stopped")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        input("Press Enter to exit...")