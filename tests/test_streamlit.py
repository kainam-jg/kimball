#!/usr/bin/env python3
"""
Test script to verify KIMBALL Streamlit frontend is working correctly
"""

import requests
import time
import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

def test_streamlit():
    """Test the Streamlit frontend"""
    base_url = "http://localhost:8501"
    
    print("Testing KIMBALL v2.0 Streamlit Frontend...")
    print("=" * 50)
    
    # Test Streamlit health
    try:
        print("1. Testing Streamlit frontend...")
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            print("SUCCESS: Streamlit frontend is accessible!")
        else:
            print(f"ERROR: Streamlit frontend failed: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to Streamlit. Is it running?")
        return False
    except Exception as e:
        print(f"ERROR: Error testing Streamlit: {e}")
        return False
    
    # Test Streamlit health endpoint
    try:
        print("2. Testing Streamlit health endpoint...")
        response = requests.get(f"{base_url}/healthz", timeout=5)
        if response.status_code == 200:
            print("SUCCESS: Streamlit health endpoint working!")
        else:
            print(f"INFO: Streamlit health endpoint returned: {response.status_code}")
    except Exception as e:
        print(f"INFO: Streamlit health endpoint not available: {e}")
    
    print("\nStreamlit frontend test completed!")
    print("=" * 50)
    return True

if __name__ == "__main__":
    print("KIMBALL v2.0 Streamlit Frontend Test")
    print("Make sure the frontend is running with: start_streamlit.bat")
    print()
    
    # Wait a moment for frontend to start
    print("Waiting for frontend to start...")
    time.sleep(2)
    
    success = test_streamlit()
    
    if success:
        print("\nSUCCESS: All tests passed! Streamlit frontend is working correctly.")
        sys.exit(0)
    else:
        print("\nERROR: Some tests failed. Check frontend logs.")
        sys.exit(1)
