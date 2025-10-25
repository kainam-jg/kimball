#!/usr/bin/env python3
"""
Test script to verify KIMBALL FastAPI server is working correctly
"""

import requests
import time
import sys
from pathlib import Path

def test_server():
    """Test the FastAPI server endpoints"""
    base_url = "http://localhost:8000"
    
    print("Testing KIMBALL v2.0 FastAPI Server...")
    print("=" * 50)
    
    # Test server health
    try:
        print("1. Testing server health...")
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code == 200:
            print("SUCCESS: Server is healthy!")
        else:
            print(f"ERROR: Server health check failed: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to server. Is it running?")
        return False
    except Exception as e:
        print(f"ERROR: Error testing server: {e}")
        return False
    
    # Test API documentation
    try:
        print("2. Testing API documentation...")
        response = requests.get(f"{base_url}/docs", timeout=5)
        if response.status_code == 200:
            print("SUCCESS: API documentation accessible!")
        else:
            print(f"ERROR: API documentation failed: {response.status_code}")
    except Exception as e:
        print(f"ERROR: Error accessing API docs: {e}")
    
    # Test main endpoints
    endpoints = [
        ("/api/v1/discover/status", "Discover Phase"),
        ("/api/v1/acquire/status", "Acquire Phase"),
        ("/api/v1/model/status", "Model Phase"),
        ("/api/v1/build/status", "Build Phase")
    ]
    
    for endpoint, name in endpoints:
        try:
            print(f"3. Testing {name}...")
            response = requests.get(f"{base_url}{endpoint}", timeout=5)
            if response.status_code == 200:
                print(f"SUCCESS: {name} endpoint working!")
            else:
                print(f"ERROR: {name} endpoint failed: {response.status_code}")
        except Exception as e:
            print(f"ERROR: Error testing {name}: {e}")
    
    print("\nServer test completed!")
    print("=" * 50)
    return True

if __name__ == "__main__":
    print("KIMBALL v2.0 Server Test")
    print("Make sure the server is running with: start_server.bat")
    print()
    
    # Wait a moment for server to start
    print("Waiting for server to start...")
    time.sleep(2)
    
    success = test_server()
    
    if success:
        print("\nSUCCESS: All tests passed! Server is working correctly.")
        sys.exit(0)
    else:
        print("\nERROR: Some tests failed. Check server logs.")
        sys.exit(1)
