#!/usr/bin/env python3
"""
Test S3 connection with the provided credentials
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from kimball.acquire.source_manager import DataSourceManager
from kimball.core.logger import Logger

def test_s3_connection():
    """Test S3 connection with vehicle sales data bucket."""
    logger = Logger("test_s3")
    
    print("Testing S3 connection to kimball-data bucket...")
    print("=" * 50)
    
    # Initialize source manager
    manager = DataSourceManager()
    
    # Test S3 connection
    source_id = "s3_vehicle_sales"
    
    print(f"1. Testing connection to {source_id}...")
    if manager.test_source_connection(source_id):
        print("✅ S3 connection test successful!")
    else:
        print("❌ S3 connection test failed!")
        return False
    
    print(f"\n2. Getting schema information for {source_id}...")
    schema = manager.get_source_schema(source_id)
    if schema and "files" in schema:
        files = schema["files"]
        print(f"✅ Found {len(files)} files in bucket")
        
        # Show first few files
        if files:
            print("\nFirst 5 files:")
            for i, file in enumerate(files[:5]):
                print(f"  {i+1}. {file}")
            if len(files) > 5:
                print(f"  ... and {len(files) - 5} more files")
        
        # Filter for vehicle sales data
        vehicle_files = [f for f in files if "vehicle" in f.lower() or "sales" in f.lower()]
        if vehicle_files:
            print(f"\nVehicle sales related files ({len(vehicle_files)}):")
            for file in vehicle_files[:10]:  # Show first 10
                print(f"  - {file}")
    else:
        print("❌ Failed to get schema information")
        return False
    
    print(f"\n3. Testing data extraction from {source_id}...")
    try:
        # Try to extract data from the first file
        if files:
            first_file = files[0]
            print(f"   Attempting to extract from: {first_file}")
            
            result = manager.extract_data(source_id, file_pattern=first_file)
            if result and result.get("record_count", 0) > 0:
                print(f"✅ Successfully extracted {result['record_count']} records")
                print(f"   Columns: {result.get('columns', [])}")
            else:
                print("⚠️  No data extracted (file might be empty or unsupported format)")
        else:
            print("⚠️  No files found to extract from")
    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("✅ S3 connection test completed successfully!")
    return True

def test_s3_api_endpoints():
    """Test S3-related API endpoints."""
    import requests
    
    base_url = "http://localhost:8000"
    source_id = "s3_vehicle_sales"
    
    print("\nTesting S3 API endpoints...")
    print("=" * 50)
    
    # Test 1: Get acquire status
    try:
        response = requests.get(f"{base_url}/api/v1/acquire/status")
        if response.status_code == 200:
            print("✅ Acquire status endpoint working")
        else:
            print(f"❌ Acquire status failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error testing acquire status: {e}")
        return False
    
    # Test 2: List sources
    try:
        response = requests.get(f"{base_url}/api/v1/acquire/sources")
        if response.status_code == 200:
            data = response.json()
            if source_id in data.get("sources", {}):
                print("✅ S3 source found in configured sources")
            else:
                print("❌ S3 source not found in configured sources")
                return False
        else:
            print(f"❌ List sources failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error testing list sources: {e}")
        return False
    
    # Test 3: Test connection
    try:
        response = requests.get(f"{base_url}/api/v1/acquire/test/{source_id}")
        if response.status_code == 200:
            data = response.json()
            if data.get("connection_test") == "success":
                print("✅ S3 connection test via API successful")
            else:
                print("❌ S3 connection test via API failed")
                return False
        else:
            print(f"❌ Connection test failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False
    
    # Test 4: Get schema
    try:
        response = requests.get(f"{base_url}/api/v1/acquire/sources/{source_id}/schema")
        if response.status_code == 200:
            data = response.json()
            if "files" in data.get("schema", {}):
                print("✅ S3 schema retrieval successful")
            else:
                print("❌ S3 schema retrieval failed")
                return False
        else:
            print(f"❌ Schema retrieval failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error testing schema retrieval: {e}")
        return False
    
    print("✅ All S3 API endpoint tests passed!")
    return True

if __name__ == "__main__":
    print("KIMBALL S3 Connection Test Suite")
    print("=" * 60)
    
    # Test 1: Direct S3 connection
    print("Test 1: Direct S3 Connection")
    success1 = test_s3_connection()
    
    # Test 2: API endpoints (requires FastAPI server running)
    print("\nTest 2: S3 API Endpoints")
    print("Note: This test requires the FastAPI server to be running")
    print("Start server with: start_server.bat")
    
    try:
        success2 = test_s3_api_endpoints()
    except Exception as e:
        print(f"⚠️  API test skipped (server not running): {e}")
        success2 = True  # Don't fail if server isn't running
    
    if success1 and success2:
        print("\n🎉 All S3 tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some S3 tests failed!")
        sys.exit(1)
