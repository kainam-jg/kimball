#!/usr/bin/env python3
"""
Quick data sources test runner - tests PostgreSQL and S3 connections
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

if __name__ == "__main__":
    # Import and run the data sources test
    from test_data_sources import test_postgres_connection, test_s3_connection, test_api_endpoints
    
    print("🚀 KIMBALL Data Sources Test Runner")
    print("=" * 50)
    
    # Run PostgreSQL test
    print("Testing PostgreSQL connection...")
    success1 = test_postgres_connection()
    
    if success1:
        print("\n✅ PostgreSQL connection test passed!")
        
        # Run S3 test
        print("\nTesting S3 connection...")
        success2 = test_s3_connection()
        
        if success2:
            print("\n✅ S3 connection test passed!")
            
            # Ask if user wants to test API endpoints
            print("\nTo test API endpoints, make sure the FastAPI server is running:")
            print("Run: start_server.bat")
            print("Then run: python tests/test_data_sources.py")
        else:
            print("\n❌ S3 connection test failed!")
            sys.exit(1)
    else:
        print("\n❌ PostgreSQL connection test failed!")
        sys.exit(1)
