#!/usr/bin/env python3
"""
Quick S3 test runner - runs only the S3 connection test
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

if __name__ == "__main__":
    # Import and run the S3 test
    from test_s3_connection import test_s3_connection, test_s3_api_endpoints
    
    print("🚀 KIMBALL S3 Test Runner")
    print("=" * 40)
    
    # Run direct S3 test
    print("Running S3 connection test...")
    success1 = test_s3_connection()
    
    if success1:
        print("\n✅ S3 connection test passed!")
        
        # Ask if user wants to test API endpoints
        print("\nTo test API endpoints, make sure the FastAPI server is running:")
        print("Run: start_server.bat")
        print("Then run: python tests/test_s3_connection.py")
    else:
        print("\n❌ S3 connection test failed!")
        sys.exit(1)
