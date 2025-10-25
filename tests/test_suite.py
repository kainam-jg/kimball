#!/usr/bin/env python3
"""
KIMBALL Comprehensive Test Suite

This script runs all tests for the KIMBALL platform:
- Acquire Phase Tests
- FastAPI Server Tests
- Streamlit Frontend Tests
- Discover Phase Tests

Usage:
    python tests/test_suite.py
"""

import sys
import os
import subprocess
from pathlib import Path
from datetime import datetime

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def run_test(test_name: str, test_file: str) -> tuple[bool, str]:
    """Run a specific test and return results."""
    try:
        print(f"\n{'='*60}")
        print(f"Running {test_name}")
        print(f"{'='*60}")
        
        result = subprocess.run(
            [sys.executable, test_file],
            capture_output=True,
            text=True,
            cwd=project_root
        )
        
        success = result.returncode == 0
        output = result.stdout + result.stderr
        
        if success:
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
            print(f"Error: {result.stderr}")
        
        return success, output
        
    except Exception as e:
        print(f"❌ {test_name} ERROR: {e}")
        return False, str(e)

def main():
    """Run the complete test suite."""
    print("🚀 KIMBALL Comprehensive Test Suite")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Project root: {project_root}")
    
    # Define test suite
    tests = [
        ("FastAPI Server Test", "tests/test_server.py"),
        ("Streamlit Frontend Test", "tests/test_streamlit.py"),
        ("Acquire Phase Test", "tests/test_acquire.py"),
        ("Discover Phase Test", "tests/test_discover.py")
    ]
    
    results = []
    
    # Run each test
    for test_name, test_file in tests:
        if os.path.exists(test_file):
            success, output = run_test(test_name, test_file)
            results.append((test_name, success, output))
        else:
            print(f"⚠️  {test_name}: Test file not found ({test_file})")
            results.append((test_name, False, "Test file not found"))
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUITE SUMMARY")
    print(f"{'='*60}")
    
    passed = sum(1 for _, success, _ in results if success)
    total = len(results)
    
    for test_name, success, _ in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    print(f"\n📊 Overall Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! KIMBALL platform is ready for use.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
