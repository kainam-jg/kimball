#!/usr/bin/env python3
"""
KIMBALL Test Runner

Simple script to run all tests from the project root.

Usage:
    python run_tests.py
"""

import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

if __name__ == "__main__":
    # Import and run the test suite
    from tests.test_suite import main
    sys.exit(main())
