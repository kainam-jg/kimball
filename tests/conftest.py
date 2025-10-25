"""
Pytest configuration for KIMBALL test suite.

This module provides shared fixtures and configuration for all tests.
"""

import pytest
import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

@pytest.fixture(scope="session")
def project_root_path():
    """Return the project root path."""
    return project_root

@pytest.fixture(scope="session")
def config_path():
    """Return the path to the config.json file."""
    return project_root / "config.json"

@pytest.fixture(scope="session")
def test_data_path():
    """Return the path to test data directory."""
    return project_root / "tests" / "data"

@pytest.fixture(scope="session")
def fastapi_url():
    """Return the FastAPI server URL."""
    return "http://localhost:8000"

@pytest.fixture(scope="session")
def streamlit_url():
    """Return the Streamlit frontend URL."""
    return "http://localhost:8501"
