"""
KIMBALL Acquire Phase - MINIMAL VERSION FOR TESTING

This module provides minimal functionality for systematic testing.
All unused imports and functionality are commented out.
"""

# WORKING IMPORTS - Only what we're currently testing
from .connectors import DatabaseConnector, APIConnector
from .bucket_processor import S3DataProcessor
from .source_manager import DataSourceManager

# COMMENTED OUT UNUSED IMPORTS FOR SYSTEMATIC TESTING
# from .transformers import DataTransformer
# from .loaders import BronzeLoader
# from .bucket_processor import ClickHouseStreamLoader

__all__ = [
    'DatabaseConnector',
    'APIConnector', 
    'S3DataProcessor',
    'DataSourceManager'
    # 'DataTransformer',
    # 'BronzeLoader',
    # 'ClickHouseStreamLoader'
]