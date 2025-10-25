"""
KIMBALL Acquire Phase

This module handles data acquisition from various sources:
- Applications (ERP, CRM, EPM)
- Databases (Oracle, SQL Server, Postgres, Redshift, MongoDB, etc.)
- APIs (REST, GraphQL, etc.)
- Storage containers (S3, Google Storage, Dropbox, Azure Storage, etc.)

The goal is to flatten data into ClickHouse bronze layer with minimal transformation.
"""

from .connectors import DatabaseConnector, APIConnector, StorageConnector
from .extractors import DataExtractor
from .transformers import DataTransformer
from .loaders import BronzeLoader

__all__ = [
    'DatabaseConnector',
    'APIConnector', 
    'StorageConnector',
    'DataExtractor',
    'DataTransformer',
    'BronzeLoader'
]
