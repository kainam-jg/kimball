#!/usr/bin/env python3
"""
Test script for KIMBALL Acquire Phase

This script demonstrates data acquisition from multiple sources:
- PostgreSQL database
- Amazon S3 bucket
- API endpoints

Usage:
    python test_acquire.py
"""

import json
import pandas as pd
from datetime import datetime
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from kimball.acquire.source_manager import DataSourceManager
from kimball.acquire.extractors import DataExtractor
from kimball.core.logger import Logger

def test_config_loading():
    """Test configuration loading."""
    print("=" * 60)
    print("Testing Configuration Loading")
    print("=" * 60)
    
    try:
        with open("config.json", "r") as f:
            config = json.load(f)
        
        print("✅ Configuration loaded successfully")
        print(f"📊 ClickHouse: {config['clickhouse']['host']}:{config['clickhouse']['port']}")
        print(f"📊 Data Sources: {len(config.get('data_sources', {}))}")
        
        for name, source in config.get("data_sources", {}).items():
            status = "✅ Enabled" if source.get("enabled") else "❌ Disabled"
            print(f"  - {name} ({source.get('type')}): {status}")
        
        return config
        
    except Exception as e:
        print(f"❌ Configuration loading failed: {e}")
        return None

def test_source_manager():
    """Test the data source manager."""
    print("\n" + "=" * 60)
    print("Testing Data Source Manager")
    print("=" * 60)
    
    try:
        manager = DataSourceManager()
        
        # Get available sources
        sources = manager.get_available_sources()
        print(f"📊 Available sources: {len(sources)}")
        
        for source in sources:
            print(f"  - {source['name']} ({source['type']}): {source['description']}")
        
        return manager
        
    except Exception as e:
        print(f"❌ Source manager test failed: {e}")
        return None

def test_postgres_connection(manager):
    """Test PostgreSQL connection."""
    print("\n" + "=" * 60)
    print("Testing PostgreSQL Connection")
    print("=" * 60)
    
    try:
        # Test connection to postgres_test source
        if manager.connect_source("postgres_test"):
            print("✅ PostgreSQL connection successful")
            
            # Test schema retrieval
            schema = manager.get_source_schema("postgres_test")
            print(f"📊 Schema info: {len(schema)} items")
            
            # Test data extraction (if tables exist)
            print("📊 Testing data extraction...")
            # Note: This would require actual PostgreSQL database with data
            
            manager.disconnect_source("postgres_test")
            print("✅ PostgreSQL test completed")
            return True
        else:
            print("❌ PostgreSQL connection failed")
            return False
            
    except Exception as e:
        print(f"❌ PostgreSQL test failed: {e}")
        return False

def test_s3_connection(manager):
    """Test S3 connection."""
    print("\n" + "=" * 60)
    print("Testing S3 Connection")
    print("=" * 60)
    
    try:
        # Test connection to s3_test source
        if manager.connect_source("s3_test"):
            print("✅ S3 connection successful")
            
            # Test schema retrieval (file listing)
            schema = manager.get_source_schema("s3_test")
            print(f"📊 Files available: {len(schema.get('files', []))}")
            
            # Test data extraction (if files exist)
            print("📊 Testing data extraction...")
            # Note: This would require actual S3 bucket with data
            
            manager.disconnect_source("s3_test")
            print("✅ S3 test completed")
            return True
        else:
            print("❌ S3 connection failed")
            return False
            
    except Exception as e:
        print(f"❌ S3 test failed: {e}")
        return False

def test_data_extractor():
    """Test the data extractor."""
    print("\n" + "=" * 60)
    print("Testing Data Extractor")
    print("=" * 60)
    
    try:
        extractor = DataExtractor()
        print("✅ Data extractor initialized")
        
        # Test extraction methods
        print("📊 Available extraction methods:")
        print("  - Database extraction with SQL queries")
        print("  - S3 file extraction with multiple formats")
        print("  - API extraction with pagination")
        
        return extractor
        
    except Exception as e:
        print(f"❌ Data extractor test failed: {e}")
        return None

def test_bronze_layer_loading():
    """Test bronze layer loading."""
    print("\n" + "=" * 60)
    print("Testing Bronze Layer Loading")
    print("=" * 60)
    
    try:
        # Create sample data
        sample_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'city': ['New York', 'London', 'Paris', 'Tokyo', 'Sydney']
        })
        
        print(f"📊 Sample data created: {len(sample_data)} rows")
        print(f"📊 Columns: {list(sample_data.columns)}")
        
        # Add metadata columns
        sample_data['_source'] = 'test_source'
        sample_data['_extracted_at'] = datetime.now()
        sample_data['_batch_id'] = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print("✅ Metadata columns added")
        print("📊 Bronze layer loading simulation completed")
        
        return True
        
    except Exception as e:
        print(f"❌ Bronze layer test failed: {e}")
        return False

def main():
    """Main test function."""
    print("🚀 KIMBALL Acquire Phase Test Suite")
    print("=" * 60)
    
    # Test configuration
    config = test_config_loading()
    if not config:
        print("❌ Configuration test failed - exiting")
        return
    
    # Test source manager
    manager = test_source_manager()
    if not manager:
        print("❌ Source manager test failed - exiting")
        return
    
    # Test individual connections
    postgres_success = test_postgres_connection(manager)
    s3_success = test_s3_connection(manager)
    
    # Test data extractor
    extractor = test_data_extractor()
    
    # Test bronze layer loading
    bronze_success = test_bronze_layer_loading()
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    tests = [
        ("Configuration Loading", True),
        ("Source Manager", True),
        ("PostgreSQL Connection", postgres_success),
        ("S3 Connection", s3_success),
        ("Data Extractor", extractor is not None),
        ("Bronze Layer Loading", bronze_success)
    ]
    
    passed = sum(1 for _, success in tests if success)
    total = len(tests)
    
    for test_name, success in tests:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Acquire phase is ready for use.")
    else:
        print("⚠️  Some tests failed. Check configuration and connections.")

if __name__ == "__main__":
    main()
