"""
Data Source Manager for KIMBALL Acquire Phase

This module manages multiple data sources and provides a unified interface
for data acquisition from various sources including databases, APIs, and storage.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
from datetime import datetime
import pandas as pd

from .connectors import DatabaseConnector, APIConnector, StorageConnector
from ..core.logger import Logger
from ..core.database import DatabaseManager

class DataSourceManager:
    """Manages multiple data sources for the Acquire phase."""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize the data source manager."""
        self.config_path = config_path
        self.config = self._load_config()
        self.sources = {}
        self.logger = Logger("source_manager")
        self.db_manager = DatabaseManager()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load config: {e}")
            return {}
    
    def get_available_sources(self) -> List[Dict[str, Any]]:
        """Get list of available data sources."""
        sources = []
        for name, config in self.config.get("data_sources", {}).items():
            if config.get("enabled", False):
                sources.append({
                    "name": name,
                    "type": config.get("type"),
                    "description": config.get("description", ""),
                    "config": config
                })
        return sources
    
    def connect_source(self, source_name: str) -> bool:
        """Connect to a specific data source."""
        try:
            if source_name not in self.config.get("data_sources", {}):
                self.logger.error(f"Source '{source_name}' not found in configuration")
                return False
            
            source_config = self.config["data_sources"][source_name]
            source_type = source_config.get("type")
            
            if source_type == "postgres":
                connector = DatabaseConnector(source_config)
            elif source_type == "s3":
                connector = StorageConnector(source_config)
            elif source_type == "api":
                connector = APIConnector(source_config)
            else:
                self.logger.error(f"Unsupported source type: {source_type}")
                return False
            
            if connector.connect():
                self.sources[source_name] = connector
                self.logger.info(f"Connected to source: {source_name}")
                return True
            else:
                self.logger.error(f"Failed to connect to source: {source_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error connecting to source {source_name}: {e}")
            return False
    
    def disconnect_source(self, source_name: str):
        """Disconnect from a specific data source."""
        if source_name in self.sources:
            self.sources[source_name].disconnect()
            del self.sources[source_name]
            self.logger.info(f"Disconnected from source: {source_name}")
    
    def disconnect_all(self):
        """Disconnect from all sources."""
        for source_name in list(self.sources.keys()):
            self.disconnect_source(source_name)
    
    def test_source(self, source_name: str) -> bool:
        """Test connection to a specific source."""
        if source_name not in self.sources:
            self.logger.error(f"Source '{source_name}' not connected")
            return False
        
        return self.sources[source_name].test_connection()
    
    def test_source_connection(self, source_name: str) -> bool:
        """Test connection to a specific source without requiring it to be pre-connected."""
        try:
            if source_name not in self.config.get("data_sources", {}):
                self.logger.error(f"Source '{source_name}' not found in configuration")
                return False
            
            source_config = self.config["data_sources"][source_name]
            source_type = source_config.get("type")
            
            # Create a temporary connector to test the connection
            if source_type == "postgres":
                connector = DatabaseConnector(source_config)
            elif source_type == "s3":
                connector = StorageConnector(source_config)
            elif source_type == "api":
                connector = APIConnector(source_config)
            else:
                self.logger.error(f"Unsupported source type: {source_type}")
                return False
            
            # Test the connection
            if connector.test_connection():
                self.logger.info(f"Connection test successful for source: {source_name}")
                return True
            else:
                self.logger.error(f"Connection test failed for source: {source_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error testing connection to source {source_name}: {e}")
            return False
    
    def get_source_schema(self, source_name: str) -> Dict[str, Any]:
        """Get schema information from a specific source."""
        if source_name not in self.sources:
            self.logger.error(f"Source '{source_name}' not connected")
            return {}
        
        return self.sources[source_name].get_schema()
    
    def extract_data(self, source_name: str, query: str = None, table_name: str = None, 
                    file_path: str = None, **kwargs) -> Optional[pd.DataFrame]:
        """Extract data from a specific source."""
        if source_name not in self.sources:
            self.logger.error(f"Source '{source_name}' not connected")
            return None
        
        try:
            source = self.sources[source_name]
            source_type = self.config["data_sources"][source_name].get("type")
            
            if source_type == "postgres":
                return self._extract_from_database(source, query, table_name)
            elif source_type == "s3":
                return self._extract_from_s3(source, file_path, **kwargs)
            elif source_type == "api":
                return self._extract_from_api(source, query, **kwargs)
            else:
                self.logger.error(f"Unsupported extraction for type: {source_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting data from {source_name}: {e}")
            return None
    
    def _extract_from_database(self, source: DatabaseConnector, query: str = None, 
                              table_name: str = None) -> Optional[pd.DataFrame]:
        """Extract data from database source."""
        try:
            if query:
                return pd.read_sql(query, source.connection)
            elif table_name:
                return pd.read_sql(f"SELECT * FROM {table_name}", source.connection)
            else:
                self.logger.error("Either query or table_name must be provided")
                return None
        except Exception as e:
            self.logger.error(f"Database extraction error: {e}")
            return None
    
    def _extract_from_s3(self, source: StorageConnector, file_path: str, 
                        **kwargs) -> Optional[pd.DataFrame]:
        """Extract data from S3 source."""
        try:
            # Download file from S3
            local_path = f"/tmp/{Path(file_path).name}"
            source.client.download_file(source.bucket, file_path, local_path)
            
            # Read file based on extension
            file_ext = Path(file_path).suffix.lower()
            if file_ext == '.csv':
                return pd.read_csv(local_path, **kwargs)
            elif file_ext in ['.xlsx', '.xls']:
                return pd.read_excel(local_path, **kwargs)
            elif file_ext == '.json':
                return pd.read_json(local_path, **kwargs)
            elif file_ext == '.parquet':
                return pd.read_parquet(local_path, **kwargs)
            else:
                self.logger.error(f"Unsupported file format: {file_ext}")
                return None
                
        except Exception as e:
            self.logger.error(f"S3 extraction error: {e}")
            return None
    
    def _extract_from_api(self, source: APIConnector, endpoint: str, 
                         **kwargs) -> Optional[pd.DataFrame]:
        """Extract data from API source."""
        try:
            import requests
            
            url = f"{source.base_url}/{endpoint.lstrip('/')}"
            response = requests.get(url, headers=source.headers, **kwargs)
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict) and 'data' in data:
                return pd.DataFrame(data['data'])
            else:
                return pd.DataFrame([data])
                
        except Exception as e:
            self.logger.error(f"API extraction error: {e}")
            return None
    
    def load_to_bronze(self, data: pd.DataFrame, source_name: str, 
                      table_name: str = None) -> bool:
        """Load extracted data to ClickHouse bronze layer."""
        try:
            if table_name is None:
                table_name = f"bronze_{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Add metadata columns
            data['_source'] = source_name
            data['_extracted_at'] = datetime.now()
            data['_batch_id'] = f"{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Load to ClickHouse
            return self.db_manager.load_dataframe(data, table_name, schema="bronze")
            
        except Exception as e:
            self.logger.error(f"Error loading to bronze layer: {e}")
            return False
    
    def run_acquisition_job(self, source_name: str, job_config: Dict[str, Any]) -> bool:
        """Run a complete data acquisition job."""
        try:
            self.logger.info(f"Starting acquisition job for source: {source_name}")
            
            # Connect to source
            if not self.connect_source(source_name):
                return False
            
            # Extract data
            data = self.extract_data(
                source_name,
                query=job_config.get("query"),
                table_name=job_config.get("table_name"),
                file_path=job_config.get("file_path"),
                **job_config.get("extract_options", {})
            )
            
            if data is None or data.empty:
                self.logger.error(f"No data extracted from {source_name}")
                return False
            
            # Load to bronze layer
            table_name = job_config.get("target_table", f"bronze_{source_name}")
            if not self.load_to_bronze(data, source_name, table_name):
                return False
            
            self.logger.info(f"Successfully completed acquisition job for {source_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Acquisition job failed for {source_name}: {e}")
            return False
        finally:
            # Disconnect from source
            self.disconnect_source(source_name)
    
    def test_source_connection(self, source_name: str) -> bool:
        """Test connection to a specific source without requiring it to be pre-connected."""
        try:
            if source_name not in self.config.get("data_sources", {}):
                self.logger.error(f"Source '{source_name}' not found in configuration")
                return False
            
            source_config = self.config["data_sources"][source_name]
            source_type = source_config.get("type")
            
            # Create a temporary connector to test the connection
            if source_type == "postgres":
                connector = DatabaseConnector(source_config)
            elif source_type == "s3":
                connector = StorageConnector(source_config)
            elif source_type == "api":
                connector = APIConnector(source_config)
            else:
                self.logger.error(f"Unsupported source type: {source_type}")
                return False
            
            # Test the connection
            if connector.test_connection():
                self.logger.info(f"Connection test successful for source: {source_name}")
                return True
            else:
                self.logger.error(f"Connection test failed for source: {source_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error testing connection to source {source_name}: {e}")
            return False
