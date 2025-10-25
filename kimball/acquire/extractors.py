"""
KIMBALL Data Extractors

This module provides data extraction functionality for various sources:
- Database extraction with SQL queries
- API extraction with pagination and filtering
- Storage extraction with file processing
"""

from typing import Dict, List, Any, Optional, Iterator
import pandas as pd
import json
import requests
from datetime import datetime
import uuid

from ..core.logger import Logger

class DataExtractor:
    """
    Data extractor for various data sources.
    
    Provides unified extraction interface for databases, APIs, and storage.
    """
    
    def __init__(self):
        """Initialize the data extractor."""
        self.logger = Logger("data_extractor")
        self.extractions = {}
    
    def extract_data(self, source_id: str, config: Dict[str, Any], batch_size: int = 1000) -> Dict[str, Any]:
        """
        Extract data from a source.
        
        Args:
            source_id (str): ID of the source to extract from
            config (Dict[str, Any]): Extraction configuration
            batch_size (int): Batch size for extraction
            
        Returns:
            Dict[str, Any]: Extraction result
        """
        try:
            self.logger.info(f"Starting data extraction from source: {source_id}")
            
            # Generate extraction ID
            extraction_id = f"ext_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
            
            # Determine extraction method based on source type
            source_type = config.get("source_type", "database")
            
            if source_type == "database":
                result = self._extract_from_database(source_id, config, batch_size)
            elif source_type == "api":
                result = self._extract_from_api(source_id, config, batch_size)
            elif source_type == "storage":
                result = self._extract_from_storage(source_id, config, batch_size)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Store extraction result
            self.extractions[extraction_id] = {
                "source_id": source_id,
                "config": config,
                "result": result,
                "timestamp": datetime.now().isoformat(),
                "status": "completed"
            }
            
            self.logger.info(f"Data extraction completed: {extraction_id}")
            
            return {
                "extraction_id": extraction_id,
                "source_id": source_id,
                "record_count": result.get("record_count", 0),
                "status": "completed"
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting data: {str(e)}")
            raise
    
    def _extract_from_database(self, source_id: str, config: Dict[str, Any], batch_size: int) -> Dict[str, Any]:
        """Extract data from database source."""
        try:
            # Get database connection
            connection = self._get_database_connection(source_id)
            
            # Build extraction query
            query = config.get("query", "SELECT * FROM table_name")
            table_name = config.get("table_name")
            
            if table_name and not query.startswith("SELECT"):
                query = f"SELECT * FROM {table_name}"
            
            # Execute query with batching
            if batch_size > 0:
                # Implement batching for large datasets
                offset = 0
                all_data = []
                
                while True:
                    batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                    batch_data = connection.execute_query(batch_query)
                    
                    if not batch_data:
                        break
                    
                    all_data.extend(batch_data)
                    offset += batch_size
                    
                    if len(batch_data) < batch_size:
                        break
            else:
                all_data = connection.execute_query(query)
            
            return {
                "data": all_data,
                "record_count": len(all_data),
                "columns": list(all_data[0].keys()) if all_data else [],
                "extraction_type": "database"
            }
            
        except Exception as e:
            self.logger.error(f"Database extraction error: {str(e)}")
            raise
    
    def _extract_from_api(self, source_id: str, config: Dict[str, Any], batch_size: int) -> Dict[str, Any]:
        """Extract data from API source."""
        try:
            base_url = config.get("base_url")
            endpoint = config.get("endpoint", "/data")
            headers = config.get("headers", {})
            params = config.get("params", {})
            
            # Handle pagination
            page = 1
            all_data = []
            
            while True:
                # Add pagination parameters
                page_params = {**params, "page": page, "limit": batch_size}
                
                # Make API request
                response = requests.get(
                    f"{base_url}{endpoint}",
                    headers=headers,
                    params=page_params,
                    timeout=30
                )
                
                if response.status_code != 200:
                    break
                
                data = response.json()
                
                # Handle different API response formats
                if isinstance(data, list):
                    page_data = data
                elif isinstance(data, dict):
                    page_data = data.get("data", data.get("results", []))
                else:
                    break
                
                if not page_data:
                    break
                
                all_data.extend(page_data)
                
                # Check if there are more pages
                if len(page_data) < batch_size:
                    break
                
                page += 1
            
            return {
                "data": all_data,
                "record_count": len(all_data),
                "columns": list(all_data[0].keys()) if all_data else [],
                "extraction_type": "api"
            }
            
        except Exception as e:
            self.logger.error(f"API extraction error: {str(e)}")
            raise
    
    def _extract_from_storage(self, source_id: str, config: Dict[str, Any], batch_size: int) -> Dict[str, Any]:
        """Extract data from storage source."""
        try:
            storage_type = config.get("storage_type", "s3")
            bucket = config.get("bucket")
            file_pattern = config.get("file_pattern", "*.csv")
            
            # Get storage client
            client = self._get_storage_client(source_id)
            
            # List files matching pattern
            files = self._list_storage_files(client, bucket, file_pattern, storage_type)
            
            all_data = []
            
            # Process files
            for file_path in files:
                file_data = self._extract_from_file(client, bucket, file_path, storage_type)
                all_data.extend(file_data)
            
            return {
                "data": all_data,
                "record_count": len(all_data),
                "columns": list(all_data[0].keys()) if all_data else [],
                "extraction_type": "storage",
                "files_processed": len(files)
            }
            
        except Exception as e:
            self.logger.error(f"Storage extraction error: {str(e)}")
            raise
    
    def _get_database_connection(self, source_id: str):
        """Get database connection by source ID."""
        # In production, this would retrieve from connection pool
        # For now, return a mock connection
        return None
    
    def _get_storage_client(self, source_id: str):
        """Get storage client by source ID."""
        # In production, this would retrieve from connection pool
        # For now, return a mock client
        return None
    
    def _list_storage_files(self, client, bucket: str, pattern: str, storage_type: str) -> List[str]:
        """List files in storage matching pattern."""
        # Mock implementation
        return []
    
    def _extract_from_file(self, client, bucket: str, file_path: str, storage_type: str) -> List[Dict[str, Any]]:
        """Extract data from a single file."""
        # Mock implementation
        return []
    
    def get_extraction_status(self, extraction_id: str) -> Dict[str, Any]:
        """Get status of an extraction."""
        if extraction_id in self.extractions:
            return self.extractions[extraction_id]
        else:
            return {"error": "Extraction not found"}
    
    def list_extractions(self) -> List[Dict[str, Any]]:
        """List all extractions."""
        return list(self.extractions.values())
