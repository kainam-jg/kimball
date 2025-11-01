"""
KIMBALL Stage0 Transformation Engine

This module executes acquisition logic defined in metadata.transformation0 (Data Contracts).
It connects to data sources (from metadata.acquire) and performs data extraction and loading
into the bronze schema.
"""

import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..core.database import DatabaseManager
from ..core.logger import Logger
from .metadata_source_manager import MetadataSourceManager
from .data_contract_manager import DataContractManager
from .connectors import DatabaseConnector, APIConnector
from .bucket_processor import S3DataProcessor


class Stage0Engine:
    """Executes stage0 acquisition transformations from Data Contracts."""
    
    def __init__(self):
        """Initialize the Stage0 transformation engine."""
        self.db_manager = DatabaseManager()
        self.logger = Logger("stage0_engine")
        self.source_manager = MetadataSourceManager()
        self.contract_manager = DataContractManager()
    
    async def execute_contract(self, transformation_id: int) -> Dict[str, Any]:
        """
        Execute a Data Contract by transformation_id.
        
        Args:
            transformation_id: The transformation ID to execute
            
        Returns:
            dict: Execution results with status, records processed, etc.
        """
        try:
            self.logger.info(f"Executing Data Contract: {transformation_id}")
            
            # Get the Data Contract
            contract = self.contract_manager.get_contract(transformation_id)
            if not contract:
                raise ValueError(f"Data Contract {transformation_id} not found")
            
            # Get the source configuration
            source_id = contract['source_id']
            source = self.source_manager.get_source(source_id, decrypt=True)
            if not source:
                raise ValueError(f"Source {source_id} not found in metadata.acquire")
            
            # Execute based on acquisition type
            acquisition_type = contract['acquisition_type'].lower()
            acquisition_logic = contract['acquisition_logic']
            target_table = contract['target_table']
            source_type = source['source_type']
            
            if acquisition_type == 'sql':
                result = await self._execute_sql_acquisition(
                    source=source,
                    acquisition_logic=acquisition_logic,
                    target_table=target_table
                )
            elif acquisition_type == 'mql':
                result = await self._execute_mql_acquisition(
                    source=source,
                    acquisition_logic=acquisition_logic,
                    target_table=target_table
                )
            elif acquisition_type == 'rest':
                result = await self._execute_rest_acquisition(
                    source=source,
                    acquisition_logic=acquisition_logic,
                    target_table=target_table
                )
            elif acquisition_type == 'file':
                result = await self._execute_file_acquisition(
                    source=source,
                    acquisition_logic=acquisition_logic,
                    target_table=target_table
                )
            else:
                raise ValueError(f"Unsupported acquisition_type: {acquisition_type}")
            
            self.logger.info(
                f"Data Contract {transformation_id} executed successfully: "
                f"{result.get('records_loaded', 0)} records loaded to bronze.{target_table}"
            )
            
            return {
                "status": "success",
                "transformation_id": transformation_id,
                "transformation_name": contract['transformation_name'],
                "target_table": target_table,
                "acquisition_type": acquisition_type,
                **result
            }
            
        except Exception as e:
            self.logger.error(f"Error executing Data Contract {transformation_id}: {e}")
            return {
                "status": "error",
                "transformation_id": transformation_id,
                "error": str(e),
                "records_loaded": 0
            }
    
    async def _execute_sql_acquisition(self, source: Dict[str, Any], 
                                acquisition_logic: str,
                                target_table: str) -> Dict[str, Any]:
        """
        Execute SQL-based acquisition from database source.
        
        Args:
            source: Source configuration (decrypted)
            acquisition_logic: SQL query to execute
            target_table: Target table name in bronze schema
            
        Returns:
            dict: Execution results
        """
        try:
            source_type = source['source_type']
            
            # Only SQL databases support SQL queries
            if source_type not in ['postgres', 'postgresql', 'mysql', 'clickhouse', 'sqlserver', 'oracle']:
                raise ValueError(f"Source type {source_type} does not support SQL acquisition")
            
            # Create database connector
            connector = DatabaseConnector(source['connection_config'])
            
            if not connector.connect():
                raise ValueError(f"Failed to connect to database source")
            
            # Execute SQL query
            self.logger.info(f"Executing SQL query: {acquisition_logic[:100]}...")
            data = connector.execute_query(acquisition_logic)
            
            if not data:
                self.logger.warning("SQL query returned no data")
                return {
                    "records_extracted": 0,
                    "records_loaded": 0,
                    "message": "Query returned no data"
                }
            
            self.logger.info(f"Extracted {len(data)} records from source")
            
            # Convert to string format and load to bronze
            # Import helper functions from acquire_routes (use relative import)
            from ..api.acquire_routes import _convert_database_results_to_strings, _load_data_to_clickhouse
            
            string_data = _convert_database_results_to_strings(data)
            
            # Get column names
            column_names = list(string_data[0].keys()) if string_data else []
            
            # Load to ClickHouse bronze
            records_loaded = await _load_data_to_clickhouse(string_data, target_table, column_names)
            
            connector.disconnect()
            
            return {
                "records_extracted": len(data),
                "records_loaded": records_loaded,
                "acquisition_method": "sql"
            }
            
        except Exception as e:
            self.logger.error(f"Error in SQL acquisition: {e}")
            raise
    
    async def _execute_mql_acquisition(self, source: Dict[str, Any],
                                 acquisition_logic: str,
                                 target_table: str) -> Dict[str, Any]:
        """
        Execute MQL-based acquisition from NoSQL database (e.g., MongoDB).
        
        Args:
            source: Source configuration (decrypted)
            acquisition_logic: MQL query/aggregation pipeline
            target_table: Target table name in bronze schema
            
        Returns:
            dict: Execution results
        """
        try:
            source_type = source['source_type']
            
            if source_type != 'mongodb':
                raise ValueError(f"MQL acquisition only supported for MongoDB sources, got {source_type}")
            
            # TODO: Implement MongoDB/MQL acquisition
            # This would use pymongo to execute the MQL query
            # For now, raise NotImplementedError
            raise NotImplementedError("MQL acquisition not yet implemented. Requires MongoDB connector.")
            
        except Exception as e:
            self.logger.error(f"Error in MQL acquisition: {e}")
            raise
    
    async def _execute_rest_acquisition(self, source: Dict[str, Any],
                                  acquisition_logic: str,
                                  target_table: str) -> Dict[str, Any]:
        """
        Execute REST API-based acquisition.
        
        Args:
            source: Source configuration (decrypted)
            acquisition_logic: REST API endpoint or JSON config with endpoint, method, params
            target_table: Target table name in bronze schema
            
        Returns:
            dict: Execution results
        """
        try:
            source_type = source['source_type']
            
            if source_type != 'api':
                raise ValueError(f"REST acquisition only supported for API sources, got {source_type}")
            
            # Parse acquisition_logic - can be just endpoint string or JSON config
            import json
            try:
                api_config = json.loads(acquisition_logic)
                endpoint = api_config.get('endpoint', '')
                method = api_config.get('method', 'GET')
                params = api_config.get('params', {})
                headers = api_config.get('headers', {})
            except (json.JSONDecodeError, AttributeError):
                # If not JSON, treat as endpoint string
                endpoint = acquisition_logic
                method = 'GET'
                params = {}
                headers = {}
            
            # Create API connector
            api_config_full = source['connection_config'].copy()
            api_config_full.update({
                'headers': {**api_config_full.get('headers', {}), **headers}
            })
            
            connector = APIConnector(api_config_full)
            
            # Make API call
            if method.upper() == 'GET':
                self.logger.info(f"Making GET request to {endpoint}")
                data = connector.get_data(endpoint, params=params)
            else:
                raise ValueError(f"HTTP method {method} not yet supported. Only GET is supported.")
            
            if not data:
                self.logger.warning("REST API returned no data")
                return {
                    "records_extracted": 0,
                    "records_loaded": 0,
                    "message": "API returned no data"
                }
            
            # Convert to list of dicts if needed
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                data = [{"value": str(data)}]
            
            # Ensure all items are dicts
            dict_data = []
            for item in data:
                if isinstance(item, dict):
                    dict_data.append(item)
                else:
                    dict_data.append({"value": str(item)})
            
            self.logger.info(f"Extracted {len(dict_data)} records from REST API")
            
            # Load to ClickHouse bronze
            from ..api.acquire_routes import _load_data_to_clickhouse
            
            # Get column names
            column_names = list(dict_data[0].keys()) if dict_data else []
            
            # Convert all values to strings for bronze schema
            string_data = []
            for record in dict_data:
                string_record = {}
                for key, value in record.items():
                    string_record[key] = str(value) if value is not None else ""
                string_data.append(string_record)
            
            # Load to ClickHouse bronze
            records_loaded = await _load_data_to_clickhouse(string_data, target_table, column_names)
            
            return {
                "records_extracted": len(dict_data),
                "records_loaded": records_loaded,
                "acquisition_method": "rest",
                "endpoint": endpoint
            }
            
        except Exception as e:
            self.logger.error(f"Error in REST acquisition: {e}")
            raise
    
    async def _execute_file_acquisition(self, source: Dict[str, Any],
                                 acquisition_logic: str,
                                 target_table: str) -> Dict[str, Any]:
        """
        Execute file-based acquisition from storage source (S3, Azure, GCP).
        
        Args:
            source: Source configuration (decrypted)
            acquisition_logic: File path/key or JSON config with file paths and options
            target_table: Target table name in bronze schema
            
        Returns:
            dict: Execution results
        """
        try:
            source_type = source['source_type']
            
            if source_type not in ['s3', 'azure', 'gcp']:
                raise ValueError(f"File acquisition only supported for storage sources, got {source_type}")
            
            # Parse acquisition_logic - can be file path string or JSON config
            import json
            try:
                file_config = json.loads(acquisition_logic)
                file_keys = file_config.get('file_keys', [])
                if not file_keys:
                    file_keys = [file_config.get('file_key', '')]
            except (json.JSONDecodeError, AttributeError):
                # If not JSON, treat as single file key/path
                file_keys = [acquisition_logic]
            
            if not file_keys or not file_keys[0]:
                raise ValueError("No file keys specified in acquisition_logic")
            
            # Handle S3 specifically (most common)
            if source_type == 's3':
                return self._execute_s3_file_acquisition(
                    source=source,
                    file_keys=file_keys,
                    target_table=target_table
                )
            else:
                raise NotImplementedError(f"File acquisition for {source_type} not yet implemented. Only S3 is supported.")
            
        except Exception as e:
            self.logger.error(f"Error in file acquisition: {e}")
            raise
    
    async def _execute_s3_file_acquisition(self, source: Dict[str, Any],
                                     file_keys: List[str],
                                     target_table: str) -> Dict[str, Any]:
        """
        Execute S3 file acquisition.
        
        Args:
            source: Source configuration (decrypted)
            file_keys: List of S3 object keys to extract
            target_table: Target table name in bronze schema
            
        Returns:
            dict: Execution results
        """
        try:
            # Import async functions from acquire_routes
            from ..api.acquire_routes import _extract_multiple_from_s3, StorageExtractionRequest
            
            # Create request object
            request = StorageExtractionRequest(
                object_keys=file_keys,
                target_tables=[target_table] * len(file_keys) if len(file_keys) == 1 else None
            )
            
            # Execute S3 extraction (this uses existing logic)
            results = await _extract_multiple_from_s3(source['connection_config'], request)
            
            # Aggregate results
            total_extracted = sum(r.get('records_extracted', 0) for r in results)
            total_loaded = sum(r.get('records_loaded', 0) for r in results)
            
            # Check for errors
            errors = [r for r in results if r.get('status') == 'error']
            if errors:
                error_messages = [r.get('error', 'Unknown error') for r in errors]
                self.logger.warning(f"Some files failed to extract: {error_messages}")
            
            return {
                "records_extracted": total_extracted,
                "records_loaded": total_loaded,
                "acquisition_method": "file",
                "files_processed": len(file_keys),
                "files_successful": len([r for r in results if r.get('status') == 'success']),
                "files_failed": len(errors)
            }
            
        except Exception as e:
            self.logger.error(f"Error in S3 file acquisition: {e}")
            raise

