"""
KIMBALL Data Loaders

This module provides data loading functionality for bronze layer:
- ClickHouse data loading with optimization
- Batch processing and error handling
- Data validation and quality checks
- Loading performance monitoring
"""

from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime
import uuid
import json

from ..core.database import DatabaseManager
from ..core.logger import Logger

class BronzeLoader:
    """
    Bronze layer data loader for ClickHouse.
    
    Provides optimized loading of raw data into bronze layer
    with minimal transformation and maximum performance.
    """
    
    def __init__(self):
        """Initialize the bronze loader."""
        self.logger = Logger("bronze_loader")
        self.db_manager = DatabaseManager()
        self.loads = {}
    
    def load_data(self, extraction_id: str, target_table: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load extracted data into bronze layer.
        
        Args:
            extraction_id (str): ID of the extraction to load
            target_table (str): Target table name in bronze layer
            config (Dict[str, Any]): Loading configuration
            
        Returns:
            Dict[str, Any]: Loading result
        """
        try:
            self.logger.info(f"Starting data load to bronze layer: {target_table}")
            
            # Generate load ID
            load_id = f"load_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
            
            # Get extraction data (in production, this would retrieve from extraction store)
            extraction_data = self._get_extraction_data(extraction_id)
            
            if not extraction_data:
                raise ValueError(f"Extraction data not found: {extraction_id}")
            
            # Prepare data for loading
            prepared_data = self._prepare_data_for_loading(extraction_data, config)
            
            # Create table if it doesn't exist
            self._ensure_table_exists(target_table, prepared_data["schema"])
            
            # Load data in batches
            load_result = self._load_data_in_batches(
                target_table, 
                prepared_data["data"], 
                config
            )
            
            # Store load result
            self.loads[load_id] = {
                "extraction_id": extraction_id,
                "target_table": target_table,
                "config": config,
                "result": load_result,
                "timestamp": datetime.now().isoformat(),
                "status": "completed"
            }
            
            self.logger.info(f"Data load completed: {load_id}")
            
            return {
                "load_id": load_id,
                "target_table": target_table,
                "record_count": load_result["record_count"],
                "status": "completed"
            }
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
    
    def _get_extraction_data(self, extraction_id: str) -> Optional[Dict[str, Any]]:
        """Get extraction data by ID."""
        # In production, this would retrieve from extraction store
        # For now, return mock data
        return {
            "data": [],
            "record_count": 0,
            "columns": [],
            "extraction_type": "mock"
        }
    
    def _prepare_data_for_loading(self, extraction_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for loading into ClickHouse."""
        try:
            data = extraction_data.get("data", [])
            
            if not data:
                return {"data": [], "schema": {}}
            
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(data)
            
            # Apply any required transformations
            if "transformations" in config:
                df = self._apply_loading_transformations(df, config["transformations"])
            
            # Infer ClickHouse schema
            schema = self._infer_clickhouse_schema(df)
            
            # Convert DataFrame back to list of dictionaries
            prepared_data = df.to_dict('records')
            
            return {
                "data": prepared_data,
                "schema": schema,
                "record_count": len(prepared_data)
            }
            
        except Exception as e:
            self.logger.error(f"Error preparing data: {str(e)}")
            raise
    
    def _apply_loading_transformations(self, df: pd.DataFrame, transformations: Dict[str, Any]) -> pd.DataFrame:
        """Apply transformations specific to loading."""
        try:
            # Handle null values for ClickHouse compatibility
            if transformations.get("handle_nulls", True):
                # Replace NaN with None for ClickHouse
                df = df.where(pd.notnull(df), None)
            
            # Convert data types for ClickHouse
            if transformations.get("convert_types", True):
                df = self._convert_types_for_clickhouse(df)
            
            # Add metadata columns
            if transformations.get("add_metadata", True):
                df["_loaded_at"] = datetime.now()
                df["_source_id"] = transformations.get("source_id", "unknown")
                df["_batch_id"] = transformations.get("batch_id", str(uuid.uuid4()))
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error applying loading transformations: {str(e)}")
            return df
    
    def _convert_types_for_clickhouse(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert pandas types to ClickHouse-compatible types."""
        try:
            for column in df.columns:
                if df[column].dtype == 'object':
                    # Try to convert to numeric if possible
                    try:
                        df[column] = pd.to_numeric(df[column], errors='ignore')
                    except:
                        pass
                
                # Convert datetime columns
                if df[column].dtype == 'datetime64[ns]':
                    df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error converting types: {str(e)}")
            return df
    
    def _infer_clickhouse_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Infer ClickHouse schema from DataFrame."""
        try:
            schema = {}
            
            for column, dtype in df.dtypes.items():
                if dtype == 'int64':
                    schema[column] = 'Int64'
                elif dtype == 'float64':
                    schema[column] = 'Float64'
                elif dtype == 'bool':
                    schema[column] = 'UInt8'
                elif dtype == 'datetime64[ns]':
                    schema[column] = 'DateTime'
                else:
                    schema[column] = 'String'
            
            return schema
            
        except Exception as e:
            self.logger.error(f"Error inferring schema: {str(e)}")
            return {}
    
    def _ensure_table_exists(self, table_name: str, schema: Dict[str, str]):
        """Ensure target table exists with correct schema."""
        try:
            # Check if table exists
            tables = self.db_manager.get_tables()
            
            if table_name not in tables:
                # Create table
                self.logger.info(f"Creating table: {table_name}")
                success = self.db_manager.create_table(table_name, schema)
                
                if not success:
                    raise Exception(f"Failed to create table: {table_name}")
            else:
                # Verify schema matches
                existing_schema = self.db_manager.get_table_schema(table_name)
                if existing_schema:
                    self._validate_schema_match(schema, existing_schema)
            
        except Exception as e:
            self.logger.error(f"Error ensuring table exists: {str(e)}")
            raise
    
    def _validate_schema_match(self, expected_schema: Dict[str, str], existing_schema: List[Dict[str, str]]):
        """Validate that schemas match."""
        try:
            existing_columns = {col["name"]: col["type"] for col in existing_schema}
            
            for column, expected_type in expected_schema.items():
                if column in existing_columns:
                    existing_type = existing_columns[column]
                    if not self._types_compatible(expected_type, existing_type):
                        self.logger.warning(f"Type mismatch for column {column}: expected {expected_type}, found {existing_type}")
                else:
                    self.logger.warning(f"Missing column in existing table: {column}")
            
        except Exception as e:
            self.logger.error(f"Error validating schema: {str(e)}")
    
    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two ClickHouse types are compatible."""
        # Simple compatibility check
        compatible_groups = [
            ['Int8', 'Int16', 'Int32', 'Int64'],
            ['Float32', 'Float64'],
            ['String', 'FixedString']
        ]
        
        for group in compatible_groups:
            if type1 in group and type2 in group:
                return True
        
        return type1 == type2
    
    def _load_data_in_batches(self, table_name: str, data: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data in batches for better performance."""
        try:
            batch_size = config.get("batch_size", 1000)
            total_records = len(data)
            loaded_records = 0
            
            # Process data in batches
            for i in range(0, total_records, batch_size):
                batch = data[i:i + batch_size]
                
                # Convert batch to DataFrame
                batch_df = pd.DataFrame(batch)
                
                # Load batch to ClickHouse
                success = self._load_batch_to_clickhouse(table_name, batch_df)
                
                if success:
                    loaded_records += len(batch)
                    self.logger.info(f"Loaded batch {i//batch_size + 1}: {len(batch)} records")
                else:
                    self.logger.error(f"Failed to load batch {i//batch_size + 1}")
            
            return {
                "record_count": loaded_records,
                "total_records": total_records,
                "batches_processed": (total_records + batch_size - 1) // batch_size
            }
            
        except Exception as e:
            self.logger.error(f"Error loading data in batches: {str(e)}")
            raise
    
    def _load_batch_to_clickhouse(self, table_name: str, batch_df: pd.DataFrame) -> bool:
        """Load a single batch to ClickHouse."""
        try:
            # In production, this would use ClickHouse client to insert data
            # For now, simulate successful loading
            self.logger.debug(f"Loading {len(batch_df)} records to {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading batch to ClickHouse: {str(e)}")
            return False
    
    def get_load_status(self, load_id: str) -> Dict[str, Any]:
        """Get status of a load operation."""
        if load_id in self.loads:
            return self.loads[load_id]
        else:
            return {"error": "Load not found"}
    
    def list_loads(self) -> List[Dict[str, Any]]:
        """List all load operations."""
        return list(self.loads.values())
