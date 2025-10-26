"""
KIMBALL Data Loaders

This module provides simplified data loading functionality.
The new bucket processor handles most loading operations.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid

from ..core.database import DatabaseManager
from ..core.logger import Logger

class BronzeLoader:
    """
    Simplified bronze layer data loader for ClickHouse.
    
    This is kept for backward compatibility with SQL query and table data extractions.
    Storage data now uses the new bucket processor architecture.
    """
    
    def __init__(self):
        """Initialize the bronze loader."""
        self.logger = Logger("bronze_loader")
        self.db_manager = DatabaseManager()
        self.loads = {}
    
    def load_data(self, data: List[Dict[str, Any]], target_table: str, source_id: str, extraction_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Load extracted data into bronze layer.
        
        Args:
            data (List[Dict[str, Any]]): Data to load
            target_table (str): Target table name in bronze layer
            source_id (str): Source ID for metadata
            extraction_id (Optional[str]): Extraction ID for tracking
            
        Returns:
            Dict[str, Any]: Loading result
        """
        try:
            self.logger.info(f"Starting data load to bronze layer: {target_table}")
            
            # Generate load ID
            load_id = f"load_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
            
            if not data:
                raise ValueError("No data provided for loading")
            
            # Prepare data for loading
            prepared_data = self._prepare_data_for_loading(data, source_id)
            
            # Create table if it doesn't exist
            self._ensure_table_exists(target_table, prepared_data["schema"])
            
            # Load data in batches
            load_result = self._load_data_in_batches(
                target_table, 
                prepared_data["data"], 
                {"batch_size": 1000}
            )
            
            # Store load result
            self.loads[load_id] = {
                "extraction_id": extraction_id,
                "source_id": source_id,
                "target_table": target_table,
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
    
    def _prepare_data_for_loading(self, data: List[Dict[str, Any]], source_id: str) -> Dict[str, Any]:
        """Prepare data for loading into ClickHouse."""
        try:
            if not data:
                return {"data": [], "schema": {}}
            
            # Convert to DataFrame for easier manipulation
            import pandas as pd
            df = pd.DataFrame(data)
            
            # Apply loading transformations
            df = self._apply_loading_transformations(df, {"source_id": source_id})
            
            # Infer ClickHouse schema - all columns as String
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
    
    def _apply_loading_transformations(self, df, transformations: Dict[str, Any]):
        """Apply transformations specific to loading."""
        try:
            # Handle null values for ClickHouse compatibility
            if transformations.get("handle_nulls", True):
                # Replace NaN with None for ClickHouse
                df = df.where(pd.notnull(df), None)
            
            # Add metadata columns
            if transformations.get("add_metadata", True):
                df["_loaded_at"] = datetime.now()
                df["_source_id"] = transformations.get("source_id", "unknown")
                df["_batch_id"] = transformations.get("batch_id", str(uuid.uuid4()))
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error applying loading transformations: {str(e)}")
            return df
    
    def _infer_clickhouse_schema(self, df) -> Dict[str, str]:
        """Infer ClickHouse schema from DataFrame - all columns as String."""
        try:
            schema = {}
            
            # For now, make all columns String type to avoid ClickHouse errors
            # Later in bronze discovery phase, we'll determine true data types
            for column in df.columns:
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
            
            # Handle case where get_tables returns None
            if tables is None:
                self.logger.warning("Could not retrieve table list, assuming table doesn't exist")
                tables = []
            
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
            
            for col_name, col_type in expected_schema.items():
                if col_name not in existing_columns:
                    self.logger.warning(f"Column {col_name} missing in existing table")
                elif existing_columns[col_name] != col_type:
                    self.logger.warning(f"Column {col_name} type mismatch: expected {col_type}, got {existing_columns[col_name]}")
                    
        except Exception as e:
            self.logger.error(f"Error validating schema match: {str(e)}")
    
    def _load_data_in_batches(self, table_name: str, data: List[Dict[str, Any]], options: Dict[str, Any]) -> Dict[str, Any]:
        """Load data in batches."""
        try:
            batch_size = options.get("batch_size", 1000)
            total_records = len(data)
            
            if total_records == 0:
                return {"record_count": 0, "batches_loaded": 0}
            
            batches_loaded = 0
            
            for i in range(0, total_records, batch_size):
                batch = data[i:i + batch_size]
                self._insert_batch(table_name, batch)
                batches_loaded += 1
                self.logger.info(f"Loaded batch {batches_loaded}: {len(batch)} records")
            
            return {
                "record_count": total_records,
                "batches_loaded": batches_loaded
            }
            
        except Exception as e:
            self.logger.error(f"Error loading data in batches: {str(e)}")
            raise
    
    def _insert_batch(self, table_name: str, batch: List[Dict[str, Any]]):
        """Insert a batch of records to ClickHouse."""
        try:
            if not batch:
                return
            
            # Get column names from first record
            columns = list(batch[0].keys())
            
            # Build INSERT statement
            columns_str = ', '.join([f"`{col}`" for col in columns])
            
            # Prepare values for INSERT
            values_list = []
            for record in batch:
                # Convert all values to strings and escape single quotes
                row_values = []
                for col in columns:
                    value = str(record.get(col, ""))
                    # Escape single quotes for SQL
                    value = value.replace("'", "''")
                    row_values.append(f"'{value}'")
                values_list.append(f"({', '.join(row_values)})")
            
            # Build INSERT statement
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES {', '.join(values_list)}"
            
            # Execute INSERT
            self.db_manager.execute_query(insert_sql)
            
        except Exception as e:
            self.logger.error(f"Error inserting batch to {table_name}: {str(e)}")
            raise