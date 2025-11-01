"""
KIMBALL Data Contract Manager

This module manages Data Contracts stored in metadata.transformation0.
Data Contracts define acquisition logic, source references, and execution frequency.
"""

import json
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..core.database import DatabaseManager
from ..core.logger import Logger
from ..core.table_initializer import TableInitializer
from .metadata_source_manager import MetadataSourceManager


class DataContractManager:
    """Manages Data Contracts stored in metadata.transformation0."""
    
    def __init__(self):
        """Initialize the Data Contract manager."""
        self.db_manager = DatabaseManager()
        self.logger = Logger("data_contract_manager")
        self.source_manager = MetadataSourceManager()
        
        # Don't check table existence during __init__ to avoid recursion
        # Tables will be created by initialization endpoint or lazily when needed
    
    def _ensure_table_exists(self):
        """Ensure metadata.transformation0 table exists.
        
        Called lazily when needed (e.g., during create_contract).
        Uses TableInitializer without logger to avoid recursion.
        """
        try:
            from ..core.table_initializer import TableInitializer
            # Use TableInitializer without logger to avoid recursion
            initializer = TableInitializer(use_logger=False)
            
            # First check if the metadata schema exists
            if not initializer.schema_exists("metadata"):
                # Schema doesn't exist yet
                raise ValueError("metadata schema does not exist. Please run initialization endpoint first.")
            
            # Schema exists, try to create/ensure the table exists
            if not initializer.table_exists("metadata.transformation0"):
                initializer.create_table("metadata.transformation0")
        except Exception as e:
            self.logger.error(f"Could not ensure metadata.transformation0 table exists: {e}")
            raise
    
    def create_contract(self, transformation_name: str, source_id: str,
                       acquisition_logic: str, acquisition_type: str,
                       target_table: str, execution_frequency: str = "daily",
                       execution_sequence: int = 0,
                       statement_type: str = "INSERT",
                       dependencies: Optional[List[str]] = None,
                       metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a new Data Contract in metadata.transformation0.
        
        Args:
            transformation_name: Human-readable name for the contract
            source_id: Reference to source in metadata.acquire
            acquisition_logic: SQL, MQL, REST call, or file retrieval instructions
            acquisition_type: Type of acquisition (sql, mql, rest, file)
            target_table: Target table name in bronze schema
            execution_frequency: How often to run (daily, hourly, weekly, etc.)
            execution_sequence: Order of execution within same frequency
            statement_type: Type of statement (INSERT, CREATE, etc.)
            dependencies: Array of dependent transformation IDs
            metadata: Additional JSON metadata
            
        Returns:
            dict: Created contract information
        """
        # Ensure table exists before creating contract
        self._ensure_table_exists()
        
        try:
            # Validate source exists
            source = self.source_manager.get_source(source_id, decrypt=False)
            if not source:
                raise ValueError(f"Source {source_id} not found in metadata.acquire")
            
            # Get next transformation_id
            max_id_query = "SELECT COALESCE(MAX(transformation_id), 0) as max_id FROM metadata.transformation0"
            result = self.db_manager.execute_query_dict(max_id_query)
            max_id = result[0]['max_id'] if result else 0
            transformation_id = int(max_id) + 1
            
            # Prepare dependencies array (ClickHouse will handle the array conversion)
            dependencies_array = dependencies or []
            
            # Prepare metadata JSON
            metadata_json = json.dumps(metadata) if metadata else "{}"
            
            # Insert into metadata.transformation0
            conn = self.db_manager.get_connection("clickhouse")
            conn.insert(
                "metadata.transformation0",
                [[
                    "stage0",  # transformation_stage
                    transformation_id,
                    transformation_name,
                    source_id,
                    acquisition_logic,
                    acquisition_type,
                    target_table,
                    execution_frequency,
                    execution_sequence,
                    statement_type,
                    dependencies_array,  # ClickHouse array
                    datetime.now(),  # created_at
                    datetime.now(),  # updated_at
                    1,  # version
                    metadata_json  # metadata
                ]],
                column_names=[
                    'transformation_stage', 'transformation_id', 'transformation_name',
                    'source_id', 'acquisition_logic', 'acquisition_type', 'target_table',
                    'execution_frequency', 'execution_sequence', 'statement_type',
                    'dependencies', 'created_at', 'updated_at', 'version', 'metadata'
                ]
            )
            
            self.logger.info(f"Created Data Contract: {transformation_id} ({transformation_name})")
            
            return {
                "transformation_id": transformation_id,
                "transformation_name": transformation_name,
                "source_id": source_id,
                "source_name": source.get('source_name', ''),
                "acquisition_type": acquisition_type,
                "target_table": target_table,
                "execution_frequency": execution_frequency
            }
            
        except Exception as e:
            self.logger.error(f"Error creating Data Contract: {e}")
            raise
    
    def get_contract(self, transformation_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a Data Contract by transformation_id.
        
        Args:
            transformation_id: The transformation ID
            
        Returns:
            dict: Contract configuration or None if not found
        """
        try:
            query = f"""
            SELECT transformation_stage, transformation_id, transformation_name,
                   source_id, acquisition_logic, acquisition_type, target_table,
                   execution_frequency, execution_sequence, statement_type,
                   dependencies, created_at, updated_at, version, metadata
            FROM metadata.transformation0
            WHERE transformation_id = {transformation_id}
            ORDER BY version DESC
            LIMIT 1
            """
            
            result = self.db_manager.execute_query_dict(query)
            
            if not result:
                return None
            
            row = result[0]
            
            # Parse metadata JSON
            metadata_raw = row.get('metadata', '{}')
            if isinstance(metadata_raw, str):
                metadata = json.loads(metadata_raw) if metadata_raw else {}
            else:
                metadata = metadata_raw or {}
            
            # Get source information
            source_id = row['source_id']
            source = self.source_manager.get_source(source_id, decrypt=False)
            source_name = source['source_name'] if source else source_id
            
            return {
                "transformation_id": row['transformation_id'],
                "transformation_name": row['transformation_name'],
                "transformation_stage": row['transformation_stage'],
                "source_id": source_id,
                "source_name": source_name,
                "acquisition_logic": row['acquisition_logic'],
                "acquisition_type": row['acquisition_type'],
                "target_table": row['target_table'],
                "execution_frequency": row['execution_frequency'],
                "execution_sequence": row['execution_sequence'],
                "statement_type": row['statement_type'],
                "dependencies": row.get('dependencies', []),
                "metadata": metadata,
                "created_at": row.get('created_at'),
                "updated_at": row.get('updated_at')
            }
            
        except Exception as e:
            self.logger.error(f"Error getting Data Contract {transformation_id}: {e}")
            return None
    
    def list_contracts(self, source_id: Optional[str] = None,
                      execution_frequency: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all Data Contracts.
        
        Args:
            source_id: Optional filter by source_id
            execution_frequency: Optional filter by execution frequency
            
        Returns:
            list: List of contract configurations
        """
        try:
            where_clauses = []
            if source_id:
                escaped_source_id = source_id.replace("'", "''")
                where_clauses.append(f"source_id = '{escaped_source_id}'")
            if execution_frequency:
                escaped_freq = execution_frequency.replace("'", "''")
                where_clauses.append(f"execution_frequency = '{escaped_freq}'")
            
            where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            
            query = f"""
            SELECT transformation_stage, transformation_id, transformation_name,
                   source_id, acquisition_logic, acquisition_type, target_table,
                   execution_frequency, execution_sequence, statement_type,
                   dependencies, created_at, updated_at, version, metadata
            FROM metadata.transformation0
            {where_clause}
            ORDER BY execution_frequency, execution_sequence, transformation_id
            """
            
            results = self.db_manager.execute_query_dict(query)
            
            contracts = []
            for row in results:
                # Parse metadata JSON
                metadata_raw = row.get('metadata', '{}')
                if isinstance(metadata_raw, str):
                    metadata = json.loads(metadata_raw) if metadata_raw else {}
                else:
                    metadata = metadata_raw or {}
                
                # Get source information
                source_id_val = row['source_id']
                source = self.source_manager.get_source(source_id_val, decrypt=False)
                source_name = source['source_name'] if source else source_id_val
                
                contracts.append({
                    "transformation_id": row['transformation_id'],
                    "transformation_name": row['transformation_name'],
                    "transformation_stage": row['transformation_stage'],
                    "source_id": source_id_val,
                    "source_name": source_name,
                    "acquisition_type": row['acquisition_type'],
                    "target_table": row['target_table'],
                    "execution_frequency": row['execution_frequency'],
                    "execution_sequence": row['execution_sequence'],
                    "statement_type": row['statement_type'],
                    "dependencies": row.get('dependencies', []),
                    "created_at": row.get('created_at'),
                    "updated_at": row.get('updated_at')
                })
            
            return contracts
            
        except Exception as e:
            self.logger.error(f"Error listing Data Contracts: {e}")
            return []
    
    def update_contract(self, transformation_id: int,
                       transformation_name: Optional[str] = None,
                       source_id: Optional[str] = None,
                       acquisition_logic: Optional[str] = None,
                       acquisition_type: Optional[str] = None,
                       target_table: Optional[str] = None,
                       execution_frequency: Optional[str] = None,
                       execution_sequence: Optional[int] = None,
                       statement_type: Optional[str] = None,
                       dependencies: Optional[List[str]] = None,
                       metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Update an existing Data Contract.
        
        Args:
            transformation_id: The transformation ID to update
            transformation_name: New name (optional)
            source_id: New source_id (optional)
            acquisition_logic: New logic (optional)
            acquisition_type: New type (optional)
            target_table: New target table (optional)
            execution_frequency: New frequency (optional)
            execution_sequence: New sequence (optional)
            statement_type: New statement type (optional)
            dependencies: New dependencies (optional)
            metadata: New metadata (optional)
            
        Returns:
            bool: True if update successful
        """
        try:
            # Get existing contract
            existing = self.get_contract(transformation_id)
            if not existing:
                self.logger.error(f"Contract {transformation_id} not found")
                return False
            
            # Validate source_id if provided
            if source_id is not None:
                source = self.source_manager.get_source(source_id, decrypt=False)
                if not source:
                    raise ValueError(f"Source {source_id} not found in metadata.acquire")
            
            # Merge updates
            updated_name = transformation_name if transformation_name is not None else existing['transformation_name']
            updated_source_id = source_id if source_id is not None else existing['source_id']
            updated_logic = acquisition_logic if acquisition_logic is not None else existing['acquisition_logic']
            updated_type = acquisition_type if acquisition_type is not None else existing['acquisition_type']
            updated_table = target_table if target_table is not None else existing['target_table']
            updated_frequency = execution_frequency if execution_frequency is not None else existing['execution_frequency']
            updated_sequence = execution_sequence if execution_sequence is not None else existing['execution_sequence']
            updated_stmt_type = statement_type if statement_type is not None else existing['statement_type']
            updated_dependencies = dependencies if dependencies is not None else existing['dependencies']
            
            # Handle metadata
            if metadata is not None:
                updated_metadata = metadata
            else:
                updated_metadata = existing.get('metadata', {})
            
            metadata_json = json.dumps(updated_metadata)
            
            # Insert new version (ReplacingMergeTree will handle versioning)
            conn = self.db_manager.get_connection("clickhouse")
            conn.insert(
                "metadata.transformation0",
                [[
                    "stage0",  # transformation_stage
                    transformation_id,
                    updated_name,
                    updated_source_id,
                    updated_logic,
                    updated_type,
                    updated_table,
                    updated_frequency,
                    updated_sequence,
                    updated_stmt_type,
                    updated_dependencies,  # ClickHouse array
                    datetime.now(),  # created_at
                    datetime.now(),  # updated_at
                    1,  # version
                    metadata_json  # metadata
                ]],
                column_names=[
                    'transformation_stage', 'transformation_id', 'transformation_name',
                    'source_id', 'acquisition_logic', 'acquisition_type', 'target_table',
                    'execution_frequency', 'execution_sequence', 'statement_type',
                    'dependencies', 'created_at', 'updated_at', 'version', 'metadata'
                ]
            )
            
            self.logger.info(f"Updated Data Contract: {transformation_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating Data Contract {transformation_id}: {e}")
            return False
    
    def delete_contract(self, transformation_id: int) -> bool:
        """
        Delete a Data Contract.
        
        Note: This performs a hard delete. In production, consider soft delete.
        
        Args:
            transformation_id: The transformation ID to delete
            
        Returns:
            bool: True if delete successful
        """
        try:
            query = f"ALTER TABLE metadata.transformation0 DELETE WHERE transformation_id = {transformation_id}"
            self.db_manager.execute_command(query)
            
            self.logger.info(f"Deleted Data Contract: {transformation_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting Data Contract {transformation_id}: {e}")
            return False

