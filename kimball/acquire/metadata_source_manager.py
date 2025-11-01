"""
KIMBALL Metadata-Based Data Source Manager

This module manages data sources using metadata.acquire table instead of config.json.
It handles CRUD operations with encryption for sensitive connection parameters.
"""

import json
import uuid
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..core.database import DatabaseManager
from ..core.encryption import EncryptionManager
from ..core.logger import Logger
from ..core.table_initializer import TableInitializer


class MetadataSourceManager:
    """Manages data sources stored in metadata.acquire table."""
    
    def __init__(self):
        """Initialize the metadata-based source manager."""
        self.db_manager = DatabaseManager()
        self.encryption = EncryptionManager()
        self.logger = Logger("metadata_source_manager")
        
        # Don't check table existence during __init__ to avoid recursion
        # Tables will be created by initialization endpoint or lazily when needed
    
    def _ensure_table_exists(self):
        """Ensure metadata.acquire table exists.
        
        Called lazily when needed (e.g., during create_source).
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
            if not initializer.table_exists("metadata.acquire"):
                initializer.create_table("metadata.acquire")
        except Exception as e:
            self.logger.error(f"Could not ensure metadata.acquire table exists: {e}")
            raise
    
    def create_source(self, source_name: str, source_type: str, connection_config: Dict[str, Any],
                     enabled: bool = True, description: str = "") -> Dict[str, Any]:
        """
        Create a new data source in metadata.acquire.
        
        Args:
            source_name: Human-readable name for the source
            source_type: Type of source (postgres, s3, api, etc.)
            connection_config: Connection configuration dictionary
            enabled: Whether the source is enabled
            description: Optional description
            
        Returns:
            dict: Created source information
        """
        # Ensure table exists before creating source
        self._ensure_table_exists()
        
        try:
            # Generate unique source_id
            source_id = str(uuid.uuid4())
            
            # Encrypt sensitive fields in connection_config
            encrypted_config = self.encryption.encrypt_connection_config(connection_config)
            
            # Store connection_config as JSON string
            connection_config_json = json.dumps(encrypted_config)
            
            # Escape single quotes in strings for SQL
            def escape_sql_string(s):
                return s.replace("'", "''")
            
            # Use ClickHouse connection directly for native insert
            conn = self.db_manager.get_connection("clickhouse")
            conn.insert(
                "metadata.acquire",
                [[source_id, source_name, source_type, connection_config_json,
                 1 if enabled else 0, description, datetime.now(), datetime.now(), 1]],
                column_names=['source_id', 'source_name', 'source_type', 'connection_config',
                             'enabled', 'description', 'created_at', 'updated_at', 'version']
            )
            
            self.logger.info(f"Created data source: {source_id} ({source_name})")
            
            return {
                "source_id": source_id,
                "source_name": source_name,
                "source_type": source_type,
                "enabled": enabled,
                "description": description
            }
            
        except Exception as e:
            self.logger.error(f"Error creating data source: {e}")
            raise
    
    def get_source(self, source_id: str, decrypt: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get a data source by ID.
        
        Args:
            source_id: The source ID
            decrypt: Whether to decrypt sensitive connection parameters
            
        Returns:
            dict: Source configuration or None if not found
        """
        try:
            # Escape single quotes in source_id
            escaped_source_id = source_id.replace("'", "''")
            
            query = f"""
            SELECT source_id, source_name, source_type, connection_config,
                   enabled, description, created_at, updated_at, version
            FROM metadata.acquire
            WHERE source_id = '{escaped_source_id}'
            ORDER BY version DESC
            LIMIT 1
            """
            
            result = self.db_manager.execute_query_dict(query)
            
            if not result:
                return None
            
            row = result[0]
            
            # Parse connection_config JSON (handle both string and dict)
            connection_config_raw = row.get('connection_config', '{}')
            if isinstance(connection_config_raw, str):
                connection_config = json.loads(connection_config_raw)
            else:
                connection_config = connection_config_raw
            
            # Decrypt sensitive fields if requested
            if decrypt:
                connection_config = self.encryption.decrypt_connection_config(connection_config)
            
            return {
                "source_id": row['source_id'],
                "source_name": row['source_name'],
                "source_type": row['source_type'],
                "connection_config": connection_config,
                "enabled": bool(row['enabled']),
                "description": row.get('description', ''),
                "created_at": row.get('created_at'),
                "updated_at": row.get('updated_at')
            }
            
        except Exception as e:
            self.logger.error(f"Error getting data source {source_id}: {e}")
            return None
    
    def get_source_by_name(self, source_name: str, decrypt: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get a data source by name.
        
        Args:
            source_name: The source name
            decrypt: Whether to decrypt sensitive connection parameters
            
        Returns:
            dict: Source configuration or None if not found
        """
        try:
            # Escape single quotes in source_name
            escaped_source_name = source_name.replace("'", "''")
            
            query = f"""
            SELECT source_id, source_name, source_type, connection_config,
                   enabled, description, created_at, updated_at, version
            FROM metadata.acquire
            WHERE source_name = '{escaped_source_name}'
            ORDER BY version DESC
            LIMIT 1
            """
            
            result = self.db_manager.execute_query_dict(query)
            
            if not result:
                return None
            
            row = result[0]
            
            # Parse connection_config JSON (handle both string and dict)
            connection_config_raw = row.get('connection_config', '{}')
            if isinstance(connection_config_raw, str):
                connection_config = json.loads(connection_config_raw)
            else:
                connection_config = connection_config_raw
            
            # Decrypt sensitive fields if requested
            if decrypt:
                connection_config = self.encryption.decrypt_connection_config(connection_config)
            
            return {
                "source_id": row['source_id'],
                "source_name": row['source_name'],
                "source_type": row['source_type'],
                "connection_config": connection_config,
                "enabled": bool(row['enabled']),
                "description": row.get('description', ''),
                "created_at": row.get('created_at'),
                "updated_at": row.get('updated_at')
            }
            
        except Exception as e:
            self.logger.error(f"Error getting data source by name {source_name}: {e}")
            return None
    
    def list_sources(self, enabled_only: bool = False, decrypt: bool = False) -> List[Dict[str, Any]]:
        """
        List all data sources.
        
        Args:
            enabled_only: If True, only return enabled sources
            decrypt: Whether to decrypt sensitive connection parameters
            
        Returns:
            list: List of source configurations
        """
        try:
            where_clause = "WHERE enabled = 1" if enabled_only else ""
            
            query = f"""
            SELECT source_id, source_name, source_type, connection_config,
                   enabled, description, created_at, updated_at, version
            FROM metadata.acquire
            {where_clause}
            ORDER BY source_name
            """
            
            results = self.db_manager.execute_query_dict(query)
            
            sources = []
            for row in results:
                # Parse connection_config JSON (handle both string and dict)
                connection_config_raw = row.get('connection_config', '{}')
                if isinstance(connection_config_raw, str):
                    connection_config = json.loads(connection_config_raw)
                else:
                    connection_config = connection_config_raw
                
                # Decrypt sensitive fields if requested
                if decrypt:
                    connection_config = self.encryption.decrypt_connection_config(connection_config)
                else:
                    # Remove sensitive fields if not decrypting
                    sensitive_fields = ['password', 'secret_key', 'access_key', 'api_token', 'secret']
                    for field in sensitive_fields:
                        if field in connection_config:
                            connection_config[field] = "***ENCRYPTED***"
                
                sources.append({
                    "source_id": row['source_id'],
                    "source_name": row['source_name'],
                    "source_type": row['source_type'],
                    "connection_config": connection_config,
                    "enabled": bool(row['enabled']),
                    "description": row.get('description', ''),
                    "created_at": row.get('created_at'),
                    "updated_at": row.get('updated_at')
                })
            
            return sources
            
        except Exception as e:
            self.logger.error(f"Error listing data sources: {e}")
            return []
    
    def update_source(self, source_id: str, source_name: Optional[str] = None,
                     source_type: Optional[str] = None,
                     connection_config: Optional[Dict[str, Any]] = None,
                     enabled: Optional[bool] = None,
                     description: Optional[str] = None) -> bool:
        """
        Update an existing data source.
        
        Args:
            source_id: The source ID to update
            source_name: New source name (optional)
            source_type: New source type (optional)
            connection_config: New connection config (optional)
            enabled: New enabled status (optional)
            description: New description (optional)
            
        Returns:
            bool: True if update successful
        """
        try:
            # Get existing source
            existing = self.get_source(source_id, decrypt=False)
            if not existing:
                self.logger.error(f"Source {source_id} not found")
                return False
            
            # Merge updates
            updated_name = source_name if source_name is not None else existing['source_name']
            updated_type = source_type if source_type is not None else existing['source_type']
            updated_enabled = enabled if enabled is not None else existing['enabled']
            updated_description = description if description is not None else existing['description']
            
            # Handle connection_config
            if connection_config is not None:
                # Encrypt sensitive fields
                updated_config = self.encryption.encrypt_connection_config(connection_config)
            else:
                # Use existing encrypted config
                existing_encrypted = json.loads(existing['connection_config']) if isinstance(existing['connection_config'], str) else existing['connection_config']
                updated_config = existing_encrypted
            
            connection_config_json = json.dumps(updated_config)
            
            # Insert new version (ReplacingMergeTree will handle versioning)
            conn = self.db_manager.get_connection("clickhouse")
            conn.insert(
                "metadata.acquire",
                [[source_id, updated_name, updated_type, connection_config_json,
                 1 if updated_enabled else 0, updated_description, datetime.now(), datetime.now(), 1]],
                column_names=['source_id', 'source_name', 'source_type', 'connection_config',
                             'enabled', 'description', 'created_at', 'updated_at', 'version']
            )
            
            self.logger.info(f"Updated data source: {source_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error updating data source {source_id}: {e}")
            return False
    
    def delete_source(self, source_id: str) -> bool:
        """
        Delete a data source (soft delete by setting enabled=False).
        
        Note: We use soft delete to maintain data lineage.
        For hard delete, we can add a delete flag or use TRUNCATE if needed.
        
        Args:
            source_id: The source ID to delete
            
        Returns:
            bool: True if delete successful
        """
        try:
            return self.update_source(source_id, enabled=False)
        except Exception as e:
            self.logger.error(f"Error deleting data source {source_id}: {e}")
            return False
    
    def test_connection(self, source_id: str) -> bool:
        """
        Test connection to a data source.
        
        Args:
            source_id: The source ID to test
            
        Returns:
            bool: True if connection test successful
        """
        try:
            source = self.get_source(source_id, decrypt=True)
            if not source:
                return False
            
            source_type = source['source_type']
            config = source['connection_config']
            
            # Import connectors dynamically
            if source_type in ['postgres', 'postgresql']:
                from .connectors import DatabaseConnector
                connector = DatabaseConnector(config)
                return connector.test_connection()
            elif source_type == 's3':
                from .bucket_processor import S3DataProcessor
                processor = S3DataProcessor(config)
                return processor.connect() if hasattr(processor, 'connect') else False
            elif source_type == 'api':
                from .connectors import APIConnector
                connector = APIConnector(config)
                return connector.test_connection()
            else:
                self.logger.error(f"Unsupported source type for testing: {source_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error testing connection to {source_id}: {e}")
            return False

