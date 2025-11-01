"""
KIMBALL Table Initializer

This module provides functionality to initialize ClickHouse tables from SQL DDL files.
All DDL files are stored in the /sql directory.
"""

import os
import sys
from typing import Optional, List
from pathlib import Path

from .database import DatabaseManager
from .logger import Logger


class TableInitializer:
    """Manages initialization of ClickHouse tables from SQL DDL files."""
    
    # Standard KIMBALL schemas/databases
    STANDARD_SCHEMAS = ["logs", "metadata", "bronze", "silver", "gold"]
    
    def __init__(self, sql_directory: str = "sql", use_logger: bool = True):
        """
        Initialize the table initializer.
        
        Args:
            sql_directory: Path to directory containing SQL DDL files
            use_logger: If False, skip logger initialization to avoid recursion
        """
        self.sql_directory = sql_directory
        # CRITICAL: When use_logger=False, skip logger init in DatabaseManager to prevent recursion
        self.db_manager = DatabaseManager(skip_logger_init=not use_logger)
        if use_logger:
            self.logger = Logger("table_initializer")
        else:
            self.logger = None  # Will use print statements instead
    
    def get_sql_file_path(self, table_name: str) -> Optional[Path]:
        """
        Get the path to a SQL DDL file for a given table name.
        
        Args:
            table_name: Name of the table (e.g., 'metadata.acquire')
            
        Returns:
            Path object or None if file not found
        """
        # Convert table name to filename
        # e.g., 'metadata.acquire' -> 'sql/metadata.acquire.sql'
        filename = table_name.replace('.', '.') + '.sql'
        file_path = Path(self.sql_directory) / filename
        
        if file_path.exists():
            return file_path
        else:
            # Try alternative naming: metadata_acquire.sql
            alt_filename = table_name.replace('.', '_') + '.sql'
            alt_path = Path(self.sql_directory) / alt_filename
            if alt_path.exists():
                return alt_path
        
        return None
    
    def read_ddl_file(self, table_name: str) -> Optional[str]:
        """
        Read DDL SQL from file.
        
        Args:
            table_name: Name of the table
            
        Returns:
            SQL DDL string or None if file not found
        """
        file_path = self.get_sql_file_path(table_name)
        
        if not file_path:
            msg = f"DDL file not found for table: {table_name}"
            if self.logger:
                self.logger.warning(msg)
            else:
                print(f"Warning: {msg}", file=sys.stderr)
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception as e:
            msg = f"Error reading DDL file {file_path}: {e}"
            if self.logger:
                self.logger.error(msg)
            else:
                print(f"Error: {msg}", file=sys.stderr)
            return None
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in ClickHouse.
        
        Args:
            table_name: Name of the table (schema.table)
            
        Returns:
            bool: True if table exists
        """
        try:
            query = f"EXISTS TABLE {table_name}"
            result = self.db_manager.execute_query(query)
            
            if result and len(result) > 0:
                # ClickHouse EXISTS returns 1 if exists, 0 if not
                return result[0][0] == 1
            return False
        except Exception as e:
            msg = f"Error checking table existence {table_name}: {e}"
            if self.logger:
                self.logger.error(msg)
            else:
                print(f"Error: {msg}", file=sys.stderr)
            # Fallback: try to query the table
            try:
                test_query = f"SELECT 1 FROM {table_name} LIMIT 1"
                self.db_manager.execute_query(test_query)
                return True
            except:
                return False
    
    def create_table(self, table_name: str, force: bool = False) -> bool:
        """
        Create a table from its DDL file.
        
        Args:
            table_name: Name of the table to create
            force: If True, drop existing table before creating
            
        Returns:
            bool: True if table created successfully
        """
        # Check if table already exists
        if self.table_exists(table_name):
            if force:
                msg = f"Table {table_name} exists, dropping and recreating..."
                if self.logger:
                    self.logger.info(msg)
                else:
                    print(msg)
                try:
                    drop_query = f"DROP TABLE IF EXISTS {table_name}"
                    self.db_manager.execute_command(drop_query)
                except Exception as e:
                    msg = f"Error dropping table {table_name}: {e}"
                    if self.logger:
                        self.logger.error(msg)
                    else:
                        print(f"Error: {msg}", file=sys.stderr)
                    return False
            else:
                msg = f"Table {table_name} already exists, skipping creation"
                if self.logger:
                    self.logger.info(msg)
                else:
                    print(msg)
                return True
        
        # Read DDL from file
        ddl_sql = self.read_ddl_file(table_name)
        if not ddl_sql:
            msg = f"Cannot create table {table_name}: DDL file not found"
            if self.logger:
                self.logger.error(msg)
            else:
                print(f"Error: {msg}", file=sys.stderr)
            return False
        
        # Execute DDL
        try:
            msg = f"Creating table {table_name} from DDL file..."
            if self.logger:
                self.logger.info(msg)
            else:
                print(msg)
            success = self.db_manager.execute_command(ddl_sql)
            if success:
                msg = f"Table {table_name} created successfully"
                if self.logger:
                    self.logger.info(msg)
                else:
                    print(msg)
                return True
            else:
                msg = f"Failed to create table {table_name}"
                if self.logger:
                    self.logger.error(msg)
                else:
                    print(f"Error: {msg}", file=sys.stderr)
                return False
        except Exception as e:
            msg = f"Error creating table {table_name}: {e}"
            if self.logger:
                self.logger.error(msg)
            else:
                print(f"Error: {msg}", file=sys.stderr)
            return False
    
    def initialize_metadata_tables(self, tables: Optional[List[str]] = None) -> dict:
        """
        Initialize common metadata tables.
        
        Args:
            tables: Optional list of table names to initialize.
                   If None, initializes common metadata tables.
        
        Returns:
            dict: Results of initialization attempts
        """
        if tables is None:
            # Default metadata tables
            tables = [
                "metadata.acquire",
                "metadata.transformation0",
                "metadata.transformation1",
                "metadata.transformation2",
                "metadata.transformation3",
                "metadata.transformation4"
            ]
        
        results = {}
        for table_name in tables:
            success = self.create_table(table_name)
            results[table_name] = {
                "success": success,
                "exists": self.table_exists(table_name)
            }
        
        return results
    
    def schema_exists(self, schema_name: str) -> bool:
        """
        Check if a schema/database exists in ClickHouse.
        
        Args:
            schema_name: Name of the schema/database
            
        Returns:
            bool: True if schema exists
        """
        try:
            query = f"EXISTS DATABASE {schema_name}"
            result = self.db_manager.execute_query(query)
            if result and len(result) > 0:
                return result[0][0] == 1
            return False
        except Exception as e:
            msg = f"Error checking schema existence {schema_name}: {e}"
            if self.logger:
                self.logger.error(msg)
            else:
                print(f"Error: {msg}", file=sys.stderr)
            # Fallback: try to list databases
            try:
                query = "SHOW DATABASES"
                result = self.db_manager.execute_query_dict(query)
                if result:
                    db_names = [row.get('name', row[0] if isinstance(row, (list, tuple)) else row) 
                               for row in result]
                    return schema_name in db_names
            except:
                pass
            return False
    
    def create_schema(self, schema_name: str) -> bool:
        """
        Create a schema/database in ClickHouse.
        
        Args:
            schema_name: Name of the schema/database to create
            
        Returns:
            bool: True if schema created successfully
        """
        try:
            if self.schema_exists(schema_name):
                msg = f"Schema {schema_name} already exists, skipping creation"
                if self.logger:
                    self.logger.info(msg)
                else:
                    print(msg)
                return True
            
            msg = f"Creating schema {schema_name}..."
            if self.logger:
                self.logger.info(msg)
            else:
                print(msg)
            query = f"CREATE DATABASE IF NOT EXISTS {schema_name}"
            success = self.db_manager.execute_command(query)
            if success:
                msg = f"Schema {schema_name} created successfully"
                if self.logger:
                    self.logger.info(msg)
                else:
                    print(msg)
                return True
            else:
                msg = f"Failed to create schema {schema_name}"
                if self.logger:
                    self.logger.error(msg)
                else:
                    print(f"Error: {msg}", file=sys.stderr)
                return False
        except Exception as e:
            msg = f"Error creating schema {schema_name}: {e}"
            if self.logger:
                self.logger.error(msg)
            else:
                print(f"Error: {msg}", file=sys.stderr)
            return False
    
    def create_all_schemas(self) -> dict:
        """
        Create all standard KIMBALL schemas.
        
        Returns:
            dict: Results of schema creation attempts
        """
        results = {}
        for schema_name in self.STANDARD_SCHEMAS:
            success = self.create_schema(schema_name)
            exists = self.schema_exists(schema_name)
            results[schema_name] = {
                "success": success,
                "exists": exists,
                "message": "Created successfully" if success else ("Already exists" if exists else "Failed to create")
            }
        return results

