"""
KIMBALL Database Manager

This module provides centralized database management for the entire KIMBALL platform.
Supports multiple database types with a unified interface.
"""

import json
from typing import Dict, List, Any, Optional, Union
from abc import ABC, abstractmethod

from .config import Config
from .logger import Logger

# Import existing connection functionality
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from connection import get_clickhouse_connection

class DatabaseManager:
    """
    Centralized database manager for KIMBALL platform.
    
    Provides a unified interface for database operations across all phases.
    """
    
    def __init__(self, config_file: str = "config.json"):
        """Initialize the database manager."""
        self.config = Config(config_file)
        self.logger = Logger("database_manager")
        self.connections = {}
        
    def get_connection(self, connection_type: str = "clickhouse") -> Any:
        """
        Get a database connection.
        
        Args:
            connection_type (str): Type of connection (clickhouse, postgres, etc.)
            
        Returns:
            Any: Database connection object
        """
        try:
            if connection_type == "clickhouse":
                if "clickhouse" not in self.connections:
                    self.connections["clickhouse"] = get_clickhouse_connection()
                return self.connections["clickhouse"]
            else:
                raise ValueError(f"Unsupported connection type: {connection_type}")
                
        except Exception as e:
            self.logger.error(f"Error getting connection: {str(e)}")
            raise
    
    def test_connection(self, connection_type: str = "clickhouse") -> bool:
        """
        Test a database connection.
        
        Args:
            connection_type (str): Type of connection to test
            
        Returns:
            bool: True if connection successful
        """
        try:
            conn = self.get_connection(connection_type)
            if conn.connect():
                conn.disconnect()
                self.logger.info(f"Connection test successful for {connection_type}")
                return True
            else:
                self.logger.error(f"Connection test failed for {connection_type}")
                return False
        except Exception as e:
            self.logger.error(f"Connection test error: {str(e)}")
            return False
    
    def execute_query(self, query: str, connection_type: str = "clickhouse") -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            connection_type (str): Type of connection to use
            
        Returns:
            Optional[List[Dict[str, Any]]]: Query results
        """
        try:
            conn = self.get_connection(connection_type)
            if not conn.connect():
                self.logger.error("Failed to connect to database")
                return None
            
            result = conn.execute_query(query)
            conn.disconnect()
            
            self.logger.info(f"Query executed successfully: {query[:100]}...")
            return result
            
        except Exception as e:
            self.logger.error(f"Query execution error: {str(e)}")
            return None
    
    def get_tables(self, schema: str = "bronze", connection_type: str = "clickhouse") -> Optional[List[str]]:
        """
        Get list of tables in a schema.
        
        Args:
            schema (str): Schema name
            connection_type (str): Type of connection to use
            
        Returns:
            Optional[List[str]]: List of table names
        """
        try:
            conn = self.get_connection(connection_type)
            if not conn.connect():
                return None
            
            tables = conn.get_tables()
            conn.disconnect()
            
            self.logger.info(f"Retrieved {len(tables) if tables else 0} tables from {schema}")
            return tables
            
        except Exception as e:
            self.logger.error(f"Error getting tables: {str(e)}")
            return None
    
    def get_table_schema(self, table_name: str, connection_type: str = "clickhouse") -> Optional[List[Dict[str, str]]]:
        """
        Get schema information for a table.
        
        Args:
            table_name (str): Name of the table
            connection_type (str): Type of connection to use
            
        Returns:
            Optional[List[Dict[str, str]]]: Table schema information
        """
        try:
            conn = self.get_connection(connection_type)
            if not conn.connect():
                return None
            
            schema = conn.get_table_schema(table_name)
            conn.disconnect()
            
            self.logger.info(f"Retrieved schema for table {table_name}")
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting table schema: {str(e)}")
            return None
    
    def get_table_info(self, table_name: str, connection_type: str = "clickhouse") -> Optional[Dict[str, Any]]:
        """
        Get comprehensive information about a table.
        
        Args:
            table_name (str): Name of the table
            connection_type (str): Type of connection to use
            
        Returns:
            Optional[Dict[str, Any]]: Table information
        """
        try:
            conn = self.get_connection(connection_type)
            if not conn.connect():
                return None
            
            table_info = conn.get_table_info(table_name)
            conn.disconnect()
            
            self.logger.info(f"Retrieved info for table {table_name}")
            return table_info
            
        except Exception as e:
            self.logger.error(f"Error getting table info: {str(e)}")
            return None
    
    def create_table(self, table_name: str, schema: Dict[str, str], connection_type: str = "clickhouse") -> bool:
        """
        Create a new table.
        
        Args:
            table_name (str): Name of the table to create
            schema (Dict[str, str]): Table schema definition
            connection_type (str): Type of connection to use
            
        Returns:
            bool: True if table created successfully
        """
        try:
            # Build CREATE TABLE statement
            columns = [f"`{col_name}` {col_type}" for col_name, col_type in schema.items()]
            create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)}) ENGINE = MergeTree() ORDER BY tuple()"
            
            result = self.execute_query(create_sql, connection_type)
            
            if result is not None:
                self.logger.info(f"Table {table_name} created successfully")
                return True
            else:
                self.logger.error(f"Failed to create table {table_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error creating table: {str(e)}")
            return False
    
    def drop_table(self, table_name: str, connection_type: str = "clickhouse") -> bool:
        """
        Drop a table.
        
        Args:
            table_name (str): Name of the table to drop
            connection_type (str): Type of connection to use
            
        Returns:
            bool: True if table dropped successfully
        """
        try:
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
            result = self.execute_query(drop_sql, connection_type)
            
            if result is not None:
                self.logger.info(f"Table {table_name} dropped successfully")
                return True
            else:
                self.logger.error(f"Failed to drop table {table_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error dropping table: {str(e)}")
            return False
    
    def close_all_connections(self):
        """Close all database connections."""
        try:
            for conn_type, conn in self.connections.items():
                if hasattr(conn, 'disconnect'):
                    conn.disconnect()
                    self.logger.info(f"Closed {conn_type} connection")
            
            self.connections.clear()
            
        except Exception as e:
            self.logger.error(f"Error closing connections: {str(e)}")
