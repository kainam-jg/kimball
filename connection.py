"""
ClickHouse Database Connection Module

This module provides a reusable connection class for connecting to ClickHouse databases.
It reads configuration from config.json and provides methods for database operations.
"""

import json
import clickhouse_connect
from typing import Optional, Dict, Any, List
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseConnection:
    """
    A class to manage ClickHouse database connections and operations.
    """
    
    def __init__(self, config_file: str = "config.json"):
        """
        Initialize the ClickHouse connection with configuration from a JSON file.
        
        Args:
            config_file (str): Path to the configuration file
        """
        self.config = self._load_config(config_file)
        self.client = None
        
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """
        Load configuration from JSON file.
        
        Args:
            config_file (str): Path to the configuration file
            
        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            logger.info(f"Configuration loaded from {config_file}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in configuration file {config_file}")
            raise
    
    def connect(self) -> bool:
        """
        Establish connection to ClickHouse database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            clickhouse_config = self.config['clickhouse']
            self.client = clickhouse_connect.get_client(
                host=clickhouse_config['host'],
                port=clickhouse_config['port'],
                username=clickhouse_config['user'],
                password=clickhouse_config['password'],
                database=clickhouse_config.get('database', 'default')
            )
            
            # Test the connection
            result = self.client.command('SELECT 1')
            logger.info("Successfully connected to ClickHouse")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {str(e)}")
            return False
    
    def disconnect(self):
        """
        Close the database connection.
        """
        if self.client:
            self.client.close()
            logger.info("Disconnected from ClickHouse")
    
    def execute_query(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            Optional[List[Dict[str, Any]]]: Query results as list of dictionaries
        """
        if not self.client:
            logger.error("No active connection. Call connect() first.")
            return None
            
        try:
            result = self.client.query(query)
            return result.result_rows
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return None
    
    def get_tables(self) -> Optional[List[str]]:
        """
        Get list of all tables in the database.
        
        Returns:
            Optional[List[str]]: List of table names
        """
        query = "SHOW TABLES"
        result = self.execute_query(query)
        if result:
            return [row[0] for row in result]
        return None
    
    def get_table_schema(self, table_name: str) -> Optional[List[Dict[str, str]]]:
        """
        Get schema information for a specific table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            Optional[List[Dict[str, str]]]: List of column information
        """
        query = f"DESCRIBE TABLE {table_name}"
        result = self.execute_query(query)
        if result:
            return [{"name": row[0], "type": row[1]} for row in result]
        return None
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive information about a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            Optional[Dict[str, Any]]: Table information including row count and schema
        """
        try:
            # Get row count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count_result = self.execute_query(count_query)
            row_count = count_result[0][0] if count_result else 0
            
            # Get schema
            schema = self.get_table_schema(table_name)
            
            return {
                "table_name": table_name,
                "row_count": row_count,
                "columns": schema
            }
        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {str(e)}")
            return None


# Convenience function for quick connection
def get_clickhouse_connection(config_file: str = "config.json") -> ClickHouseConnection:
    """
    Create and return a ClickHouse connection instance.
    
    Args:
        config_file (str): Path to the configuration file
        
    Returns:
        ClickHouseConnection: Configured connection instance
    """
    return ClickHouseConnection(config_file)


if __name__ == "__main__":
    # Test the connection
    conn = get_clickhouse_connection()
    
    if conn.connect():
        print("Successfully connected to ClickHouse!")
        
        # Test getting tables
        tables = conn.get_tables()
        if tables:
            print(f"Found {len(tables)} tables:")
            for table in tables:
                print(f"  - {table}")
        else:
            print("No tables found or unable to retrieve table list")
        
        conn.disconnect()
    else:
        print("Failed to connect to ClickHouse")
