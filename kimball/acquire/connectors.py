"""
KIMBALL Data Source Connectors

This module provides simplified connectors for database sources only.
Storage sources now use the new bucket processor architecture.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData
import psycopg2
from psycopg2.extras import RealDictCursor
import clickhouse_connect
from datetime import datetime

from ..core.logger import Logger

class BaseConnector(ABC):
    """Base class for all data source connectors."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize connector with configuration."""
        self.config = config
        self.logger = Logger("connector")
        self.connection = None
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to data source."""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection to data source."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test the connection."""
        pass

class DatabaseConnector(BaseConnector):
    """Database connector for SQL databases."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.db_type = config.get("type", "postgres")
        self.connection = None
    
    def connect(self) -> bool:
        """Connect to database."""
        try:
            if self.db_type == "postgres":
                # PostgreSQL connection
                host = self.config.get("host")
                port = self.config.get("port", 5432)
                user = self.config.get("user")
                password = self.config.get("password")
                database = self.config.get("database")
                
                connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
                self.connection = create_engine(connection_string)
                
            elif self.db_type == "clickhouse":
                # ClickHouse connection
                host = self.config.get("host")
                port = self.config.get("port", 8123)
                user = self.config.get("user", "default")
                password = self.config.get("password", "")
                database = self.config.get("database", "default")
                
                self.connection = clickhouse_connect.get_client(
                    host=host,
                    port=port,
                    username=user,
                    password=password,
                    database=database
                )
            
            self.logger.info(f"Connected to {self.db_type} database")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from database."""
        if self.connection:
            try:
                if hasattr(self.connection, 'close'):
                    self.connection.close()
                elif hasattr(self.connection, 'disconnect'):
                    self.connection.disconnect()
                self.logger.info("Disconnected from database")
            except Exception as e:
                self.logger.error(f"Error disconnecting: {str(e)}")
    
    def test_connection(self) -> bool:
        """Test database connection."""
        if not self.connect():
            return False
        
        try:
            if self.db_type == "postgres":
                with self.connection.connect() as conn:
                    conn.execute(text("SELECT 1"))
            elif self.db_type == "clickhouse":
                self.connection.command("SELECT 1")
            
            self.logger.info("Database connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Database connection test failed: {str(e)}")
            return False
    
    def get_tables(self, schema: Optional[str] = None, table_pattern: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables in the database."""
        try:
            if not self.connect():
                return []
            
            tables = []
            
            if self.db_type == "postgres":
                with self.connection.connect() as conn:
                    query = """
                    SELECT table_name, table_schema 
                    FROM information_schema.tables 
                    WHERE table_schema = %s
                    """
                    if table_pattern:
                        query += " AND table_name LIKE %s"
                        result = conn.execute(text(query), (schema, f"%{table_pattern}%"))
                    else:
                        result = conn.execute(text(query), (schema,))
                    
                    for row in result:
                        tables.append({
                            "table_name": row[0],
                            "schema": row[1]
                        })
            
            elif self.db_type == "clickhouse":
                result = self.connection.query("SHOW TABLES")
                for row in result.result_rows:
                    tables.append({
                        "table_name": row[0],
                        "schema": "default"
                    })
            
            return tables
            
        except Exception as e:
            self.logger.error(f"Error getting tables: {str(e)}")
            return []
    
    def execute_query(self, query: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        try:
            if not self.connect():
                return []
            
            if limit:
                query = f"{query} LIMIT {limit}"
            
            results = []
            
            if self.db_type == "postgres":
                with self.connection.connect() as conn:
                    result = conn.execute(text(query))
                    for row in result:
                        results.append(dict(row._mapping))
            
            elif self.db_type == "clickhouse":
                result = self.connection.query(query)
                columns = [col[0] for col in result.column_names]
                for row in result.result_rows:
                    results.append(dict(zip(columns, row)))
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            return []
    
    def extract_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Extract all data from a table."""
        try:
            if not self.connect():
                return []
            
            query = f"SELECT * FROM {table_name}"
            return self.execute_query(query)
            
        except Exception as e:
            self.logger.error(f"Error extracting table {table_name}: {str(e)}")
            return []

class APIConnector(BaseConnector):
    """API connector for REST/GraphQL APIs."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = config.get("base_url", "")
        self.headers = config.get("headers", {})
        self.auth = config.get("auth")
    
    def connect(self) -> bool:
        """Test API connection."""
        try:
            import requests
            response = requests.get(f"{self.base_url}/health", headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Failed to connect to API: {str(e)}")
            return False
    
    def disconnect(self):
        """No persistent connection for APIs."""
        pass
    
    def test_connection(self) -> bool:
        """Test API connection."""
        return self.connect()
    
    def get_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Get data from API endpoint."""
        try:
            import requests
            response = requests.get(
                f"{self.base_url}/{endpoint}",
                headers=self.headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Error getting data from API: {str(e)}")
            return []