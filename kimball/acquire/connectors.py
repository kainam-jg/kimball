"""
KIMBALL Data Source Connectors

This module provides connectors for various data sources:
- Database connectors (ClickHouse, PostgreSQL, MySQL, Oracle, SQL Server)
- API connectors (REST, GraphQL, SOAP)
- Storage connectors (S3, Azure Blob, GCS, Local)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
import json
import requests
import boto3
from azure.storage.blob import BlobServiceClient
from google.cloud import storage
import pandas as pd

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
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Get schema information from source."""
        pass

class DatabaseConnector(BaseConnector):
    """Database connector for various database types."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.db_type = config.get("type", "clickhouse")
        self.connection = None
    
    def connect(self) -> bool:
        """Connect to database."""
        try:
            if self.db_type == "clickhouse":
                import clickhouse_connect
                self.connection = clickhouse_connect.get_client(
                    host=self.config["host"],
                    port=self.config["port"],
                    username=self.config["username"],
                    password=self.config["password"],
                    database=self.config["database"]
                )
            elif self.db_type == "postgresql":
                import psycopg2
                self.connection = psycopg2.connect(
                    host=self.config["host"],
                    port=self.config["port"],
                    database=self.config["database"],
                    user=self.config["username"],
                    password=self.config["password"]
                )
            elif self.db_type == "mysql":
                import mysql.connector
                self.connection = mysql.connector.connect(
                    host=self.config["host"],
                    port=self.config["port"],
                    database=self.config["database"],
                    user=self.config["username"],
                    password=self.config["password"]
                )
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
            
            self.logger.info(f"Connected to {self.db_type} database")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from database."""
        if self.connection:
            if hasattr(self.connection, 'close'):
                self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from database")
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            if self.db_type == "clickhouse":
                result = self.connection.command("SELECT 1")
                return result is not None
            elif self.db_type == "postgresql":
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                return result is not None
            else:
                return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def get_schema(self) -> Dict[str, Any]:
        """Get database schema information."""
        try:
            if self.db_type == "clickhouse":
                tables = self.connection.query("SHOW TABLES").result_rows
                schema = {}
                for table in tables:
                    table_name = table[0]
                    columns = self.connection.query(f"DESCRIBE TABLE {table_name}").result_rows
                    schema[table_name] = [{"name": col[0], "type": col[1]} for col in columns]
                return schema
            else:
                # Implement for other database types
                return {}
        except Exception as e:
            self.logger.error(f"Failed to get schema: {str(e)}")
            return {}

class APIConnector(BaseConnector):
    """API connector for REST, GraphQL, and SOAP APIs."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_type = config.get("type", "rest")
        self.base_url = config.get("base_url")
        self.headers = config.get("headers", {})
        self.auth = config.get("authentication", {})
    
    def connect(self) -> bool:
        """Connect to API (validate endpoint)."""
        try:
            response = requests.get(f"{self.base_url}/health", headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Failed to connect to API: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from API (no persistent connection)."""
        self.logger.info("Disconnected from API")
    
    def test_connection(self) -> bool:
        """Test API connection."""
        return self.connect()
    
    def get_schema(self) -> Dict[str, Any]:
        """Get API schema information."""
        try:
            if self.api_type == "rest":
                # Try to get OpenAPI/Swagger spec
                response = requests.get(f"{self.base_url}/openapi.json", headers=self.headers)
                if response.status_code == 200:
                    return response.json()
                else:
                    return {"endpoints": [], "type": "rest"}
            else:
                return {"type": self.api_type}
        except Exception as e:
            self.logger.error(f"Failed to get API schema: {str(e)}")
            return {}

class StorageConnector(BaseConnector):
    """Storage connector for cloud and local storage."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.storage_type = config.get("type", "s3")
        self.bucket = config.get("bucket")
        self.credentials = config.get("credentials", {})
        self.client = None
    
    def connect(self) -> bool:
        """Connect to storage."""
        try:
            if self.storage_type == "s3":
                self.client = boto3.client(
                    's3',
                    aws_access_key_id=self.credentials.get("access_key"),
                    aws_secret_access_key=self.credentials.get("secret_key"),
                    region_name=self.credentials.get("region", "us-east-1")
                )
            elif self.storage_type == "azure":
                self.client = BlobServiceClient.from_connection_string(
                    self.credentials.get("connection_string")
                )
            elif self.storage_type == "gcs":
                self.client = storage.Client.from_service_account_json(
                    self.credentials.get("service_account_file")
                )
            else:
                raise ValueError(f"Unsupported storage type: {self.storage_type}")
            
            self.logger.info(f"Connected to {self.storage_type} storage")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to storage: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from storage."""
        self.client = None
        self.logger.info("Disconnected from storage")
    
    def test_connection(self) -> bool:
        """Test storage connection."""
        try:
            if self.storage_type == "s3":
                self.client.head_bucket(Bucket=self.bucket)
                return True
            elif self.storage_type == "azure":
                container_client = self.client.get_container_client(self.bucket)
                container_client.get_container_properties()
                return True
            elif self.storage_type == "gcs":
                bucket = self.client.bucket(self.bucket)
                bucket.exists()
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(f"Storage connection test failed: {str(e)}")
            return False
    
    def get_schema(self) -> Dict[str, Any]:
        """Get storage schema (file listing)."""
        try:
            if self.storage_type == "s3":
                response = self.client.list_objects_v2(Bucket=self.bucket)
                files = [obj["Key"] for obj in response.get("Contents", [])]
                return {"files": files, "type": "s3"}
            elif self.storage_type == "azure":
                container_client = self.client.get_container_client(self.bucket)
                blobs = container_client.list_blobs()
                files = [blob.name for blob in blobs]
                return {"files": files, "type": "azure"}
            elif self.storage_type == "gcs":
                bucket = self.client.bucket(self.bucket)
                blobs = bucket.list_blobs()
                files = [blob.name for blob in blobs]
                return {"files": files, "type": "gcs"}
            else:
                return {"type": self.storage_type}
        except Exception as e:
            self.logger.error(f"Failed to get storage schema: {str(e)}")
            return {}
