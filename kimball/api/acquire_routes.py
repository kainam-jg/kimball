"""
KIMBALL Acquire Phase API Routes - PRODUCTION READY

This module provides comprehensive FastAPI routes for the Acquire phase,
including data source management, discovery, and extraction capabilities.

Features:
- Data source CRUD operations (Create, Read, Update, Delete)
- Connection testing for all source types
- S3 object discovery and exploration
- Database table discovery and SQL query execution
- Data extraction from storage sources (S3) with parallel processing
- Data extraction from database sources (PostgreSQL) with chunking
- Universal chunking and parallelization framework
- Stream-based data processing for optimal memory usage
- Automatic file type detection (CSV, Excel, Parquet)
- Unicode character cleaning and encoding handling
- ClickHouse bronze layer integration with optimized batch sizes
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel
import asyncio
import math

from ..acquire.source_manager import DataSourceManager
from ..core.logger import Logger
from ..core.config import Config

# Pydantic models for request bodies
class DataSourceConfigRequest(BaseModel):
    """Request model for creating a new data source."""
    name: str
    type: str  # postgres, s3, api
    enabled: bool = True
    description: Optional[str] = ""
    config: Dict[str, Any]

class DataSourceUpdateRequest(BaseModel):
    """Request model for updating an existing data source."""
    name: Optional[str] = None
    type: Optional[str] = None
    enabled: Optional[bool] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

class StorageExploreRequest(BaseModel):
    """Request model for exploring storage sources."""
    prefix: Optional[str] = ""
    max_keys: Optional[int] = None
    search_subdirectories: bool = True

class DatabaseExploreRequest(BaseModel):
    """Request model for exploring database sources."""
    schema_name: Optional[str] = None
    table_pattern: Optional[str] = None

class StorageExtractionRequest(BaseModel):
    """Request model for extracting data from storage sources."""
    object_keys: List[str]  # List of S3 object keys to extract
    target_tables: Optional[List[str]] = None  # Optional list of custom table names

class DatabaseTableExtractionRequest(BaseModel):
    """Request model for extracting entire database tables."""
    table_names: List[str]  # List of table names to extract
    target_tables: Optional[List[str]] = None  # Optional list of custom table names

class DatabaseSQLExtractionRequest(BaseModel):
    """Request model for extracting data using custom SQL queries."""
    sql_queries: List[str]  # List of SQL queries to execute
    target_tables: Optional[List[str]] = None  # Optional list of custom table names

class DatabaseExtractionRequest(BaseModel):
    """Request model for extracting data from database sources."""
    query: str
    target_table: Optional[str] = None

# Initialize router
acquire_router = APIRouter(prefix="/api/v1/acquire", tags=["Acquire"])
logger = Logger("acquire_api")

# Initialize data source manager
source_manager = DataSourceManager()

# ONLY ACTIVE ENDPOINT - Status
@acquire_router.get("/status")
async def get_acquire_status():
    """Get overall status of the Acquire phase."""
    try:
        logger.log_api_call("/acquire/status", "GET")
        
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        # Count enabled vs disabled sources
        enabled_count = sum(1 for source in data_sources.values() if source.get("enabled", True))
        disabled_count = len(data_sources) - enabled_count
        
        return {
            "status": "success",
            "phase": "acquire",
            "total_sources": len(data_sources),
            "enabled_sources": enabled_count,
            "disabled_sources": disabled_count,
            "source_types": list(set(source.get("type", "unknown") for source in data_sources.values())),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting acquire status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data Source Management Endpoints
@acquire_router.get("/datasources")
async def list_data_sources():
    """List all configured data sources."""
    try:
        logger.log_api_call("/acquire/datasources", "GET")
        
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        return {
            "status": "success",
            "data_sources": data_sources,
            "count": len(data_sources),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error listing data sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# COMMENTED OUT ALL OTHER ENDPOINTS FOR SYSTEMATIC TESTING
# Uncomment one at a time as we test each functionality

# # Data Source Management Endpoints
# @acquire_router.post("/datasources")
# async def create_data_source(request: DataSourceConfigRequest):
#     """Create a new data source configuration."""
#     pass

# @acquire_router.get("/datasources")
# async def list_data_sources():
#     """List all configured data sources."""
#     pass

@acquire_router.get("/datasources/{source_id}")
async def get_data_source(source_id: str):
    """Get a specific data source configuration."""
    try:
        logger.log_api_call(f"/acquire/datasources/{source_id}", "GET")
        
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        source_config = data_sources[source_id]
        
        return {
            "status": "success",
            "source_id": source_id,
            "source_config": source_config,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.put("/datasources/{source_id}")
async def update_data_source(source_id: str, request: DataSourceUpdateRequest):
    """Update an existing data source configuration."""
    try:
        logger.log_api_call(f"/acquire/datasources/{source_id}", "PUT", request_data=request.dict())
        
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        # Get existing configuration
        existing_config = data_sources[source_id]
        
        # Update fields if provided
        if request.name is not None:
            # If renaming, check if new name already exists
            if request.name != source_id and request.name in data_sources:
                raise HTTPException(status_code=400, detail=f"Data source '{request.name}' already exists")
            
            # If renaming, remove old entry and add new one
            if request.name != source_id:
                del data_sources[source_id]
                source_id = request.name
        
        if request.type is not None:
            # Validate source type
            valid_types = ["postgres", "s3", "api"]
            if request.type not in valid_types:
                raise HTTPException(status_code=400, detail=f"Invalid source type '{request.type}'. Must be one of: {valid_types}")
            existing_config["type"] = request.type
        
        if request.enabled is not None:
            existing_config["enabled"] = request.enabled
        
        if request.description is not None:
            existing_config["description"] = request.description
        
        if request.config is not None:
            # Merge config updates
            existing_config.update(request.config)
        
        # Update the data source
        data_sources[source_id] = existing_config
        config["data_sources"] = data_sources
        
        # Save configuration
        config_manager.save_config(config)
        
        return {
            "status": "success",
            "message": f"Data source '{source_id}' updated successfully",
            "source_id": source_id,
            "source_config": existing_config,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating data source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.delete("/datasources/{source_id}")
async def delete_data_source(source_id: str):
    """Delete a data source configuration."""
    try:
        logger.log_api_call(f"/acquire/datasources/{source_id}", "DELETE")
        
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        # Get the source config before deletion for response
        source_config = data_sources[source_id]
        
        # Remove the data source
        del data_sources[source_id]
        config["data_sources"] = data_sources
        
        # Save configuration
        config_manager.save_config(config)
        
        return {
            "status": "success",
            "message": f"Data source '{source_id}' deleted successfully",
            "deleted_source_id": source_id,
            "deleted_source_config": source_config,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting data source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/datasources")
async def create_data_source(request: DataSourceConfigRequest):
    """Create a new data source configuration."""
    try:
        logger.log_api_call("/acquire/datasources", "POST", request_data=request.dict())
        
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        # Check if data source already exists
        if request.name in data_sources:
            raise HTTPException(status_code=400, detail=f"Data source '{request.name}' already exists")
        
        # Validate source type
        valid_types = ["postgres", "s3", "api"]
        if request.type not in valid_types:
            raise HTTPException(status_code=400, detail=f"Invalid source type '{request.type}'. Must be one of: {valid_types}")
        
        # Create new data source configuration
        new_source = {
            "type": request.type,
            "enabled": request.enabled,
            "description": request.description,
            **request.config
        }
        
        # Add to configuration
        data_sources[request.name] = new_source
        config["data_sources"] = data_sources
        
        # Save configuration
        config_manager.save_config(config)
        
        return {
            "status": "success",
            "message": f"Data source '{request.name}' created successfully",
            "source_name": request.name,
            "source_type": request.type,
            "enabled": request.enabled,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/explore/storage/{source_id}")
async def explore_storage_source(source_id: str, request: StorageExploreRequest):
    """Explore objects in a storage source (S3, Azure, GCP, etc.)."""
    try:
        logger.log_api_call(f"/acquire/explore/storage/{source_id}", "POST", request_data=request.dict())
        
        # Get the source configuration
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        source_config = data_sources[source_id]
        source_type = source_config.get("type")
        
        # Validate that this is a storage source
        storage_types = ["s3", "azure", "gcp"]
        if source_type not in storage_types:
            raise HTTPException(status_code=400, detail=f"Source '{source_id}' is not a storage source. Type: {source_type}")
        
        # Handle different storage types
        if source_type == "s3":
            return await _explore_s3_objects(source_config, request)
        elif source_type == "azure":
            return await _explore_azure_objects(source_config, request)
        elif source_type == "gcp":
            return await _explore_gcp_objects(source_config, request)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported storage type: {source_type}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exploring storage source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def _explore_s3_objects(source_config: Dict[str, Any], request: StorageExploreRequest) -> Dict[str, Any]:
    """Explore S3 objects using boto3."""
    try:
        import boto3
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=source_config.get("access_key"),
            aws_secret_access_key=source_config.get("secret_key"),
            region_name=source_config.get("region", "us-east-1")
        )
        
        bucket_name = source_config.get("bucket")
        if not bucket_name:
            raise HTTPException(status_code=400, detail="S3 bucket name not configured")
        
        # Prepare list_objects_v2 parameters
        kwargs = {
            'Bucket': bucket_name,
            'Prefix': request.prefix
        }
        
        if request.max_keys:
            kwargs['MaxKeys'] = request.max_keys
        
        if not request.search_subdirectories:
            kwargs['Delimiter'] = '/'
        
        # List objects
        response = s3_client.list_objects_v2(**kwargs)
        
        objects = []
        if 'Contents' in response:
            for obj in response['Contents']:
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'etag': obj['ETag'],
                    'storage_class': obj.get('StorageClass', 'STANDARD')
                })
        
        # Handle common prefixes (folders) if delimiter is used
        folders = []
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                folders.append({
                    'prefix': prefix['Prefix'],
                    'type': 'folder'
                })
        
        return {
            "status": "success",
            "source_type": "s3",
            "bucket": bucket_name,
            "prefix": request.prefix,
            "max_keys": request.max_keys,
            "search_subdirectories": request.search_subdirectories,
            "objects": objects,
            "folders": folders,
            "object_count": len(objects),
            "folder_count": len(folders),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error exploring S3 objects: {e}")
        raise HTTPException(status_code=500, detail=f"S3 exploration failed: {str(e)}")

async def _explore_azure_objects(source_config: Dict[str, Any], request: StorageExploreRequest) -> Dict[str, Any]:
    """Explore Azure Blob Storage objects."""
    # TODO: Implement Azure Blob Storage exploration
    raise HTTPException(status_code=501, detail="Azure Blob Storage exploration not yet implemented")

async def _explore_gcp_objects(source_config: Dict[str, Any], request: StorageExploreRequest) -> Dict[str, Any]:
    """Explore Google Cloud Storage objects."""
    # TODO: Implement GCP Storage exploration
    raise HTTPException(status_code=501, detail="GCP Storage exploration not yet implemented")

@acquire_router.post("/explore/database/{source_id}")
async def explore_database_source(source_id: str, request: DatabaseExploreRequest):
    """Explore tables in a database source."""
    try:
        logger.log_api_call(f"/acquire/explore/database/{source_id}", "POST", request_data=request.dict())
        
        # Get the source configuration
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        source_config = data_sources[source_id]
        source_type = source_config.get("type")
        
        # Validate that this is a database source
        database_types = ["postgres", "mysql", "sqlserver", "oracle", "clickhouse"]
        if source_type not in database_types:
            raise HTTPException(status_code=400, detail=f"Source '{source_id}' is not a database source. Type: {source_type}")
        
        # Handle different database types
        if source_type == "postgres":
            return await _explore_postgres_tables(source_config, request)
        elif source_type == "mysql":
            return await _explore_mysql_tables(source_config, request)
        elif source_type == "clickhouse":
            return await _explore_clickhouse_tables(source_config, request)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported database type: {source_type}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exploring database source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def _explore_postgres_tables(source_config: Dict[str, Any], request: DatabaseExploreRequest) -> Dict[str, Any]:
    """Explore PostgreSQL tables."""
    try:
        from sqlalchemy import create_engine, text
        
        # Create connection string
        conn_str = f"postgresql+psycopg2://{source_config['user']}:{source_config['password']}@{source_config['host']}:{source_config['port']}/{source_config['database']}"
        engine = create_engine(conn_str)
        
        with engine.connect() as connection:
            # Build the query to get tables
            schema_filter = ""
            if request.schema_name:
                schema_filter = f"AND table_schema = '{request.schema_name}'"
            
            table_pattern_filter = ""
            if request.table_pattern:
                table_pattern_filter = f"AND table_name LIKE '{request.table_pattern}'"
            
            query = f"""
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            {schema_filter}
            {table_pattern_filter}
            ORDER BY table_schema, table_name
            """
            
            result = connection.execute(text(query))
            tables = []
            
            for row in result:
                tables.append({
                    'schema': row[0],
                    'table_name': row[1],
                    'table_type': row[2],
                    'full_name': f"{row[0]}.{row[1]}"
                })
            
            return {
                "status": "success",
                "source_type": "postgres",
                "database": source_config.get("database"),
                "schema_filter": request.schema_name,
                "table_pattern": request.table_pattern,
                "tables": tables,
                "table_count": len(tables),
                "timestamp": datetime.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error exploring PostgreSQL tables: {e}")
        raise HTTPException(status_code=500, detail=f"PostgreSQL exploration failed: {str(e)}")

async def _explore_mysql_tables(source_config: Dict[str, Any], request: DatabaseExploreRequest) -> Dict[str, Any]:
    """Explore MySQL tables."""
    # TODO: Implement MySQL table exploration
    raise HTTPException(status_code=501, detail="MySQL table exploration not yet implemented")

async def _explore_clickhouse_tables(source_config: Dict[str, Any], request: DatabaseExploreRequest) -> Dict[str, Any]:
    """Explore ClickHouse tables."""
    # TODO: Implement ClickHouse table exploration
    raise HTTPException(status_code=501, detail="ClickHouse table exploration not yet implemented")

@acquire_router.post("/extract/storage/{source_id}")
async def extract_from_storage_source(source_id: str, request: StorageExtractionRequest):
    """Extract data from a storage source and load it into the bronze layer."""
    try:
        logger.log_api_call(f"/acquire/extract/storage/{source_id}", "POST", request_data=request.dict())
        
        # Get the source configuration
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        source_config = data_sources[source_id]
        source_type = source_config.get("type")
        
        # Validate that this is a storage source
        storage_types = ["s3", "azure", "gcp"]
        if source_type not in storage_types:
            raise HTTPException(status_code=400, detail=f"Source '{source_id}' is not a storage source. Type: {source_type}")
        
        # Validate input
        if not request.object_keys:
            raise HTTPException(status_code=400, detail="No object keys provided")
        
        if request.target_tables and len(request.target_tables) != len(request.object_keys):
            raise HTTPException(status_code=400, detail="Number of target tables must match number of object keys")
        
        # Handle different storage types with parallel processing
        if source_type == "s3":
            results = await _extract_multiple_from_s3(source_config, request)
        elif source_type == "azure":
            raise HTTPException(status_code=501, detail="Azure Blob Storage extraction not yet implemented")
        elif source_type == "gcp":
            raise HTTPException(status_code=501, detail="GCP Storage extraction not yet implemented")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported storage type: {source_type}")
        
        return {
            "status": "success",
            "source_type": source_type,
            "total_files": len(request.object_keys),
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error extracting from storage source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def _extract_multiple_from_s3(source_config: Dict[str, Any], request: StorageExtractionRequest) -> List[Dict[str, Any]]:
    """Extract multiple files from S3 in parallel and load them into ClickHouse bronze layer."""
    try:
        import asyncio
        import concurrent.futures
        from datetime import datetime
        
        # Create S3 client
        import boto3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=source_config.get("access_key"),
            aws_secret_access_key=source_config.get("secret_key"),
            region_name=source_config.get("region", "us-east-1")
        )
        
        bucket_name = source_config.get("bucket")
        if not bucket_name:
            raise HTTPException(status_code=400, detail="S3 bucket name not configured")
        
        # Create tasks for parallel processing
        tasks = []
        for i, object_key in enumerate(request.object_keys):
            # Generate target table name
            if request.target_tables and i < len(request.target_tables):
                target_table = request.target_tables[i]
            else:
                # Extract filename from object key
                filename = object_key.split('/')[-1]
                # Remove extension
                table_name = filename.split('.')[0]
                target_table = table_name
            
            # Create task for this file
            task = _process_single_s3_file(s3_client, bucket_name, object_key, target_table)
            tasks.append(task)
        
        # Execute all tasks in parallel
        logger.info(f"Processing {len(tasks)} files in parallel from S3")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "object_key": request.object_keys[i],
                    "status": "error",
                    "error": str(result),
                    "records_extracted": 0,
                    "records_loaded": 0
                })
            else:
                processed_results.append(result)
        
        return processed_results
        
    except Exception as e:
        logger.error(f"Error in parallel S3 extraction: {e}")
        raise HTTPException(status_code=500, detail=f"Parallel S3 extraction failed: {str(e)}")

async def _process_single_s3_file(s3_client, bucket_name: str, object_key: str, target_table: str) -> Dict[str, Any]:
    """Process a single S3 file and load it into ClickHouse."""
    try:
        from datetime import datetime

        # Download the file from S3
        logger.info(f"Downloading file from S3: {bucket_name}/{object_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read()

        # Detect file type from extension
        file_extension = object_key.split('.')[-1].lower()
        logger.info(f"Processing file: {object_key}, detected type: {file_extension}")

        if file_extension == 'csv':
            data = await _parse_csv_data(file_content)
        elif file_extension in ['xlsx', 'xls']:
            data = await _parse_excel_data(file_content)
        elif file_extension == 'parquet':
            data = await _parse_parquet_data(file_content)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_extension}")

        if not data:
            raise HTTPException(status_code=400, detail="No data found in file")

        logger.info(f"Successfully parsed {len(data)} records from {object_key}")

        # Get column names from first row (header)
        column_names = list(data[0].keys())
        logger.info(f"File columns: {column_names}")

        # Initialize ClickHouse table BEFORE loading data
        from ..core.database import DatabaseManager
        db_manager = DatabaseManager()
        
        # Drop existing table if it exists
        drop_table_sql = f"DROP TABLE IF EXISTS bronze.{target_table}"
        db_manager.execute_query(drop_table_sql)
        logger.info(f"Dropped existing table bronze.{target_table}")
        
        # Create table with proper column names from file header
        create_table_sql = f"""
        CREATE TABLE bronze.{target_table} (
            {', '.join([f'`{col}` String' for col in column_names])},
            `create_date` String
        ) ENGINE = MergeTree()
        ORDER BY create_date
        """
        
        db_manager.execute_query(create_table_sql)
        logger.info(f"Created table bronze.{target_table} with columns: {column_names}")

        # Load data into ClickHouse
        logger.info(f"Starting to load {len(data)} records to ClickHouse")
        records_loaded = await _load_data_to_clickhouse(data, target_table, column_names)
        logger.info(f"Completed loading: {records_loaded} records loaded to ClickHouse")
        
        return {
            "object_key": object_key,
            "file_type": file_extension,
            "target_table": target_table,
            "records_extracted": len(data),
            "records_loaded": records_loaded,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Error processing S3 file {object_key}: {e}")
        raise



def _clean_text_content(text: str) -> str:
    """Clean text content by removing/replacing problematic characters."""
    if not text:
        return ""
    
    # Convert to string if not already
    text = str(text)
    
    # Common encoding issues and their fixes
    encoding_fixes = {
        'â€"': '–',  # En-dash
        'â€"': '—',  # Em-dash
        'â€™': "'",  # Right single quotation mark
        'â€œ': '"',  # Left double quotation mark
        'â€': '"',   # Right double quotation mark
        'â€¢': '•',  # Bullet point
        'â€¦': '…',  # Horizontal ellipsis
        'â€': '°',   # Degree symbol
        'â€': '±',   # Plus-minus sign
        'â€': '×',   # Multiplication sign
        'â€': '÷',   # Division sign
        'â€': '€',   # Euro sign
        'â€': '£',   # Pound sign
        'â€': '¥',   # Yen sign
        'â€': '©',   # Copyright sign
        'â€': '®',   # Registered trademark
        'â€': '™',   # Trademark
    }
    
    # Apply encoding fixes
    for bad_char, good_char in encoding_fixes.items():
        text = text.replace(bad_char, good_char)
    
    # Remove any remaining non-display characters (ASCII < 32 except \t\n\r)
    text = ''.join(char for char in text if ord(char) >= 32 or char in '\t\n\r')
    
    # Remove any remaining problematic Unicode characters
    text = ''.join(char for char in text if ord(char) < 0x10000)  # Keep only BMP characters
    
    return text.strip()

async def _detect_and_decode_content(file_content: bytes) -> str:
    """Detect encoding and decode file content to string for all file types."""
    try:
        import chardet
        
        logger.info(f"Detecting encoding for content size: {len(file_content)} bytes")
        
        # Detect encoding
        detected = chardet.detect(file_content)
        encoding = detected.get('encoding', 'utf-8')
        confidence = detected.get('confidence', 0)
        
        logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
        
        # Try detected encoding first
        try:
            content_str = file_content.decode(encoding)
            logger.info(f"Successfully decoded with {encoding}")
            return content_str
        except (UnicodeDecodeError, LookupError) as e:
            logger.warning(f"Failed to decode with {encoding}: {e}")
        
        # Fallback encodings to try
        fallback_encodings = ['utf-8', 'latin1', 'windows-1252', 'iso-8859-1', 'cp1252']
        
        for fallback_encoding in fallback_encodings:
            if fallback_encoding == encoding:  # Skip if already tried
                continue
                
            try:
                content_str = file_content.decode(fallback_encoding)
                logger.info(f"Successfully decoded with fallback encoding: {fallback_encoding}")
                return content_str
            except (UnicodeDecodeError, LookupError) as e:
                logger.warning(f"Failed to decode with {fallback_encoding}: {e}")
                continue
        
        # Last resort: decode with errors='replace' to handle any remaining issues
        logger.warning("All encodings failed, using utf-8 with error replacement")
        content_str = file_content.decode('utf-8', errors='replace')
        return content_str
        
    except Exception as e:
        logger.error(f"Error in encoding detection: {e}")
        # Last resort fallback
        return file_content.decode('utf-8', errors='replace')

async def _parse_csv_data(file_content: bytes) -> List[Dict[str, Any]]:
    """Parse CSV data from bytes using stream-based approach."""
    try:
        import io
        import csv
        
        logger.info(f"Starting CSV file parsing, content size: {len(file_content)} bytes")
        
        # Use universal encoding detection and conversion
        content_str = await _detect_and_decode_content(file_content)
        
        # Parse CSV using stream
        csv_reader = csv.DictReader(io.StringIO(content_str))
        data = []
        
        for row in csv_reader:
            # Convert all values to strings and clean encoding issues
            string_row = {key: _clean_text_content(str(value)) if value is not None else "" for key, value in row.items()}
            data.append(string_row)
        
        logger.info(f"CSV parsing completed, {len(data)} records processed")
        return data
        
    except Exception as e:
        logger.error(f"Error parsing CSV data: {e}")
        raise

async def _parse_excel_data(file_content: bytes) -> List[Dict[str, Any]]:
    """Parse Excel data from bytes using stream-based approach."""
    try:
        import openpyxl
        import io
        
        logger.info(f"Starting Excel file parsing, content size: {len(file_content)} bytes")
        
        # Read Excel file from bytes using openpyxl
        excel_file = io.BytesIO(file_content)
        workbook = openpyxl.load_workbook(excel_file, read_only=True)
        
        # Get the first sheet
        sheet = workbook.active
        logger.info(f"Reading Excel sheet: {sheet.title}")
        
        # Read headers from first row
        headers = []
        for cell in sheet[1]:
            cell_value = cell.value
            # Handle encoding for text content
            if cell_value is not None:
                # Convert to string and clean encoding issues
                cell_str = _clean_text_content(str(cell_value))
                headers.append(cell_str)
            else:
                headers.append(f"Column_{len(headers)+1}")
        
        logger.info(f"Excel headers: {headers}")
        
        # Read data rows
        data = []
        row_count = 0
        for row in sheet.iter_rows(min_row=2, values_only=True):  # Skip header row
            row_count += 1
            # Convert row to dictionary with string values
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(headers):
                    # Handle encoding and clean non-display characters
                    if value is not None:
                        value_str = _clean_text_content(str(value))
                        row_dict[headers[i]] = value_str
                    else:
                        row_dict[headers[i]] = ""
            data.append(row_dict)
        
        workbook.close()
        logger.info(f"Excel parsing completed, {len(data)} records processed")
        return data
        
    except ImportError as e:
        logger.error(f"Missing dependency for Excel parsing: {e}")
        raise HTTPException(status_code=500, detail=f"Excel parsing dependency missing: {str(e)}")
    except Exception as e:
        logger.error(f"Error parsing Excel data: {e}")
        logger.error(f"Excel parsing error type: {type(e).__name__}")
        raise

async def _parse_parquet_data(file_content: bytes) -> List[Dict[str, Any]]:
    """Parse Parquet data from bytes using stream-based approach."""
    try:
        import pyarrow.parquet as pq
        import io
        
        logger.info(f"Starting Parquet file parsing, content size: {len(file_content)} bytes")
        
        # Read Parquet file from bytes using stream
        parquet_file = io.BytesIO(file_content)
        table = pq.read_table(parquet_file)
        
        # Convert to list of dictionaries with string values
        data = []
        for batch in table.to_batches():
            for row in batch.to_pydict():
                string_row = {key: str(value) if value is not None else "" for key, value in row.items()}
                data.append(string_row)
        
        logger.info(f"Parquet parsing completed, {len(data)} records processed")
        return data
        
    except ImportError as e:
        logger.error(f"Missing dependency for Parquet parsing: {e}")
        raise HTTPException(status_code=500, detail=f"Parquet parsing dependency missing: {str(e)}")
    except Exception as e:
        logger.error(f"Error parsing Parquet data: {e}")
        logger.error(f"Parquet parsing error type: {type(e).__name__}")
        raise

async def _load_data_to_clickhouse(data: List[Dict[str, Any]], target_table: str) -> int:
    """Load data into ClickHouse bronze layer."""
    try:
        from ..core.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        if not data:
            return 0
        
        # Get sample record to determine schema
        sample_record = data[0]
        columns = list(sample_record.keys())
        
        # Add create_date column to all records
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for record in data:
            record['create_date'] = current_date
        
        # Add create_date to columns list
        columns.append('create_date')
        
        # Check if table exists and truncate if it does, otherwise create it
        check_table_sql = f"EXISTS TABLE bronze.{target_table}"
        table_exists_result = db_manager.execute_query(check_table_sql)
        
        if table_exists_result and len(table_exists_result) > 0 and table_exists_result[0].get('result', 0) == 1:
            # Table exists - truncate it to replace data
            truncate_sql = f"TRUNCATE TABLE bronze.{target_table}"
            db_manager.execute_query(truncate_sql)
            logger.info(f"Truncated existing table bronze.{target_table}")
        else:
            # Table doesn't exist - create it
            create_table_sql = f"""
            CREATE TABLE bronze.{target_table} (
                {', '.join([f'`{col}` String' for col in columns])}
            ) ENGINE = MergeTree() ORDER BY `create_date`
            """
            db_manager.execute_query(create_table_sql)
            logger.info(f"Created new table bronze.{target_table} with columns: {columns}")
        
        # Insert data in batches using ClickHouse native insert method
        batch_size = 10000  # Increased batch size for better performance
        total_inserted = 0
        
        # Get ClickHouse connection directly for native insert
        conn = db_manager.get_connection("clickhouse")
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            # Debug: Log problematic characters before insertion
            for record in batch:
                for col, value in record.items():
                    if 'â€' in str(value) or any(ord(char) > 127 for char in str(value)):
                        logger.warning(f"Found problematic characters in {col}: {repr(value)}")
            
            # Use ClickHouse native insert method
            try:
                # Convert data to list of tuples for ClickHouse insert
                insert_data = []
                for record in batch:
                    row_tuple = tuple(str(record.get(col, "")) for col in columns)
                    insert_data.append(row_tuple)
                
                # Use ClickHouse native insert
                conn.insert(f"bronze.{target_table}", insert_data, column_names=columns)
                
                total_inserted += len(batch)
                logger.info(f"Inserted batch of {len(batch)} records to bronze.{target_table} using native insert")
                
            except Exception as insert_error:
                logger.error(f"Native insert failed, falling back to SQL: {insert_error}")
                
                # Fallback to SQL insert with proper Unicode handling
                columns_str = ', '.join([f'`{col}`' for col in columns])
                values_list = []
                
                for record in batch:
                    row_values = []
                    for col in columns:
                        value = str(record.get(col, ""))
                        # Escape single quotes
                        value = value.replace("'", "''")
                        row_values.append(f"'{value}'")
                    values_list.append(f"({', '.join(row_values)})")
                
                insert_sql = f"INSERT INTO bronze.{target_table} ({columns_str}) VALUES {', '.join(values_list)}"
                db_manager.execute_query(insert_sql)
                
                total_inserted += len(batch)
                logger.info(f"Inserted batch of {len(batch)} records to bronze.{target_table} using SQL fallback")
        
        return total_inserted
        
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {e}")
        raise


async def _extract_multiple_from_database(source_config: Dict[str, Any], request: DatabaseTableExtractionRequest) -> List[Dict[str, Any]]:
    """Extract multiple tables from database in parallel and load them into ClickHouse bronze layer."""
    try:
        import asyncio
        import concurrent.futures
        from datetime import datetime

        # Create database connection
        from ..acquire.connectors import DatabaseConnector
        db_connector = DatabaseConnector(source_config)
        
        # Test connection first
        if not db_connector.test_connection():
            raise HTTPException(status_code=400, detail="Database connection failed")

        # Create tasks for parallel processing
        tasks = []
        for i, table_name in enumerate(request.table_names):
            # Generate target table name
            if request.target_tables and i < len(request.target_tables):
                target_table = request.target_tables[i]
            else:
                # Use source table name as target table name
                target_table = table_name.lower().replace(' ', '_')
            
            # Create task for this table (use chunked processing for large tables)
            task = _process_single_database_table_chunked(db_connector, table_name, target_table)
            tasks.append(task)

        # Execute all tasks in parallel
        logger.info(f"Processing {len(tasks)} database tables in parallel")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "table_name": request.table_names[i],
                    "status": "error",
                    "error": str(result),
                    "records_extracted": 0,
                    "records_loaded": 0
                })
            else:
                processed_results.append(result)

        return processed_results

    except Exception as e:
        logger.error(f"Error in parallel database extraction: {e}")
        raise HTTPException(status_code=500, detail=f"Parallel database extraction failed: {str(e)}")

async def _extract_multiple_from_database_sql(source_config: Dict[str, Any], request: DatabaseSQLExtractionRequest) -> List[Dict[str, Any]]:
    """Extract data using multiple SQL queries in parallel and load them into ClickHouse bronze layer."""
    try:
        import asyncio
        import concurrent.futures
        from datetime import datetime

        # Create database connection
        from ..acquire.connectors import DatabaseConnector
        db_connector = DatabaseConnector(source_config)
        
        # Test connection first
        if not db_connector.test_connection():
            raise HTTPException(status_code=400, detail="Database connection failed")

        # Create tasks for parallel processing
        tasks = []
        for i, sql_query in enumerate(request.sql_queries):
            # Generate target table name
            if request.target_tables and i < len(request.target_tables):
                target_table = request.target_tables[i]
            else:
                # Extract table name from SQL query
                target_table = _extract_table_name_from_sql(sql_query)
            
            # Create task for this SQL query
            task = _process_single_database_sql(db_connector, sql_query, target_table)
            tasks.append(task)

        # Execute all tasks in parallel
        logger.info(f"Processing {len(tasks)} SQL queries in parallel")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "sql_query": request.sql_queries[i][:100] + "..." if len(request.sql_queries[i]) > 100 else request.sql_queries[i],
                    "status": "error",
                    "error": str(result),
                    "records_extracted": 0,
                    "records_loaded": 0
                })
            else:
                processed_results.append(result)

        return processed_results

    except Exception as e:
        logger.error(f"Error in parallel SQL extraction: {e}")
        raise HTTPException(status_code=500, detail=f"Parallel SQL extraction failed: {str(e)}")



def _convert_database_results_to_strings(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert database results to string streams using common framework."""
    try:
        string_data = []
        
        for record in data:
            # Convert all values to strings using our common text cleaning function
            string_record = {}
            for key, value in record.items():
                if value is not None:
                    # Use our common text cleaning function
                    string_record[key] = _clean_text_content(str(value))
                else:
                    string_record[key] = ""
            string_data.append(string_record)
        
        logger.info(f"Converted {len(data)} database records to string streams")
        return string_data
        
    except Exception as e:
        logger.error(f"Error converting database results to strings: {e}")
        raise

def _extract_table_name_from_sql(sql_query: str) -> str:
    """Extract table name from SQL query, ignoring schema prefix."""
    try:
        import re
        
        # Convert to lowercase for easier parsing
        sql_lower = sql_query.lower().strip()
        
        # Look for FROM clause
        from_match = re.search(r'from\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)', sql_lower)
        if from_match:
            table_name = from_match.group(1)
            # Remove schema prefix if present (e.g., "vehicles.daily_sales" -> "daily_sales")
            if '.' in table_name:
                table_name = table_name.split('.')[-1]
            return table_name
        
        # Look for INTO clause (for INSERT statements)
        into_match = re.search(r'into\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)', sql_lower)
        if into_match:
            table_name = into_match.group(1)
            if '.' in table_name:
                table_name = table_name.split('.')[-1]
            return table_name
        
        # Fallback: generate a generic name
        return f"sql_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    except Exception as e:
        logger.error(f"Error extracting table name from SQL: {e}")
        return f"sql_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

async def _process_single_database_table_chunked(db_connector, table_name: str, target_table: str) -> Dict[str, Any]:
    """Process a single database table with chunking for large tables."""
    try:
        # First, get the total count to determine chunking strategy
        count_query = f"SELECT COUNT(*) as total_count FROM {table_name}"
        count_result = db_connector.execute_query(count_query)
        total_count = count_result[0]['total_count'] if count_result else 0
        
        logger.info(f"Table {table_name} has {total_count} total records")
        
        if total_count == 0:
            raise HTTPException(status_code=400, detail=f"No data found in table {table_name}")
        
        # Determine chunk size based on table size
        if total_count > 1000000:  # > 1M records
            chunk_size = 50000
            batch_size = 1000
        elif total_count > 100000:  # > 100K records
            chunk_size = 10000
            batch_size = 1000
        else:
            chunk_size = 5000
            batch_size = 1000
        
        logger.info(f"Using chunk size: {chunk_size}, batch size: {batch_size}")
        
        # Process table in chunks
        total_extracted = 0
        total_loaded = 0
        offset = 0
        
        while offset < total_count:
            # Extract chunk
            chunk_query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
            logger.info(f"Extracting chunk: {offset} to {offset + chunk_size}")
            
            chunk_data = db_connector.execute_query(chunk_query)
            if not chunk_data:
                break
            
            logger.info(f"Extracted {len(chunk_data)} records from chunk")
            
            # Convert to string streams
            string_data = _convert_database_results_to_strings(chunk_data)
            
            # Load chunk into ClickHouse
            records_loaded = await _load_data_to_clickhouse(string_data, target_table)
            
            total_extracted += len(chunk_data)
            total_loaded += records_loaded
            
            logger.info(f"Loaded {records_loaded} records from chunk. Total: {total_loaded}")
            
            offset += chunk_size
            
            # If we got fewer records than chunk_size, we're done
            if len(chunk_data) < chunk_size:
                break
        
        return {
            "table_name": table_name,
            "target_table": target_table,
            "records_extracted": total_extracted,
            "records_loaded": total_loaded,
            "status": "success"
        }

    except Exception as e:
        logger.error(f"Error processing database table {table_name}: {e}")
        raise


@acquire_router.post("/extract/database/{source_id}")
async def extract_from_database_source(source_id: str, request: DatabaseTableExtractionRequest):
    """Extract entire tables from a database source and load them into the bronze layer."""
    try:
        logger.log_api_call(f"/acquire/extract/database/{source_id}", "POST", request_data=request.dict())

        # Get the source configuration
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})

        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")

        source_config = data_sources[source_id]
        source_type = source_config.get("type")

        # Validate that this is a database source
        if source_type not in ["postgres", "postgresql"]:
            raise HTTPException(status_code=400, detail=f"Source '{source_id}' is not a database source. Type: {source_type}")

        # Validate input
        if not request.table_names:
            raise HTTPException(status_code=400, detail="No table names provided")

        if request.target_tables and len(request.target_tables) != len(request.table_names):
            raise HTTPException(status_code=400, detail="Number of target tables must match number of source tables")

        # Extract data from database tables
        results = await _extract_multiple_from_database(source_config, request)

        return {
            "status": "success",
            "source_type": source_type,
            "total_tables": len(request.table_names),
            "results": results,
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error extracting from database source {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/extract/database-sql/{source_id}")
async def extract_from_database_sql(source_id: str, request: DatabaseSQLExtractionRequest):
    """Extract data using custom SQL queries from a database source and load them into the bronze layer."""
    try:
        logger.log_api_call(f"/acquire/extract/database-sql/{source_id}", "POST", request_data=request.dict())

        # Get the source configuration
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})

        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")

        source_config = data_sources[source_id]
        source_type = source_config.get("type")

        # Validate that this is a database source
        if source_type not in ["postgres", "postgresql"]:
            raise HTTPException(status_code=400, detail=f"Source '{source_id}' is not a database source. Type: {source_type}")

        # Validate input
        if not request.sql_queries:
            raise HTTPException(status_code=400, detail="No SQL queries provided")

        if request.target_tables and len(request.target_tables) != len(request.sql_queries):
            raise HTTPException(status_code=400, detail="Number of target tables must match number of SQL queries")

        # Extract data using custom SQL
        results = await _extract_multiple_from_database_sql(source_config, request)

        return {
            "status": "success",
            "source_type": source_type,
            "total_queries": len(request.sql_queries),
            "results": results,
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error extracting from database SQL {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/test/{source_id}")
async def test_data_source_connection(source_id: str):
    """Test connection to a data source."""
    try:
        logger.log_api_call(f"/acquire/test/{source_id}", "GET")
        
        # Get the source configuration
        config_manager = Config()
        config = config_manager.get_config()
        data_sources = config.get("data_sources", {})
        
        if source_id not in data_sources:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        source_config = data_sources[source_id]
        
        # Test the connection using DataSourceManager
        test_result = source_manager.test_source_connection(source_id)
        
        return {
            "status": "success" if test_result else "failed",
            "source_id": source_id,
            "source_type": source_config.get("type"),
            "connection_test": test_result,
            "message": f"Connection test {'passed' if test_result else 'failed'} for {source_id}",
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing connection to {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# # Discovery Endpoints
# @acquire_router.post("/discover/s3-objects")
# async def discover_s3_objects(request: S3ObjectRequest):
#     """Discover objects in S3 bucket."""
#     pass

# @acquire_router.post("/discover/database-tables/{source_id}")
# async def discover_database_tables(source_id: str, request: DatabaseTableRequest):
#     """Discover tables in a database source."""
#     pass

# @acquire_router.post("/execute/sql-query/{source_id}")
# async def execute_sql_query(source_id: str, request: SQLQueryRequest):
#     """Execute a custom SQL query against a database source."""
#     pass

# Helper Functions for Database Extraction

def _extract_table_name_from_sql(sql_query: str) -> str:
    """Extract table name from SQL query, removing ALL schema prefixes and sanitizing for ClickHouse."""
    import re
    
    # Look for table name after FROM keyword - handle multiple dots
    match = re.search(r'FROM\s+([^\s,]+)', sql_query, re.IGNORECASE)
    if match:
        full_table_name = match.group(1)
        # Remove any quotes and take the last part after any dots
        full_table_name = full_table_name.strip('"\'`')
        table_name = full_table_name.split('.')[-1]  # Always take the last part
    else:
        # Fallback: try to extract any word that looks like a table name
        words = re.findall(r'\b\w+\b', sql_query)
        table_name = words[-1] if words else "extracted_data"
    
    # Sanitize table name for ClickHouse (remove special chars, but keep alphanumeric and underscores)
    table_name = re.sub(r'[^a-zA-Z0-9_]', '', table_name)  # Remove special chars completely
    table_name = table_name.lower()
    
    return table_name

async def _extract_multiple_from_database(source_config: Dict[str, Any], request: DatabaseTableExtractionRequest) -> List[Dict[str, Any]]:
    """Extract multiple database tables in parallel."""
    try:
        from ..acquire.connectors import DatabaseConnector
        
        connector = DatabaseConnector(source_config)
        connector.connect()
        
        # Process tables in parallel
        tasks = []
        for i, table_name in enumerate(request.table_names):
            # Extract clean table name (remove schema prefix)
            clean_table_name = table_name.split('.')[-1] if '.' in table_name else table_name
            target_table = request.target_tables[i] if request.target_tables else clean_table_name
            task = _process_single_database_table_chunked(connector, table_name, target_table)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                clean_table_name = request.table_names[i].split('.')[-1] if '.' in request.table_names[i] else request.table_names[i]
                processed_results.append({
                    "table_name": request.table_names[i],
                    "target_table": request.target_tables[i] if request.target_tables else clean_table_name,
                    "status": "error",
                    "error": str(result),
                    "records_processed": 0
                })
            else:
                processed_results.append(result)
        
        return processed_results
        
    except Exception as e:
        logger.error(f"Error in parallel database extraction: {e}")
        raise

async def _extract_multiple_from_database_sql(source_config: Dict[str, Any], request: DatabaseSQLExtractionRequest) -> List[Dict[str, Any]]:
    """Extract data using multiple SQL queries in parallel."""
    try:
        from ..acquire.connectors import DatabaseConnector
        
        connector = DatabaseConnector(source_config)
        connector.connect()
        
        # Process SQL queries in parallel
        tasks = []
        for i, sql_query in enumerate(request.sql_queries):
            target_table = request.target_tables[i] if request.target_tables else _extract_table_name_from_sql(sql_query)
            task = _process_single_database_sql(connector, sql_query, target_table)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "sql_query": request.sql_queries[i],
                    "target_table": request.target_tables[i] if request.target_tables else _extract_table_name_from_sql(request.sql_queries[i]),
                    "status": "error",
                    "error": str(result),
                    "records_processed": 0
                })
            else:
                processed_results.append(result)
        
        return processed_results
        
    except Exception as e:
        logger.error(f"Error in parallel SQL extraction: {e}")
        raise

async def _process_single_database_table_chunked(connector, table_name: str, target_table: str) -> Dict[str, Any]:
    """Process a single database table with chunking for large tables."""
    try:
        # Get total count first
        count_query = f"SELECT COUNT(*) as total_count FROM {table_name}"
        count_result = connector.execute_query(count_query)
        total_count = count_result[0]['total_count'] if count_result else 0
        
        logger.info(f"Processing table {table_name} with {total_count} records")
        
        if total_count == 0:
            return {
                "table_name": table_name,
                "target_table": target_table,
                "status": "success",
                "records_processed": 0,
                "message": "Table is empty"
            }
        
        # Determine chunk size based on total count
        if total_count > 1000000:  # > 1M records
            chunk_size = 200000  # Much larger chunks for big tables
        elif total_count > 100000:  # > 100K records
            chunk_size = 100000  # Larger chunks
        else:
            chunk_size = 50000  # Still larger than before
        
        # Get column schema from first chunk to create table
        logger.info(f"Getting column schema from first chunk")
        first_chunk_query = f"SELECT * FROM {table_name} LIMIT 1"
        first_chunk_data = connector.execute_query(first_chunk_query)
        
        if not first_chunk_data:
            raise HTTPException(status_code=400, detail=f"No data found in table {table_name}")
        
        # Get column names from first row
        column_names = list(first_chunk_data[0].keys())
        logger.info(f"Table columns: {column_names}")
        
        # Initialize ClickHouse table BEFORE chunking
        from ..core.database import DatabaseManager
        db_manager = DatabaseManager()
        
        # Drop existing table if it exists
        drop_table_sql = f"DROP TABLE IF EXISTS bronze.{target_table}"
        db_manager.execute_query(drop_table_sql)
        
        # Create table with proper column names
        create_table_sql = f"""
        CREATE TABLE bronze.{target_table} (
            {', '.join([f'`{col}` String' for col in column_names])},
            `create_date` String
        ) ENGINE = MergeTree()
        ORDER BY create_date
        """
        
        db_manager.execute_query(create_table_sql)
        logger.info(f"Created table bronze.{target_table} with columns: {column_names}")
        
        # Process in chunks - stream each chunk directly to ClickHouse
        total_loaded = 0
        offset = 0
        chunk_number = 1
        total_chunks = math.ceil(total_count / chunk_size)
        
        logger.info(f"Starting chunked processing: {total_chunks} chunks of {chunk_size} records each")
        
        while offset < total_count:
            chunk_query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
            logger.info(f"Chunk {chunk_number}/{total_chunks}: Extracting records {offset+1} to {min(offset+chunk_size, total_count)}")
            
            chunk_data = connector.execute_query(chunk_query)
            
            if not chunk_data:
                logger.warning(f"Chunk {chunk_number}: No data returned, stopping processing")
                break
            
            logger.info(f"Chunk {chunk_number}: Extracted {len(chunk_data)} records from database")
            
            # Convert chunk to string streams
            string_data = _convert_database_results_to_strings(chunk_data)
            logger.info(f"Chunk {chunk_number}: Converted to string streams")
            
            # Load chunk directly to ClickHouse
            records_loaded = await _load_data_to_clickhouse(string_data, target_table, column_names)
            total_loaded += records_loaded
            
            logger.info(f"Chunk {chunk_number}/{total_chunks}: Loaded {records_loaded} records to ClickHouse (Total: {total_loaded}/{total_count})")
            
            offset += chunk_size
            chunk_number += 1
            
            # If we got fewer records than chunk_size, we're done
            if len(chunk_data) < chunk_size:
                logger.info(f"Chunk {chunk_number-1}: Got {len(chunk_data)} records (< {chunk_size}), processing complete")
                break
        
        # Load to ClickHouse
        records_loaded = total_loaded
        
        return {
            "table_name": table_name,
            "target_table": target_table,
            "status": "success",
            "records_processed": total_count,
            "records_loaded": records_loaded,
            "chunk_size": chunk_size,
            "total_chunks": math.ceil(total_count / chunk_size)
        }
        
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        return {
            "table_name": table_name,
            "target_table": target_table,
            "status": "error",
            "error": str(e),
            "records_processed": 0
        }

async def _process_single_database_sql(connector, sql_query: str, target_table: str) -> Dict[str, Any]:
    """Process a single SQL query."""
    try:
        # Execute SQL query
        data = connector.execute_query(sql_query)
        
        if not data:
            return {
                "sql_query": sql_query,
                "target_table": target_table,
                "status": "success",
                "records_processed": 0,
                "message": "Query returned no data"
            }
        
        # Get column names from first row
        column_names = list(data[0].keys())
        logger.info(f"SQL query columns: {column_names}")
        
        # Initialize ClickHouse table BEFORE loading data
        from ..core.database import DatabaseManager
        db_manager = DatabaseManager()
        
        # Drop existing table if it exists
        drop_table_sql = f"DROP TABLE IF EXISTS bronze.{target_table}"
        db_manager.execute_query(drop_table_sql)
        
        # Create table with proper column names
        create_table_sql = f"""
        CREATE TABLE bronze.{target_table} (
            {', '.join([f'`{col}` String' for col in column_names])},
            `create_date` String
        ) ENGINE = MergeTree()
        ORDER BY create_date
        """
        
        db_manager.execute_query(create_table_sql)
        logger.info(f"Created table bronze.{target_table} with columns: {column_names}")
        
        # Convert to string streams
        string_data = _convert_database_results_to_strings(data)
        
        # Load to ClickHouse
        records_loaded = await _load_data_to_clickhouse(string_data, target_table, column_names)
        
        return {
            "sql_query": sql_query,
            "target_table": target_table,
            "status": "success",
            "records_processed": len(data),
            "records_loaded": records_loaded
        }
        
    except Exception as e:
        logger.error(f"Error processing SQL query: {e}")
        return {
            "sql_query": sql_query,
            "target_table": target_table,
            "status": "error",
            "error": str(e),
            "records_processed": 0
        }

def _convert_database_results_to_strings(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert database results to string streams with character cleaning."""
    try:
        if not data:
            return []
        
        string_data = []
        for record in data:
            string_record = {}
            for key, value in record.items():
                # Convert to string and clean
                string_value = str(value) if value is not None else ""
                string_value = _clean_text_content(string_value)
                string_record[key] = string_value
            string_data.append(string_record)
        
        return string_data
        
    except Exception as e:
        logger.error(f"Error converting database results to strings: {e}")
        return []

async def _load_data_to_clickhouse(data: List[Dict[str, Any]], target_table: str, column_names: List[str] = None) -> int:
    """Load data to ClickHouse bronze layer."""
    try:
        from ..core.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        if not data:
            return 0
        
        # Use provided column names or generate generic ones
        if column_names:
            columns = column_names
        else:
            columns = list(data[0].keys())
        
        # Insert data in batches for efficiency
        records_inserted = 0
        create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        batch_size = 1000
        total_batches = math.ceil(len(data) / batch_size)
        
        logger.info(f"Loading {len(data)} records to ClickHouse in {total_batches} batches of {batch_size}")
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            
            logger.info(f"Processing batch {batch_number}/{total_batches}: {len(batch)} records")
            
            # Prepare batch insert
            values_list = []
            for row in batch:
                values = []
                for col_name in columns:
                    value = str(row[col_name]) if row[col_name] is not None else ''
                    escaped_value = value.replace("'", "''")
                    values.append(f"'{escaped_value}'")
                values.append(f"'{create_date}'")
                values_list.append(f"({', '.join(values)})")
            
            # Execute batch insert
            insert_sql = f"""
            INSERT INTO bronze.{target_table} ({', '.join([f'`{col}`' for col in columns])}, `create_date`)
            VALUES {', '.join(values_list)}
            """
            
            db_manager.execute_query(insert_sql)
            records_inserted += len(batch)
            
            logger.info(f"Batch {batch_number}/{total_batches}: Inserted {len(batch)} records (Total: {records_inserted})")
        
        return records_inserted
        
    except Exception as e:
        logger.error(f"Error loading data to ClickHouse: {e}")
        raise
