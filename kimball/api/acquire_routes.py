"""
KIMBALL Acquire Phase API Routes - MINIMAL VERSION FOR TESTING

This module provides FastAPI routes for the Acquire phase.
Currently only has the status endpoint for systematic testing.
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel

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
    schema: Optional[str] = None
    table_pattern: Optional[str] = None

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
            if request.schema:
                schema_filter = f"AND table_schema = '{request.schema}'"
            
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
                "schema_filter": request.schema,
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

# # Data Extraction Endpoint
# @acquire_router.post("/extract-data")
# async def extract_data_to_bronze(request: DataExtractionRequest):
#     """Extract data from a source and load it to the bronze layer."""
#     pass
