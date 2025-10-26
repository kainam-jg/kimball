"""
KIMBALL Acquire Phase API Routes

This module provides FastAPI routes for the Acquire phase:
- Data source connections
- Data extraction
- Bronze layer loading
- Source validation
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, List, Any, Optional, Union
import json
from datetime import datetime

from ..acquire.connectors import DatabaseConnector, APIConnector
from ..acquire.loaders import BronzeLoader
from ..acquire.source_manager import DataSourceManager
from ..acquire.bucket_processor import S3DataProcessor, ClickHouseStreamLoader
from ..core.logger import Logger
from ..core.enhanced_logger import logger as enhanced_logger
from ..core.config import Config

# Initialize router
acquire_router = APIRouter(prefix="/api/v1/acquire", tags=["acquire"])
logger = Logger("acquire_api")

# Initialize data source manager
source_manager = DataSourceManager()

# Pydantic Models
class DataSourceConfigRequest(BaseModel):
    """Request model for creating a new data source."""
    name: str
    type: str
    config: Dict[str, Any]

class DataSourceUpdateRequest(BaseModel):
    """Request model for updating an existing data source."""
    name: Optional[str] = None
    type: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

class S3ObjectRequest(BaseModel):
    """Request model for S3 object discovery."""
    bucket: str
    prefix: Optional[str] = ""
    max_keys: Optional[int] = None
    search_subdirectories: bool = True

class DatabaseTableRequest(BaseModel):
    """Request model for database table discovery."""
    schema: Optional[str] = None
    table_pattern: Optional[str] = None

class SQLQueryRequest(BaseModel):
    """Request model for SQL query execution."""
    query: str
    limit: Optional[int] = None

class DataExtractionRequest(BaseModel):
    """Request model for data extraction."""
    source_id: str
    extraction_type: str
    extraction_config: Dict[str, List[str]]
    target_table: Optional[str] = None

def validate_data_loading(table_name: str, expected_count: int) -> Dict[str, Any]:
    """Validate that data was actually written to the table."""
    try:
        import time
        from kimball.core.database import DatabaseManager
        
        # Add a small delay to ensure data is committed
        time.sleep(0.5)
        
        db_manager = DatabaseManager()
        
        # Get actual count from table
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = db_manager.execute_query(count_query)
        
        if result and len(result) > 0:
            # Handle both tuple and dict results
            if isinstance(result[0], dict):
                actual_count = result[0].get('count', 0)
            else:
                actual_count = result[0][0] if len(result[0]) > 0 else 0
            
            validation_passed = actual_count == expected_count
            
            return {
                "count": actual_count,
                "expected_count": expected_count,
                "validation_passed": validation_passed,
                "status": "success" if validation_passed else "warning",
                "message": f"Data validation {'passed' if validation_passed else 'failed'}: {actual_count}/{expected_count} records"
            }
        else:
            return {
                "count": 0,
                "expected_count": expected_count,
                "validation_passed": False,
                "status": "error",
                "message": "Could not retrieve count from table"
            }
            
    except Exception as e:
        logger.error(f"Error validating data loading: {str(e)}")
        return {
            "count": 0,
            "expected_count": expected_count,
            "validation_passed": False,
            "status": "error",
            "message": f"Validation error: {str(e)}"
        }

# Data Source Management Endpoints
@acquire_router.post("/datasources")
async def create_data_source(request: DataSourceConfigRequest):
    """Create a new data source configuration."""
    try:
        logger.log_api_call("/acquire/datasources", "POST")
        
        config_manager = Config()
        config = config_manager.get_config()
        
        # Add new data source
        if "data_sources" not in config:
            config["data_sources"] = {}
        
        config["data_sources"][request.name] = {
            "type": request.type,
            "enabled": True,
            **request.config
        }
        
        # Save configuration
        config_manager.save_config(config)
        
        return {
            "status": "success",
            "message": f"Data source '{request.name}' created successfully",
            "source_id": request.name,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error creating data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
        
        return {
            "status": "success",
            "source_id": source_id,
            "configuration": data_sources[source_id],
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.put("/datasources/{source_id}")
async def update_data_source(source_id: str, request: DataSourceUpdateRequest):
    """Update an existing data source configuration."""
    try:
        logger.log_api_call(f"/acquire/datasources/{source_id}", "PUT")
        
        config_manager = Config()
        config = config_manager.get_config()
        
        if "data_sources" not in config or source_id not in config["data_sources"]:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        # Update fields if provided
        if request.name:
            config["data_sources"][request.name] = config["data_sources"].pop(source_id)
            source_id = request.name
        
        if request.type:
            config["data_sources"][source_id]["type"] = request.type
        
        if request.config:
            config["data_sources"][source_id].update(request.config)
        
        # Save configuration
        config_manager.save_config(config)
        
        return {
            "status": "success",
            "message": f"Data source '{source_id}' updated successfully",
            "source_id": source_id,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.delete("/datasources/{source_id}")
async def delete_data_source(source_id: str):
    """Delete a data source configuration."""
    try:
        logger.log_api_call(f"/acquire/datasources/{source_id}", "DELETE")
        
        config_manager = Config()
        config = config_manager.get_config()
        
        if "data_sources" not in config or source_id not in config["data_sources"]:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        # Remove data source
        del config["data_sources"][source_id]
        
        # Save configuration
        config_manager.save_config(config)
        
        return {
            "status": "success",
            "message": f"Data source '{source_id}' deleted successfully",
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/test/{source_id}")
async def test_data_source_connection(source_id: str):
    """Test connection to a data source."""
    try:
        logger.log_api_call(f"/acquire/test/{source_id}", "GET")
        
        # Test connection using source manager
        success = source_manager.test_source_connection(source_id)
        
        if success:
            return {
                "status": "success",
                "message": f"Connection to '{source_id}' successful",
                "source_id": source_id,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "error",
                "message": f"Connection to '{source_id}' failed",
                "source_id": source_id,
                "timestamp": datetime.now().isoformat()
            }
        
    except Exception as e:
        logger.error(f"Error testing connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/status")
async def get_acquire_status():
    """Get overall status of the Acquire phase."""
    try:
        logger.log_api_call("/acquire/status", "GET")
        
        # Get available sources
        sources = source_manager.get_available_sources()
        
        return {
            "status": "success",
            "phase": "acquire",
            "available_sources": len(sources),
            "sources": sources,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting acquire status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Discovery Endpoints
@acquire_router.post("/discover/s3-objects")
async def discover_s3_objects(request: S3ObjectRequest):
    """Discover objects in S3 bucket using new bucket processor."""
    try:
        logger.info(f"Discovering S3 objects in bucket: {request.bucket}")
        
        config_manager = Config()
        config = config_manager.get_config()
        s3_sources = {k: v for k, v in config.get("data_sources", {}).items() if v.get("type") == "s3"}
        
        if not s3_sources:
            raise HTTPException(status_code=404, detail="No S3 data sources configured")
        
        source_name = list(s3_sources.keys())[0]
        source_config = s3_sources[source_name]
        
        import boto3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=source_config.get("access_key"),
            aws_secret_access_key=source_config.get("secret_key"),
            region_name=source_config.get("region", "us-east-1")
        )
        
        kwargs = {
            'Bucket': source_config.get("bucket"),
            'Prefix': request.prefix
        }
        
        if request.max_keys:
            kwargs['MaxKeys'] = request.max_keys
        
        if not request.search_subdirectories:
            kwargs['Delimiter'] = '/'
        
        response = s3_client.list_objects_v2(**kwargs)
        
        objects = []
        if 'Contents' in response:
            for obj in response['Contents']:
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'etag': obj['ETag']
                })
        
        return {
            "status": "success",
            "bucket": request.bucket,
            "prefix": request.prefix,
            "max_keys": request.max_keys,
            "search_subdirectories": request.search_subdirectories,
            "objects": objects,
            "count": len(objects),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error discovering S3 objects: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/discover/database-tables/{source_id}")
async def discover_database_tables(source_id: str, request: DatabaseTableRequest):
    """Discover tables in a database source."""
    try:
        logger.log_api_call(f"/acquire/discover/database-tables/{source_id}", "POST")
        
        # Get source configuration
        source_config = source_manager.get_source_config(source_id)
        if not source_config:
            raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
        
        # Create database connector
        connector = DatabaseConnector(source_config)
        
        if not connector.connect():
            raise HTTPException(status_code=400, detail="Failed to connect to database")
        
        # Get tables
        tables = connector.get_tables(
            schema=request.schema,
            table_pattern=request.table_pattern
        )
        
        return {
            "status": "success",
            "source_id": source_id,
            "schema": request.schema,
            "tables": tables,
            "count": len(tables),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error discovering database tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/execute/sql-query/{source_id}")
async def execute_sql_query(source_id: str, request: SQLQueryRequest):
    """Execute a custom SQL query against a database source."""
    try:
        logger.log_api_call(f"/acquire/execute/sql-query/{source_id}", "POST")
        
        # Get source configuration
        source_config = source_manager.get_source_config(source_id)
        if not source_config:
            raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
        
        # Create database connector
        connector = DatabaseConnector(source_config)
        
        if not connector.connect():
            raise HTTPException(status_code=400, detail="Failed to connect to database")
        
        # Execute query
        results = connector.execute_query(
            query=request.query,
            limit=request.limit
        )
        
        return {
            "status": "success",
            "source_id": source_id,
            "query": request.query,
            "results": results,
            "count": len(results),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data Extraction Endpoint
@acquire_router.post("/extract-data")
async def extract_data_to_bronze(request: DataExtractionRequest):
    """Extract data from a source and load it to the bronze layer."""
    try:
        enhanced_logger.log_api_call("/acquire/extract-data", "POST", request.dict())
        enhanced_logger.info(f"Starting data extraction: {request.source_id}, type: {request.extraction_type}")
        
        # Get source configuration
        source_config = source_manager.get_source_config(request.source_id)
        if not source_config:
            enhanced_logger.error(f"Source {request.source_id} not found in configuration")
            raise HTTPException(status_code=404, detail=f"Source {request.source_id} not found")
        
        enhanced_logger.info(f"Found source config for {request.source_id}")
        
        # Initialize data list
        data = []
        
        # Extract data based on type
        if request.extraction_type == "s3_objects":
            # Extract from S3 objects using new bucket processor
            object_keys = request.extraction_config.get("object_keys", [])
            if not object_keys:
                raise HTTPException(status_code=400, detail="No S3 objects specified")
            
            # Create S3 processor
            processor = S3DataProcessor(source_config)
            if not processor.connect():
                raise HTTPException(status_code=400, detail="Failed to connect to S3")
            
            # Use filename as table name if not specified
            if (not request.target_table or (request.target_table and request.target_table.strip() == "")) and object_keys:
                # Extract filename from the first object key
                first_key = object_keys[0]
                filename = first_key.split('/')[-1]  # Get filename from path (no path)
                table_name = filename.split('.')[0]  # Remove extension
                
                # Add timestamp to table name
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                request.target_table = f"bronze_{table_name}_{timestamp}"
            
            # Stream data directly to ClickHouse
            loader = ClickHouseStreamLoader()
            load_result = loader.stream_data_to_table(
                table_name=request.target_table,
                data_stream=processor.extract_data(object_keys),
                batch_size=1000
            )
            
            if "error" in load_result:
                raise HTTPException(status_code=500, detail=load_result["error"])
            
            # Get actual count for validation
            validation_result = validate_data_loading(request.target_table, load_result["total_records"])
            
            return {
                "status": "success",
                "source_id": request.source_id,
                "extraction_type": request.extraction_type,
                "target_table": request.target_table,
                "records_extracted": load_result["total_records"],
                "records_loaded": load_result["total_records"],
                "records_in_table": validation_result.get("count", 0),
                "data_validation": validation_result,
                "load_result": load_result,
                "timestamp": datetime.now().isoformat()
            }
            
        elif request.extraction_type == "sql_query":
            # Extract using SQL query
            query = request.extraction_config.get("query", "")
            if not query:
                raise HTTPException(status_code=400, detail="No SQL query specified")
            
            # Create database connector
            connector = DatabaseConnector(source_config)
            if not connector.connect():
                raise HTTPException(status_code=400, detail="Failed to connect to database")
            
            data = connector.execute_query(query) or []
            
            # Load data to bronze layer using old method for now
            loader = BronzeLoader()
            load_result = loader.load_data(
                data=data,
                target_table=request.target_table,
                source_id=request.source_id
            )
            
            # Validate data was actually written to the table
            validation_result = validate_data_loading(request.target_table, len(data))
            
            return {
                "status": "success",
                "source_id": request.source_id,
                "extraction_type": request.extraction_type,
                "target_table": request.target_table,
                "records_extracted": len(data),
                "records_loaded": load_result.get("record_count", 0),
                "records_in_table": validation_result.get("count", 0),
                "data_validation": validation_result,
                "load_result": load_result,
                "timestamp": datetime.now().isoformat()
            }
            
        elif request.extraction_type == "table_data":
            # Extract entire table
            table_name = request.extraction_config.get("table_name", "")
            if not table_name:
                raise HTTPException(status_code=400, detail="No table name specified")
            
            # Create database connector
            connector = DatabaseConnector(source_config)
            if not connector.connect():
                raise HTTPException(status_code=400, detail="Failed to connect to database")
            
            data = connector.extract_table(table_name) or []
            
            # Load data to bronze layer using old method for now
            loader = BronzeLoader()
            load_result = loader.load_data(
                data=data,
                target_table=request.target_table,
                source_id=request.source_id
            )
            
            # Validate data was actually written to the table
            validation_result = validate_data_loading(request.target_table, len(data))
            
            return {
                "status": "success",
                "source_id": request.source_id,
                "extraction_type": request.extraction_type,
                "target_table": request.target_table,
                "records_extracted": len(data),
                "records_loaded": load_result.get("record_count", 0),
                "records_in_table": validation_result.get("count", 0),
                "data_validation": validation_result,
                "load_result": load_result,
                "timestamp": datetime.now().isoformat()
            }
            
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported extraction type: {request.extraction_type}")
        
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
