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
from typing import Dict, List, Any, Optional
import json
from datetime import datetime

from ..acquire.connectors import DatabaseConnector, APIConnector, StorageConnector
from ..acquire.extractors import DataExtractor
from ..acquire.loaders import BronzeLoader
from ..acquire.source_manager import DataSourceManager
from ..core.logger import Logger

# Initialize router
acquire_router = APIRouter(prefix="/api/v1/acquire", tags=["acquire"])
logger = Logger("acquire_api")

# Initialize data source manager
source_manager = DataSourceManager()

@acquire_router.get("/status")
async def get_acquire_status():
    """Get overall Acquire phase status."""
    try:
        sources = source_manager.get_available_sources()
        return {
            "phase": "acquire",
            "status": "active",
            "sources_count": len(sources),
            "sources": [{"name": s["name"], "type": s["type"], "enabled": True} for s in sources],
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting acquire status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Pydantic models for request/response
class ConnectionRequest(BaseModel):
    """Request model for connection operations."""
    source_type: str  # database, api, storage
    connection_config: Dict[str, Any]
    test_connection: bool = True

class ExtractionRequest(BaseModel):
    """Request model for data extraction."""
    source_id: str
    extraction_config: Dict[str, Any]
    batch_size: int = 1000

class LoadRequest(BaseModel):
    """Request model for bronze layer loading."""
    extraction_id: str
    target_table: str
    load_config: Dict[str, Any]

class DataSourceConfigRequest(BaseModel):
    """Request model for data source configuration."""
    source_id: str
    source_type: str  # postgres, s3, api, etc.
    config: Dict[str, Any]
    enabled: bool = True
    description: Optional[str] = None

class DataSourceUpdateRequest(BaseModel):
    """Request model for updating data source configuration."""
    config: Dict[str, Any]
    enabled: Optional[bool] = None
    description: Optional[str] = None

@acquire_router.post("/connect")
async def create_connection(request: ConnectionRequest):
    """
    Create a connection to a data source.
    
    This endpoint establishes connections to various data sources
    including databases, APIs, and storage containers.
    """
    try:
        logger.log_api_call("/acquire/connect", "POST")
        
        # Initialize appropriate connector
        if request.source_type == "database":
            connector = DatabaseConnector(request.connection_config)
        elif request.source_type == "api":
            connector = APIConnector(request.connection_config)
        elif request.source_type == "storage":
            connector = StorageConnector(request.connection_config)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported source type: {request.source_type}")
        
        # Test connection if requested
        if request.test_connection:
            if not connector.test_connection():
                raise HTTPException(status_code=400, detail="Connection test failed")
        
        # Store connection (in production, use proper connection management)
        connection_id = f"conn_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return {
            "status": "success",
            "connection_id": connection_id,
            "source_type": request.source_type,
            "message": "Connection established successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/extract")
async def extract_data(request: ExtractionRequest):
    """
    Extract data from a connected source.
    
    This endpoint performs data extraction from various sources
    with configurable extraction parameters.
    """
    try:
        logger.log_api_call("/acquire/extract", "POST")
        
        # Initialize extractor
        extractor = DataExtractor()
        
        # Perform extraction
        extraction_result = extractor.extract_data(
            source_id=request.source_id,
            config=request.extraction_config,
            batch_size=request.batch_size
        )
        
        return {
            "status": "success",
            "extraction_id": extraction_result["extraction_id"],
            "records_extracted": extraction_result["record_count"],
            "message": "Data extraction completed successfully"
        }
        
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/load")
async def load_to_bronze(request: LoadRequest):
    """
    Load extracted data into bronze layer.
    
    This endpoint loads extracted data into ClickHouse bronze layer
    with minimal transformation.
    """
    try:
        logger.log_api_call("/acquire/load", "POST")
        
        # Initialize loader
        loader = BronzeLoader()
        
        # Load data to bronze layer
        load_result = loader.load_data(
            extraction_id=request.extraction_id,
            target_table=request.target_table,
            config=request.load_config
        )
        
        return {
            "status": "success",
            "load_id": load_result["load_id"],
            "records_loaded": load_result["record_count"],
            "target_table": request.target_table,
            "message": "Data loaded to bronze layer successfully"
        }
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/sources")
async def list_sources():
    """
    List available data sources.
    """
    try:
        logger.log_api_call("/acquire/sources", "GET")
        
        # Return available source types and their configurations
        sources = {
            "database": {
                "supported_types": ["clickhouse", "postgresql", "mysql", "oracle", "sqlserver"],
                "required_config": ["host", "port", "database", "username", "password"]
            },
            "api": {
                "supported_types": ["rest", "graphql", "soap"],
                "required_config": ["base_url", "authentication"]
            },
            "storage": {
                "supported_types": ["s3", "azure_blob", "gcs", "local"],
                "required_config": ["bucket", "credentials"]
            }
        }
        
        return {"sources": sources}
        
    except Exception as e:
        logger.error(f"Error listing sources: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/status/{operation_id}")
async def get_operation_status(operation_id: str):
    """
    Get status of an acquire operation.
    """
    try:
        logger.log_api_call(f"/acquire/status/{operation_id}", "GET")
        
        # In production, this would check actual operation status
        # For now, return a mock status
        return {
            "operation_id": operation_id,
            "status": "completed",
            "progress": 100,
            "message": "Operation completed successfully"
        }
        
    except Exception as e:
        logger.error(f"Error getting operation status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Additional comprehensive API endpoints

@acquire_router.get("/status")
async def get_acquire_status():
    """Get the status of the Acquire phase."""
    return {
        "status": "active",
        "message": "Acquire phase is ready",
        "available_sources": source_manager.get_all_enabled_sources(),
        "timestamp": datetime.now().isoformat()
    }

@acquire_router.post("/connect/{source_id}")
async def connect_source(source_id: str):
    """Connect to a specific data source."""
    try:
        success = source_manager.connect_source(source_id)
        if success:
            return {
                "status": "success",
                "message": f"Connected to {source_id}",
                "source_id": source_id,
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail=f"Failed to connect to {source_id}")
    except Exception as e:
        logger.error(f"Connection error for {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/disconnect/{source_id}")
async def disconnect_source(source_id: str):
    """Disconnect from a specific data source."""
    try:
        source_manager.disconnect_source(source_id)
        return {
            "status": "success",
            "message": f"Disconnected from {source_id}",
            "source_id": source_id,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Disconnection error for {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/sources")
async def list_configured_sources():
    """List all configured data sources."""
    try:
        sources = source_manager.get_available_sources()
        source_configs = {}
        for source_id in sources:
            source_configs[source_id] = source_manager.get_source_config(source_id)
        
        return {
            "sources": source_configs,
            "count": len(sources),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error listing sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/sources/{source_id}/schema")
async def get_source_schema(source_id: str):
    """Get schema information for a specific data source."""
    try:
        schema = source_manager.get_source_schema(source_id)
        return {
            "source_id": source_id,
            "schema": schema,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting schema for {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/extract/{source_id}")
async def extract_data_from_source(source_id: str, request: ExtractionRequest):
    """Extract data from a specific source."""
    try:
        result = source_manager.extract_data(source_id, **request.extraction_config)
        return {
            "source_id": source_id,
            "extraction_result": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error extracting data from {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/load/{source_id}")
async def load_to_bronze_layer(source_id: str, request: LoadRequest):
    """Load extracted data to bronze layer."""
    try:
        success = source_manager.load_to_bronze(request.data, source_id, request.target_table)
        if success:
            return {
                "status": "success",
                "message": f"Data loaded to bronze.{request.target_table}",
                "source_id": source_id,
                "table_name": request.target_table,
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail="Failed to load data to bronze layer")
    except Exception as e:
        logger.error(f"Error loading data to bronze: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/test/{source_id}")
async def test_connection(source_id: str):
    """Test connection to a specific data source."""
    try:
        success = source_manager.test_source_connection(source_id)
        return {
            "source_id": source_id,
            "connection_test": "success" if success else "failed",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error testing connection to {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/sources/{source_id}/files")
async def list_source_files(source_id: str, prefix: Optional[str] = None):
    """List files in a storage source (S3, etc.)."""
    try:
        source_config = source_manager.get_source_config(source_id)
        if not source_config:
            raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
        
        if source_config.get("type") in ["s3", "azure", "gcs"]:
            # For storage sources, list files
            schema = source_manager.get_source_schema(source_id)
            files = schema.get("files", [])
            
            # Filter by prefix if provided
            if prefix:
                files = [f for f in files if f.startswith(prefix)]
            
            return {
                "source_id": source_id,
                "files": files,
                "count": len(files),
                "prefix": prefix,
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail=f"Source {source_id} is not a storage source")
    except Exception as e:
        logger.error(f"Error listing files for {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/extract-file/{source_id}")
async def extract_file(source_id: str, file_path: str):
    """Extract data from a specific file in a storage source."""
    try:
        result = source_manager.extract_data(source_id, file_pattern=file_path)
        return {
            "source_id": source_id,
            "file_path": file_path,
            "extraction_result": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error extracting file {file_path} from {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.post("/full-pipeline/{source_id}")
async def run_full_pipeline(source_id: str, table_name: str, extraction_config: Optional[Dict[str, Any]] = None):
    """Run the full acquire pipeline: connect, extract, and load to bronze."""
    try:
        # Step 1: Connect to source
        if not source_manager.connect_source(source_id):
            raise HTTPException(status_code=400, detail=f"Failed to connect to {source_id}")
        
        # Step 2: Extract data
        extraction_config = extraction_config or {}
        result = source_manager.extract_data(source_id, **extraction_config)
        
        if not result or result.get("record_count", 0) == 0:
            raise HTTPException(status_code=400, detail="No data extracted")
        
        # Step 3: Load to bronze
        success = source_manager.load_to_bronze(result, source_id, table_name)
        
        if success:
            return {
                "status": "success",
                "message": f"Full pipeline completed for {source_id}",
                "source_id": source_id,
                "table_name": table_name,
                "records_processed": result.get("record_count", 0),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=400, detail="Failed to load data to bronze layer")
    except Exception as e:
        logger.error(f"Error in full pipeline for {source_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Data Source Configuration Management Endpoints

@acquire_router.post("/datasources")
async def create_data_source(request: DataSourceConfigRequest):
    """Create a new data source configuration."""
    try:
        from ..core.config import Config
        
        # Load current config
        config_manager = Config()
        config = config_manager.get_config()
        
        # Add new data source
        if "data_sources" not in config:
            config["data_sources"] = {}
        
        config["data_sources"][request.source_id] = {
            "type": request.source_type,
            "enabled": request.enabled,
            "description": request.description,
            **request.config
        }
        
        # Save updated config
        config_manager.save_config(config)
        
        return {
            "status": "success",
            "message": f"Data source '{request.source_id}' created successfully",
            "source_id": request.source_id,
            "source_type": request.source_type,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error creating data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.put("/datasources/{source_id}")
async def update_data_source(source_id: str, request: DataSourceUpdateRequest):
    """Update an existing data source configuration."""
    try:
        from ..core.config import Config
        
        # Load current config
        config_manager = Config()
        config = config_manager.get_config()
        
        if "data_sources" not in config or source_id not in config["data_sources"]:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        # Update configuration
        if request.config:
            config["data_sources"][source_id].update(request.config)
        
        if request.enabled is not None:
            config["data_sources"][source_id]["enabled"] = request.enabled
        
        if request.description is not None:
            config["data_sources"][source_id]["description"] = request.description
        
        # Save updated config
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
        from ..core.config import Config
        
        # Load current config
        config_manager = Config()
        config = config_manager.get_config()
        
        if "data_sources" not in config or source_id not in config["data_sources"]:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        # Remove data source
        del config["data_sources"][source_id]
        
        # Save updated config
        config_manager.save_config(config)
        
        return {
            "status": "success",
            "message": f"Data source '{source_id}' deleted successfully",
            "source_id": source_id,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/datasources/{source_id}")
async def get_data_source(source_id: str):
    """Get a specific data source configuration."""
    try:
        from ..core.config import Config
        
        # Load current config
        config_manager = Config()
        config = config_manager.get_config()
        
        if "data_sources" not in config or source_id not in config["data_sources"]:
            raise HTTPException(status_code=404, detail=f"Data source '{source_id}' not found")
        
        return {
            "source_id": source_id,
            "configuration": config["data_sources"][source_id],
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@acquire_router.get("/datasources")
async def list_data_sources():
    """List all data source configurations."""
    try:
        from ..core.config import Config
        
        # Load current config
        config_manager = Config()
        config = config_manager.get_config()
        
        data_sources = config.get("data_sources", {})
        
        return {
            "data_sources": data_sources,
            "count": len(data_sources),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error listing data sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))
