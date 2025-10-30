"""
Demo API Routes for KIMBALL Platform

This module provides demo/utility endpoints for database management operations
like dropping and truncating tables across different schemas.

Note: These endpoints use direct ClickHouse connections and do not interfere
with the existing DatabaseManager or connection classes.
"""

import logging
from typing import List, Dict, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import clickhouse_connect

# Configure logging
logger = logging.getLogger(__name__)

# Create router
demo_router = APIRouter(prefix="/api/v1/demo", tags=["Demo"])

class SchemaOperationRequest(BaseModel):
    """Request model for schema operations"""
    schemas: List[str]
    operation: str  # "drop" or "truncate"

class SchemaOperationResponse(BaseModel):
    """Response model for schema operations"""
    status: str
    message: str
    schemas_processed: List[str]
    tables_affected: Dict[str, List[str]]
    total_tables: int

def get_clickhouse_client():
    """Get ClickHouse client connection using direct clickhouse_connect"""
    try:
        # Use the same connection details as our main config
        client = clickhouse_connect.get_client(
            host='3.150.3.160',
            port=8123,
            username='default',
            password='Kainam2023'
        )
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise HTTPException(status_code=500, detail=f"ClickHouse connection failed: {str(e)}")

def get_tables_in_schema(client, schema: str) -> List[str]:
    """Get all table names in a schema"""
    try:
        query = f"SELECT name FROM system.tables WHERE database = '{schema}'"
        result = client.query(query)
        return [row[0] for row in result.result_rows]
    except Exception as e:
        logger.error(f"Failed to get tables for schema {schema}: {e}")
        raise Exception(f"Failed to get tables for schema {schema}: {str(e)}")

def drop_tables_in_schema(client, schema: str) -> List[str]:
    """Drop all tables in a schema"""
    dropped_tables = []
    try:
        tables = get_tables_in_schema(client, schema)
        for table in tables:
            try:
                drop_query = f"DROP TABLE IF EXISTS {schema}.{table}"
                client.command(drop_query)
                dropped_tables.append(table)
                logger.info(f"Dropped table {schema}.{table}")
            except Exception as e:
                logger.error(f"Failed to drop table {schema}.{table}: {e}")
        return dropped_tables
    except Exception as e:
        logger.error(f"Failed to drop tables in schema {schema}: {e}")
        return []

def truncate_tables_in_schema(client, schema: str) -> List[str]:
    """Truncate all tables in a schema"""
    truncated_tables = []
    try:
        tables = get_tables_in_schema(client, schema)
        for table in tables:
            try:
                truncate_query = f"TRUNCATE TABLE {schema}.{table}"
                client.command(truncate_query)
                truncated_tables.append(table)
                logger.info(f"Truncated table {schema}.{table}")
            except Exception as e:
                logger.error(f"Failed to truncate table {schema}.{table}: {e}")
        return truncated_tables
    except Exception as e:
        logger.error(f"Failed to truncate tables in schema {schema}: {e}")
        return []

@demo_router.get("/status")
async def get_demo_status():
    """Get demo API status"""
    return {
        "status": "active",
        "phase": "Demo",
        "description": "Demo and utility endpoints for database management",
        "available_operations": ["drop_tables", "truncate_tables", "list_tables", "cleanup_all"],
        "supported_schemas": ["bronze", "silver", "gold", "metadata"],
        "cleanup_all_description": "Drops all tables in bronze, silver, and gold schemas, truncates metadata tables"
    }

@demo_router.post("/drop-tables")
async def drop_tables(request: SchemaOperationRequest):
    """
    Drop all tables in specified schemas
    
    This endpoint drops all tables in the provided schemas.
    Use with caution as this operation is irreversible.
    """
    try:
        client = get_clickhouse_client()
        schemas_processed = []
        tables_affected = {}
        total_tables = 0
        
        for schema in request.schemas:
            logger.info(f"Dropping all tables in schema: {schema}")
            dropped_tables = drop_tables_in_schema(client, schema)
            if dropped_tables:
                schemas_processed.append(schema)
                tables_affected[schema] = dropped_tables
                total_tables += len(dropped_tables)
        
        return SchemaOperationResponse(
            status="success",
            message=f"Successfully dropped {total_tables} tables across {len(schemas_processed)} schemas",
            schemas_processed=schemas_processed,
            tables_affected=tables_affected,
            total_tables=total_tables
        )
        
    except Exception as e:
        logger.error(f"Error in drop_tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@demo_router.post("/truncate-tables")
async def truncate_tables(request: SchemaOperationRequest):
    """
    Truncate all tables in specified schemas
    
    This endpoint truncates (empties) all tables in the provided schemas.
    The table structure remains intact but all data is removed.
    """
    try:
        client = get_clickhouse_client()
        schemas_processed = []
        tables_affected = {}
        total_tables = 0
        
        for schema in request.schemas:
            logger.info(f"Truncating all tables in schema: {schema}")
            truncated_tables = truncate_tables_in_schema(client, schema)
            if truncated_tables:
                schemas_processed.append(schema)
                tables_affected[schema] = truncated_tables
                total_tables += len(truncated_tables)
        
        return SchemaOperationResponse(
            status="success",
            message=f"Successfully truncated {total_tables} tables across {len(schemas_processed)} schemas",
            schemas_processed=schemas_processed,
            tables_affected=tables_affected,
            total_tables=total_tables
        )
        
    except Exception as e:
        logger.error(f"Error in truncate_tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@demo_router.get("/list-tables/{schema}")
async def list_tables(schema: str):
    """
    List all tables in a specific schema
    
    Returns a list of all table names in the specified schema.
    """
    try:
        client = get_clickhouse_client()
        tables = get_tables_in_schema(client, schema)
        
        return {
            "status": "success",
            "schema": schema,
            "tables": tables,
            "table_count": len(tables)
        }
        
    except Exception as e:
        logger.error(f"Error listing tables for schema {schema}: {e}")
        raise HTTPException(status_code=500, detail=f"Error listing tables for schema {schema}: {str(e)}")

@demo_router.post("/cleanup-all")
async def cleanup_all():
    """
    Complete cleanup: Drop bronze, silver, and gold tables, truncate metadata tables
    
    This is equivalent to running the original script:
    - Drop all tables in 'bronze' schema
    - Drop all tables in 'silver' schema
    - Drop all tables in 'gold' schema
    - Truncate all tables in 'metadata' schema
    """
    try:
        client = get_clickhouse_client()
        schemas_processed = []
        tables_affected = {}
        total_tables = 0
        
        # Drop bronze tables
        logger.info("Dropping all tables in bronze schema")
        bronze_tables = drop_tables_in_schema(client, 'bronze')
        if bronze_tables:
            schemas_processed.append('bronze')
            tables_affected['bronze'] = bronze_tables
            total_tables += len(bronze_tables)
        
        # Drop silver tables
        logger.info("Dropping all tables in silver schema")
        silver_tables = drop_tables_in_schema(client, 'silver')
        if silver_tables:
            schemas_processed.append('silver')
            tables_affected['silver'] = silver_tables
            total_tables += len(silver_tables)
        
        # Drop gold tables
        logger.info("Dropping all tables in gold schema")
        gold_tables = drop_tables_in_schema(client, 'gold')
        if gold_tables:
            schemas_processed.append('gold')
            tables_affected['gold'] = gold_tables
            total_tables += len(gold_tables)
        
        # Truncate metadata tables
        logger.info("Truncating all tables in metadata schema")
        metadata_tables = truncate_tables_in_schema(client, 'metadata')
        if metadata_tables:
            schemas_processed.append('metadata')
            tables_affected['metadata'] = metadata_tables
            total_tables += len(metadata_tables)
        
        return SchemaOperationResponse(
            status="success",
            message=f"Complete cleanup finished: {total_tables} tables processed across {len(schemas_processed)} schemas",
            schemas_processed=schemas_processed,
            tables_affected=tables_affected,
            total_tables=total_tables
        )
        
    except Exception as e:
        logger.error(f"Error in cleanup_all: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@demo_router.get("/schemas")
async def list_all_schemas():
    """
    List all available schemas (databases) in ClickHouse
    """
    try:
        client = get_clickhouse_client()
        query = "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')"
        result = client.query(query)
        schemas = [row[0] for row in result.result_rows]
        
        return {
            "status": "success",
            "schemas": schemas,
            "schema_count": len(schemas)
        }
        
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        raise HTTPException(status_code=500, detail=str(e))
