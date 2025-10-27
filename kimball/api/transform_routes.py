#!/usr/bin/env python3
"""
Transform Phase API Routes

This module handles UDF (User-Defined Function) management, creation, updating, 
and execution for ELT transformations in the KIMBALL pipeline.

Key Features:
- UDF lifecycle management (create, read, update, delete)
- Schema-based UDF organization
- ClickHouse UDF function creation and execution
- Transform orchestration by stage
- Real-time monitoring and metrics

Architecture:
- UDF metadata stored in metadata.transformation1 table
- Multi-stage processing (Bronze → Silver → Gold)
- Metadata-driven transformations with dependencies
- Version control and upsert functionality
- Gold layer dimensional model generation
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from ..core.database import DatabaseManager

# Configure logging
logger = logging.getLogger(__name__)

# Create router for Transformation Phase API endpoints
transform_router = APIRouter(prefix="/api/v1/transform", tags=["Transform"])

# Pydantic models for request/response validation
class UDFRequest(BaseModel):
    """Request model for creating/updating UDFs."""
    transformation_stage: str
    udf_name: str
    udf_number: int
    udf_logic: str
    udf_schema_name: str = "default"
    dependencies: List[str] = []
    execution_frequency: str = "daily"
    source_schema: Optional[str] = None
    source_table: Optional[str] = None
    target_schema: Optional[str] = None
    target_table: Optional[str] = None

class UDFUpdateRequest(BaseModel):
    """Request model for updating existing UDFs."""
    udf_logic: Optional[str] = None
    udf_schema_name: Optional[str] = None
    dependencies: Optional[List[str]] = None
    execution_frequency: Optional[str] = None

class UDFExecutionRequest(BaseModel):
    """Request model for executing UDFs."""
    udf_name: str
    dry_run: bool = False
    create_if_not_exists: bool = True

class UDFCreationRequest(BaseModel):
    """Request model for creating UDFs in ClickHouse."""
    udf_name: str
    transformation_stage: str
    create_if_not_exists: bool = True

class TransformationStatus(BaseModel):
    """Response model for transformation status."""
    stage: str
    udf_name: str
    status: str
    records_processed: int
    execution_time: float
    error_message: Optional[str] = None

# API Endpoints

@transform_router.get("/status")
async def get_transformation_status():
    """
    Get overall transformation phase status.
    
    Returns:
        - Current phase status and description
        - UDF counts by stage and schema
        - Total UDFs and schemas
        - Active transformation metrics
    """
    try:
        db_manager = DatabaseManager()
        
        # Get UDF count by stage
        udf_count_query = """
        SELECT 
            transformation_stage,
            COUNT(*) as udf_count
        FROM metadata.transformation1
        GROUP BY transformation_stage
        ORDER BY transformation_stage
        """
        
        udf_counts = db_manager.execute_query(udf_count_query)
        
        # Get schema count
        schema_count_query = """
        SELECT 
            udf_schema_name,
            COUNT(*) as udf_count
        FROM metadata.transformation1
        GROUP BY udf_schema_name
        ORDER BY udf_schema_name
        """
        
        schema_counts = db_manager.execute_query(schema_count_query)
        
        return {
            "status": "active",
            "phase": "Transform",
            "description": "ELT transformation orchestration with ClickHouse UDFs",
            "udf_counts_by_stage": {row["transformation_stage"]: row["udf_count"] for row in udf_counts} if udf_counts else {},
            "udf_counts_by_schema": {row["udf_schema_name"]: row["udf_count"] for row in schema_counts} if schema_counts else {},
            "total_udfs": sum(row["udf_count"] for row in udf_counts) if udf_counts else 0,
            "total_schemas": len(schema_counts) if schema_counts else 0
        }
        
    except Exception as e:
        logger.error(f"Error getting transformation status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.get("/udfs")
async def get_udfs(
    stage: Optional[str] = Query(None, description="Filter by transformation stage"),
    schema: Optional[str] = Query(None, description="Filter by UDF schema name"),
    limit: int = Query(100, description="Maximum number of UDFs to return")
):
    """
    Get all UDFs or filter by stage/schema.
    
    Parameters:
        - stage: Filter UDFs by transformation stage (e.g., 'stage1', 'stage2')
        - schema: Filter UDFs by schema name (e.g., 'default', 'custom')
        - limit: Maximum number of UDFs to return (default: 100)
    
    Returns:
        - List of UDFs with metadata
        - Total count and filter information
    """
    try:
        db_manager = DatabaseManager()
        
        where_conditions = []
        if stage:
            where_conditions.append(f"transformation_stage = '{stage}'")
        if schema:
            where_conditions.append(f"udf_schema_name = '{schema}'")
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query = f"""
        SELECT 
            transformation_stage,
            udf_name,
            udf_number,
            udf_logic,
            udf_schema_name,
            dependencies,
            execution_frequency,
            source_schema,
            source_table,
            target_schema,
            target_table,
            created_at,
            updated_at,
            version
        FROM metadata.transformation1
        {where_clause}
        ORDER BY transformation_stage, udf_number
        LIMIT {limit}
        """
        
        results = db_manager.execute_query(query)
        
        return {
            "status": "success",
            "udfs": results if results else [],
            "total_count": len(results) if results else 0,
            "filtered_by_stage": stage,
            "filtered_by_schema": schema
        }
        
    except Exception as e:
        logger.error(f"Error getting UDFs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.get("/udfs/{udf_name}")
async def get_udf_by_name(udf_name: str):
    """Get a specific UDF by name."""
    try:
        db_manager = DatabaseManager()
        
        query = f"""
        SELECT 
            transformation_stage,
            udf_name,
            udf_number,
            udf_logic,
            udf_schema_name,
            dependencies,
            execution_frequency,
            source_schema,
            source_table,
            target_schema,
            target_table,
            created_at,
            updated_at,
            version
        FROM metadata.transformation1
        WHERE udf_name = '{udf_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        results = db_manager.execute_query(query)
        
        if not results:
            raise HTTPException(status_code=404, detail=f"UDF '{udf_name}' not found")
        
        return {
            "status": "success",
            "udf": results[0]
        }
        
    except Exception as e:
        logger.error(f"Error getting UDF {udf_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.post("/udfs")
async def create_udf(request: UDFRequest):
    """
    Create a new UDF with metadata.
    
    This endpoint creates a new UDF entry in the metadata.transformation1 table.
    The UDF logic is stored as SQL text and can be executed later.
    
    Parameters:
        - transformation_stage: Stage identifier (e.g., 'stage1', 'stage2')
        - udf_name: Unique name for the UDF
        - udf_number: Execution order number
        - udf_logic: SQL transformation logic
        - udf_schema_name: Schema where UDF is stored (default: 'default')
        - dependencies: List of dependent UDFs
        - execution_frequency: How often to run (e.g., 'daily', 'hourly')
    
    Returns:
        - Success message with UDF details
        - New version number for upsert functionality
    """
    try:
        db_manager = DatabaseManager()
        
        # Generate new version for upsert
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Insert UDF metadata
        escaped_logic = request.udf_logic.replace("'", "''")
        insert_sql = f"""
        INSERT INTO metadata.transformation1 (
            transformation_stage,
            udf_name,
            udf_number,
            udf_logic,
            udf_schema_name,
            dependencies,
            execution_frequency,
            source_schema,
            source_table,
            target_schema,
            target_table,
            version
        ) VALUES (
            '{request.transformation_stage}',
            '{request.udf_name}',
            {request.udf_number},
            '{escaped_logic}',
            '{request.udf_schema_name}',
            {request.dependencies},
            '{request.execution_frequency}',
            '{request.source_schema or ''}',
            '{request.source_table or ''}',
            '{request.target_schema or ''}',
            '{request.target_table or ''}',
            {new_version}
        )
        """
        
        result = db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"UDF '{request.udf_name}' created successfully",
            "udf_name": request.udf_name,
            "transformation_stage": request.transformation_stage,
            "udf_schema_name": request.udf_schema_name,
            "version": new_version
        }
        
    except Exception as e:
        logger.error(f"Error creating UDF: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.put("/udfs/{udf_name}")
async def update_udf(udf_name: str, request: UDFUpdateRequest):
    """Update an existing UDF."""
    try:
        db_manager = DatabaseManager()
        
        # Check if UDF exists
        check_query = f"""
        SELECT transformation_stage, udf_schema_name, version
        FROM metadata.transformation1
        WHERE udf_name = '{udf_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        existing = db_manager.execute_query(check_query)
        if not existing:
            raise HTTPException(status_code=404, detail=f"UDF '{udf_name}' not found")
        
        existing_record = existing[0]
        current_version = existing_record["version"]
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Build update fields
        update_fields = []
        if request.udf_logic is not None:
            escaped_logic = request.udf_logic.replace("'", "''")
            update_fields.append(f"udf_logic = '{escaped_logic}'")
        if request.udf_schema_name is not None:
            update_fields.append(f"udf_schema_name = '{request.udf_schema_name}'")
        if request.dependencies is not None:
            update_fields.append(f"dependencies = {request.dependencies}")
        if request.execution_frequency is not None:
            update_fields.append(f"execution_frequency = '{request.execution_frequency}'")
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields provided for update")
        
        # Insert updated record (upsert)
        insert_sql = f"""
        INSERT INTO metadata.transformation1 (
            transformation_stage,
            udf_name,
            udf_number,
            udf_logic,
            udf_schema_name,
            dependencies,
            execution_frequency,
            version
        )
        SELECT 
            transformation_stage,
            udf_name,
            udf_number,
            {update_fields[0].split(' = ')[1] if 'udf_logic' in update_fields[0] else 'udf_logic'},
            {update_fields[1].split(' = ')[1] if 'udf_schema_name' in update_fields[1] else 'udf_schema_name'},
            {update_fields[2].split(' = ')[1] if 'dependencies' in update_fields[2] else 'dependencies'},
            {update_fields[3].split(' = ')[1] if 'execution_frequency' in update_fields[3] else 'execution_frequency'},
            {new_version}
        FROM metadata.transformation1
        WHERE udf_name = '{udf_name}' AND version = {current_version}
        """
        
        result = db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"UDF '{udf_name}' updated successfully",
            "udf_name": udf_name,
            "updated_fields": [field.split(' = ')[0] for field in update_fields],
            "new_version": new_version
        }
        
    except Exception as e:
        logger.error(f"Error updating UDF {udf_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.post("/udfs/create")
async def create_udf_in_clickhouse(request: UDFCreationRequest):
    """
    Create the actual UDF function in ClickHouse.
    
    This endpoint creates the actual UDF function in ClickHouse using the stored SQL logic.
    It retrieves the UDF metadata and creates a ClickHouse function that can be executed.
    
    Parameters:
        - udf_name: Name of the UDF to create
        - transformation_stage: Stage identifier
        - create_if_not_exists: Whether to create if function doesn't exist
    
    Returns:
        - Success message with UDF creation details
        - SQL statement used for creation
    """
    try:
        db_manager = DatabaseManager()
        
        # Get UDF metadata
        udf_query = f"""
        SELECT 
            udf_logic,
            udf_schema_name,
            transformation_stage
        FROM metadata.transformation1
        WHERE udf_name = '{request.udf_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        udf_results = db_manager.execute_query(udf_query)
        if not udf_results:
            raise HTTPException(status_code=404, detail=f"UDF '{request.udf_name}' not found in metadata")
        
        udf_data = udf_results[0]
        udf_logic = udf_data["udf_logic"]
        schema_name = udf_data["udf_schema_name"]
        stage = udf_data["transformation_stage"]
        
        # Create UDF function in ClickHouse
        create_udf_sql = f"""
        CREATE OR REPLACE FUNCTION {schema_name}.{request.udf_name}()
        AS $$
        {udf_logic}
        $$
        """
        
        if request.create_if_not_exists:
            create_udf_sql = create_udf_sql.replace("CREATE OR REPLACE", "CREATE")
        
        result = db_manager.execute_query(create_udf_sql)
        
        return {
            "status": "success",
            "message": f"UDF '{request.udf_name}' created in ClickHouse schema '{schema_name}'",
            "udf_name": request.udf_name,
            "schema_name": schema_name,
            "transformation_stage": stage,
            "sql": create_udf_sql
        }
        
    except Exception as e:
        logger.error(f"Error creating UDF in ClickHouse: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.post("/udfs/execute")
async def execute_udf(request: UDFExecutionRequest):
    """Execute a UDF."""
    try:
        db_manager = DatabaseManager()
        
        # Get UDF logic
        udf_query = f"""
        SELECT udf_logic, transformation_stage, udf_schema_name
        FROM metadata.transformation1
        WHERE udf_name = '{request.udf_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        udf_results = db_manager.execute_query(udf_query)
        if not udf_results:
            if request.create_if_not_exists:
                # Try to create the UDF first
                create_request = UDFCreationRequest(
                    udf_name=request.udf_name,
                    transformation_stage="unknown",
                    create_if_not_exists=True
                )
                await create_udf_in_clickhouse(create_request)
                # Re-query after creation
                udf_results = db_manager.execute_query(udf_query)
                if not udf_results:
                    raise HTTPException(status_code=404, detail=f"UDF '{request.udf_name}' not found")
            else:
                raise HTTPException(status_code=404, detail=f"UDF '{request.udf_name}' not found")
        
        udf_logic = udf_results[0]["udf_logic"]
        stage = udf_results[0]["transformation_stage"]
        schema = udf_results[0]["udf_schema_name"]
        
        if request.dry_run:
            return {
                "status": "dry_run",
                "message": f"Dry run for UDF '{request.udf_name}'",
                "udf_logic": udf_logic,
                "transformation_stage": stage,
                "schema_name": schema
            }
        
        # Execute UDF
        start_time = datetime.now()
        result = db_manager.execute_query(udf_logic)
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return {
            "status": "success",
            "message": f"UDF '{request.udf_name}' executed successfully",
            "udf_name": request.udf_name,
            "transformation_stage": stage,
            "schema_name": schema,
            "execution_time": execution_time,
            "result": result
        }
        
    except Exception as e:
        logger.error(f"Error executing UDF: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.post("/transformations/stage1")
async def execute_stage1_transformations():
    """
    Execute all Stage 1 transformations (Bronze to Silver).
    
    This endpoint executes all UDFs in the 'stage1' transformation stage.
    It processes transformations in order and provides detailed results.
    
    Returns:
        - Execution results for each UDF
        - Total records processed
        - Execution times and error information
    """
    try:
        db_manager = DatabaseManager()
        
        # Get all Stage 1 UDFs
        stage1_query = """
        SELECT udf_name, udf_logic, udf_schema_name
        FROM metadata.transformation1
        WHERE transformation_stage = 'stage1'
        ORDER BY udf_number
        """
        
        udfs = db_manager.execute_query(stage1_query)
        if not udfs:
            return {
                "status": "success",
                "message": "No Stage 1 UDFs found",
                "transformations_executed": 0
            }
        
        results = []
        total_records = 0
        
        for udf in udfs:
            udf_name = udf["udf_name"]
            udf_logic = udf["udf_logic"]
            schema_name = udf["udf_schema_name"]
            
            try:
                start_time = datetime.now()
                result = db_manager.execute_query(udf_logic)
                execution_time = (datetime.now() - start_time).total_seconds()
                
                # Try to get record count from result
                records_processed = 0
                if result and isinstance(result, list) and len(result) > 0:
                    if isinstance(result[0], dict) and "written_rows" in result[0]:
                        records_processed = result[0]["written_rows"]
                
                results.append({
                    "udf_name": udf_name,
                    "schema_name": schema_name,
                    "status": "success",
                    "records_processed": records_processed,
                    "execution_time": execution_time
                })
                
                total_records += records_processed
                
            except Exception as e:
                results.append({
                    "udf_name": udf_name,
                    "schema_name": schema_name,
                    "status": "error",
                    "error_message": str(e),
                    "records_processed": 0,
                    "execution_time": 0
                })
        
        return {
            "status": "success",
            "message": f"Stage 1 transformations completed",
            "transformations_executed": len(results),
            "total_records_processed": total_records,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error executing Stage 1 transformations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.post("/transformations/stage2")
async def execute_stage2_transformations():
    """
    Execute all Stage 2 transformations (Silver Stage1 to Silver Stage2 CDC).
    
    This endpoint executes all UDFs in the 'stage2' transformation stage.
    It implements CDC (Change Data Capture) logic using create_date for change detection.
    
    CDC Process:
    1. Creates Stage 2 tables with ReplacingMergeTree engine
    2. Inserts/updates records from Stage 1 to Stage 2
    3. Uses create_date as version for deduplication
    4. Optimizes tables to merge duplicates
    
    Returns:
        - Execution results for each UDF
        - Total records processed
        - Execution times and error information
    """
    try:
        db_manager = DatabaseManager()
        
        # Get all Stage 2 UDFs
        stage2_query = """
        SELECT udf_name, udf_logic, udf_schema_name
        FROM metadata.transformation1
        WHERE transformation_stage = 'stage2'
        ORDER BY udf_number
        """
        
        udfs = db_manager.execute_query(stage2_query)
        if not udfs:
            return {
                "status": "success",
                "message": "No Stage 2 UDFs found",
                "transformations_executed": 0
            }
        
        results = []
        total_records = 0
        
        for udf in udfs:
            udf_name = udf["udf_name"]
            udf_logic = udf["udf_logic"]
            schema_name = udf["udf_schema_name"]
            
            try:
                start_time = datetime.now()
                
                # Split multi-statement SQL and execute each statement separately
                statements = [stmt.strip() for stmt in udf_logic.split(';') if stmt.strip()]
                
                records_processed = 0
                for statement in statements:
                    if statement:  # Skip empty statements
                        result = db_manager.execute_query(statement)
                        # Try to get record count from result
                        if result and isinstance(result, list) and len(result) > 0:
                            if isinstance(result[0], dict) and "written_rows" in result[0]:
                                records_processed += result[0]["written_rows"]
                
                execution_time = (datetime.now() - start_time).total_seconds()
                
                results.append({
                    "udf_name": udf_name,
                    "schema_name": schema_name,
                    "status": "success",
                    "records_processed": records_processed,
                    "execution_time": execution_time
                })
                
                total_records += records_processed
                
            except Exception as e:
                results.append({
                    "udf_name": udf_name,
                    "schema_name": schema_name,
                    "status": "error",
                    "error_message": str(e),
                    "records_processed": 0,
                    "execution_time": 0
                })
        
        return {
            "status": "success",
            "message": f"Stage 2 CDC transformations completed",
            "transformations_executed": len(results),
            "total_records_processed": total_records,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error executing Stage 2 transformations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.get("/schemas")
async def get_udf_schemas():
    """Get all UDF schemas and their UDF counts."""
    try:
        db_manager = DatabaseManager()
        
        query = """
        SELECT 
            udf_schema_name,
            COUNT(*) as udf_count,
            COUNT(DISTINCT transformation_stage) as stage_count
        FROM metadata.transformation1
        GROUP BY udf_schema_name
        ORDER BY udf_schema_name
        """
        
        results = db_manager.execute_query(query)
        
        return {
            "status": "success",
            "schemas": results if results else [],
            "total_schemas": len(results) if results else 0
        }
        
    except Exception as e:
        logger.error(f"Error getting UDF schemas: {e}")
        raise HTTPException(status_code=500, detail=str(e))
