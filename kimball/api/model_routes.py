#!/usr/bin/env python3
"""
Model Phase API Routes
Handles ELT transformation orchestration, UDF management, and metadata-driven transformations.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from ..core.database import DatabaseManager

# Configure logging
logger = logging.getLogger(__name__)

# Create router
model_router = APIRouter(prefix="/api/v1/model", tags=["Model"])

# Pydantic models
class UDFRequest(BaseModel):
    """Request model for creating/updating UDFs."""
    transformation_stage: str
    udf_name: str
    udf_number: int
    udf_logic: str
    dependencies: List[str] = []
    execution_frequency: str = "daily"

class UDFExecutionRequest(BaseModel):
    """Request model for executing UDFs."""
    udf_name: str
    dry_run: bool = False

class TransformationStatus(BaseModel):
    """Response model for transformation status."""
    stage: str
    udf_name: str
    status: str
    records_processed: int
    execution_time: float
    error_message: Optional[str] = None

# API Endpoints

@model_router.get("/status")
async def get_model_status():
    """Get overall model phase status."""
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
        
        # Get silver table count
        silver_tables_query = "SHOW TABLES FROM silver"
        silver_tables = db_manager.execute_query(silver_tables_query)
        
        return {
            "status": "active",
            "phase": "Model",
            "description": "ELT transformation orchestration with ClickHouse UDFs",
            "udf_counts_by_stage": {row["transformation_stage"]: row["udf_count"] for row in udf_counts} if udf_counts else {},
            "silver_tables": [table["name"] for table in silver_tables] if silver_tables else [],
            "total_udfs": sum(row["udf_count"] for row in udf_counts) if udf_counts else 0,
            "total_silver_tables": len(silver_tables) if silver_tables else 0
        }
        
    except Exception as e:
        logger.error(f"Error getting model status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.get("/udfs")
async def get_udfs(
    stage: Optional[str] = Query(None, description="Filter by transformation stage"),
    limit: int = Query(100, description="Maximum number of UDFs to return")
):
    """Get all UDFs or filter by stage."""
    try:
        db_manager = DatabaseManager()
        
        where_clause = ""
        if stage:
            where_clause = f"WHERE transformation_stage = '{stage}'"
        
        query = f"""
        SELECT 
            transformation_stage,
            udf_name,
            udf_number,
            udf_logic,
            dependencies,
            execution_frequency,
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
            "filtered_by_stage": stage
        }
        
    except Exception as e:
        logger.error(f"Error getting UDFs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/udfs")
async def create_udf(request: UDFRequest):
    """Create a new UDF."""
    try:
        db_manager = DatabaseManager()
        
        # Generate new version for upsert
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Insert UDF metadata
        insert_sql = f"""
        INSERT INTO metadata.transformation1 (
            transformation_stage,
            udf_name,
            udf_number,
            udf_logic,
            dependencies,
            execution_frequency,
            version
        ) VALUES (
            '{request.transformation_stage}',
            '{request.udf_name}',
            {request.udf_number},
            '{request.udf_logic.replace("'", "''")}',
            {request.dependencies},
            '{request.execution_frequency}',
            {new_version}
        )
        """
        
        result = db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"UDF '{request.udf_name}' created successfully",
            "udf_name": request.udf_name,
            "transformation_stage": request.transformation_stage,
            "version": new_version
        }
        
    except Exception as e:
        logger.error(f"Error creating UDF: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/udfs/execute")
async def execute_udf(request: UDFExecutionRequest):
    """Execute a UDF."""
    try:
        db_manager = DatabaseManager()
        
        # Get UDF logic
        udf_query = f"""
        SELECT udf_logic, transformation_stage
        FROM metadata.transformation1
        WHERE udf_name = '{request.udf_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        udf_results = db_manager.execute_query(udf_query)
        if not udf_results:
            raise HTTPException(status_code=404, detail=f"UDF '{request.udf_name}' not found")
        
        udf_logic = udf_results[0]["udf_logic"]
        stage = udf_results[0]["transformation_stage"]
        
        if request.dry_run:
            return {
                "status": "dry_run",
                "message": f"Dry run for UDF '{request.udf_name}'",
                "udf_logic": udf_logic,
                "transformation_stage": stage
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
            "execution_time": execution_time,
            "result": result
        }
        
    except Exception as e:
        logger.error(f"Error executing UDF: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/transformations/stage1")
async def execute_stage1_transformations():
    """Execute all Stage 1 transformations (Bronze to Silver)."""
    try:
        db_manager = DatabaseManager()
        
        # Get all Stage 1 UDFs
        stage1_query = """
        SELECT udf_name, udf_logic
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
                    "status": "success",
                    "records_processed": records_processed,
                    "execution_time": execution_time
                })
                
                total_records += records_processed
                
            except Exception as e:
                results.append({
                    "udf_name": udf_name,
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

@model_router.get("/silver/tables")
async def get_silver_tables():
    """Get all silver layer tables with their structure."""
    try:
        db_manager = DatabaseManager()
        
        # Get all silver tables
        tables_query = "SHOW TABLES FROM silver"
        tables = db_manager.execute_query(tables_query)
        
        if not tables:
            return {
                "status": "success",
                "silver_tables": [],
                "total_count": 0
            }
        
        # Get structure for each table
        table_details = []
        for table in tables:
            table_name = table["name"]
            
            # Get table structure
            desc_query = f"DESCRIBE silver.{table_name}"
            columns = db_manager.execute_query(desc_query)
            
            # Get record count
            count_query = f"SELECT COUNT(*) as count FROM silver.{table_name}"
            count_result = db_manager.execute_query(count_query)
            record_count = count_result[0]["count"] if count_result else 0
            
            table_details.append({
                "table_name": table_name,
                "columns": columns if columns else [],
                "record_count": record_count
            })
        
        return {
            "status": "success",
            "silver_tables": table_details,
            "total_count": len(table_details)
        }
        
    except Exception as e:
        logger.error(f"Error getting silver tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.get("/silver/tables/{table_name}/sample")
async def get_silver_table_sample(
    table_name: str,
    limit: int = Query(10, description="Number of sample records to return")
):
    """Get sample data from a silver table."""
    try:
        db_manager = DatabaseManager()
        
        # Get sample data
        sample_query = f"""
        SELECT *
        FROM silver.{table_name}
        LIMIT {limit}
        """
        
        results = db_manager.execute_query(sample_query)
        
        return {
            "status": "success",
            "table_name": table_name,
            "sample_data": results if results else [],
            "sample_size": len(results) if results else 0,
            "requested_limit": limit
        }
        
    except Exception as e:
        logger.error(f"Error getting sample data from {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))