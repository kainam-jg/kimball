"""
Transform Phase API Routes

This module provides FastAPI endpoints for the Transform phase of the KIMBALL API.
It handles multi-statement transformations with sequential execution support.

The Transform phase focuses on:
- Multi-statement transformation management
- Sequential SQL execution (solving ClickHouse limitations)
- Stage orchestration (stage1, stage2, stage3, stage4)
- Parallel execution of different transformations
- SQL validation and error handling
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from ..core.database import DatabaseManager

# Configure logging
logger = logging.getLogger(__name__)

# Create router for Transform Phase API endpoints
transform_router = APIRouter(prefix="/api/v1/transform", tags=["Transform"])

# Pydantic models for request/response validation
class StatementRequest(BaseModel):
    """
    Request model for creating/updating individual SQL statements.
    
    This model defines the structure for individual SQL statement creation
    within a multi-statement transformation.
    
    Attributes:
        transformation_id: Unique identifier for the transformation
        execution_sequence: Order of execution (1, 2, 3, etc.)
        sql_statement: Single SQL statement to execute
        statement_type: Type of statement (DROP, CREATE, INSERT, UPDATE, etc.)
        description: Human-readable description of the statement
    """
    transformation_id: str
    execution_sequence: int
    sql_statement: str
    statement_type: str
    description: Optional[str] = None

class TransformationRequest(BaseModel):
    """
    Request model for creating complete transformations with multiple statements.
    
    This model defines the structure for creating a complete transformation
    with multiple SQL statements that execute in sequence.
    
    Attributes:
        transformation_stage: Stage identifier (stage1, stage2, stage3, stage4)
        transformation_id: Unique identifier for the transformation
        statements: List of StatementRequest objects
        source_schema: Source schema for transformations (optional)
        source_table: Source table for transformations (optional)
        target_schema: Target schema for transformations (optional)
        target_table: Target table for transformations (optional)
        execution_frequency: How often to run (e.g., 'daily', 'hourly')
    """
    transformation_stage: str
    transformation_id: str
    statements: List[StatementRequest]
    source_schema: Optional[str] = None
    source_table: Optional[str] = None
    target_schema: Optional[str] = None
    target_table: Optional[str] = None
    execution_frequency: str = "daily"

class SQLValidationRequest(BaseModel):
    """Request model for SQL validation."""
    sql: str
    stage: str

# Helper Functions

def validate_sql(sql: str, stage: str) -> Dict[str, Any]:
    """
    Validate SQL syntax and structure for ClickHouse compatibility.
    
    This function performs basic SQL validation to catch common syntax errors
    and provide suggestions for ClickHouse compatibility.
    
    Args:
        sql: SQL string to validate
        stage: Transformation stage for context-specific validation
    
    Returns:
        Dict containing validation results, errors, warnings, and suggestions
    """
    errors = []
    warnings = []
    suggestions = []
    
    # Basic syntax checks
    sql_upper = sql.upper().strip()
    
    # Check for empty SQL
    if not sql.strip():
        errors.append("SQL cannot be empty")
        return {
            "is_valid": False,
            "errors": errors,
            "warnings": warnings,
            "suggestions": suggestions
        }
    
    # Check for semicolon termination
    if not sql.strip().endswith(';'):
        warnings.append("SQL should end with semicolon")
        suggestions.append("Add semicolon at the end of the statement")
    
    # Check for common ClickHouse-specific issues
    if "CREATE TABLE AS SELECT" in sql_upper:
        errors.append("ClickHouse does not support CREATE TABLE AS SELECT")
        suggestions.append("Use separate CREATE TABLE and INSERT statements")
    
    # Check for unsupported functions
    unsupported_functions = ["CURRENT_TIMESTAMP", "NOW()", "SYSDATE"]
    for func in unsupported_functions:
        if func in sql_upper:
            warnings.append(f"Function '{func}' may not work as expected in ClickHouse")
            suggestions.append(f"Consider using ClickHouse date/time functions instead of '{func}'")
    
    # Stage-specific validation
    if stage == "stage1":
        if "CREATE TABLE" in sql_upper and "ENGINE" not in sql_upper:
            warnings.append("Stage1 CREATE TABLE should specify ENGINE")
            suggestions.append("Add ENGINE = MergeTree() or appropriate ClickHouse engine")
    
    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "suggestions": suggestions
    }

# API Endpoints

@transform_router.post("/sql/validate")
async def validate_sql_endpoint(request: SQLValidationRequest):
    """
    Validate SQL syntax and structure for ClickHouse.
    
    This endpoint validates SQL before creating transformations to catch syntax errors
    and provide suggestions for ClickHouse compatibility.
    
    Parameters:
        - sql: SQL string to validate
        - stage: Transformation stage (stage1, stage2, stage3, stage4)
    
    Returns:
        Dict containing validation results, errors, warnings, and suggestions
    """
    try:
        validation_result = validate_sql(request.sql, request.stage)
        return {
            "status": "success",
            "validation": validation_result
        }
        
    except Exception as e:
        logger.error(f"Error validating SQL: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Multi-Statement Transformation APIs

@transform_router.post("/transformations")
async def create_transformation(request: TransformationRequest):
    """
    Create a new multi-statement transformation.
    
    This endpoint creates a complete transformation with multiple SQL statements
    that will be executed in sequence by the TransformEngine.
    
    Parameters:
        - transformation_stage: Stage identifier (stage1, stage2, stage3, stage4)
        - transformation_id: Unique identifier for the transformation
        - statements: List of StatementRequest objects with sequence numbers
        - source_schema: Source schema for transformations (optional)
        - source_table: Source table for transformations (optional)
        - target_schema: Target schema for transformations (optional)
        - target_table: Target table for transformations (optional)
        - execution_frequency: How often to run (e.g., 'daily', 'hourly')
    
    Returns:
        Dict containing creation results and statement details
    """
    try:
        db_manager = DatabaseManager()
        
        # Validate SQL statements
        validation_results = []
        for statement in request.statements:
            validation = validate_sql(statement.sql_statement, request.transformation_stage)
            validation_results.append({
                "execution_sequence": statement.execution_sequence,
                "statement_type": statement.statement_type,
                "validation": validation
            })
            
            if not validation["is_valid"]:
                raise HTTPException(
                    status_code=400, 
                    detail=f"SQL validation failed for statement {statement.execution_sequence}: {'; '.join(validation['errors'])}"
                )
        
        # Get next transformation_id (same for all statements in this transformation)
        get_max_id_sql = "SELECT COALESCE(MAX(transformation_id), 0) as max_id FROM metadata.transformation1"
        max_id_result = db_manager.execute_query_dict(get_max_id_sql)
        next_transformation_id = (max_id_result[0]['max_id'] if max_id_result else 0) + 1
        
        # Create statements in the database
        created_statements = []
        for statement in request.statements:
            # Generate new version for upsert
            new_version = int(datetime.now().timestamp() * 1000000)
            
            # Escape SQL statement
            escaped_sql = statement.sql_statement.replace("'", "''")
            escaped_desc = (statement.description or f"{statement.statement_type} statement").replace("'", "''")
            
            insert_sql = f"""
            INSERT INTO metadata.transformation1 (
                transformation_stage,
                transformation_id,
                transformation_name,
                transformation_schema_name,
                dependencies,
                execution_frequency,
                source_schema,
                source_table,
                target_schema,
                target_table,
                execution_sequence,
                sql_statement,
                statement_type,
                version
            ) VALUES (
                '{request.transformation_stage}',
                {next_transformation_id},
                '{request.transformation_id}',
                'metadata',
                '[]',
                '{request.execution_frequency}',
                '{request.source_schema or ''}',
                '{request.source_table or ''}',
                '{request.target_schema or ''}',
                '{request.target_table or ''}',
                {statement.execution_sequence},
                '{escaped_sql}',
                '{statement.statement_type}',
                {new_version}
            )
            """
            
            db_manager.execute_query_dict(insert_sql)
            
            created_statements.append({
                "execution_sequence": statement.execution_sequence,
                "statement_type": statement.statement_type,
                "description": statement.description,
                "version": new_version
            })
        
        return {
            "status": "success",
            "message": f"Transformation '{request.transformation_id}' created successfully",
            "transformation_id": request.transformation_id,
            "transformation_stage": request.transformation_stage,
            "statements_created": len(created_statements),
            "statements": created_statements,
            "validation_results": validation_results
        }
        
    except Exception as e:
        logger.error(f"Error creating transformation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.put("/transformations/{transformation_name}")
async def update_transformation(transformation_name: str, request: TransformationRequest):
    """
    Update an existing transformation with upsert logic.
    
    This endpoint updates an existing transformation by:
    1. Upserting all provided statements (based on transformation_id + execution_sequence)
    2. Keeping existing statements that are not in the request
    3. Adding new execution sequences as needed
    
    Parameters:
        - transformation_name: Name of the transformation to update
        - request: TransformationRequest with updated statements
    """
    try:
        db_manager = DatabaseManager()
        
        # First, check if transformation exists and get its transformation_id
        check_sql = f"""
        SELECT DISTINCT transformation_id 
        FROM metadata.transformation1 
        WHERE transformation_name = '{transformation_name}'
        """
        existing_result = db_manager.execute_query_dict(check_sql)
        
        if not existing_result:
            raise HTTPException(
                status_code=404, 
                detail=f"Transformation '{transformation_name}' not found"
            )
        
        existing_transformation_id = existing_result[0]['transformation_id']
        
        # Validate SQL statements
        validation_results = []
        for statement in request.statements:
            validation = validate_sql(statement.sql_statement, request.transformation_stage)
            validation_results.append({
                "execution_sequence": statement.execution_sequence,
                "statement_type": statement.statement_type,
                "validation": validation
            })
            
            if not validation["is_valid"]:
                raise HTTPException(
                    status_code=400, 
                    detail=f"SQL validation failed for statement {statement.execution_sequence}: {'; '.join(validation['errors'])}"
                )
        
        # Upsert each statement
        upserted_statements = []
        for statement in request.statements:
            # Generate new version for upsert
            new_version = int(datetime.now().timestamp() * 1000000)
            
            # Escape SQL statement
            escaped_sql = statement.sql_statement.replace("'", "''")
            escaped_desc = (statement.description or f"{statement.statement_type} statement").replace("'", "''")
            
            # Use INSERT with ReplacingMergeTree for upsert behavior
            upsert_sql = f"""
            INSERT INTO metadata.transformation1 (
                transformation_stage,
                transformation_id,
                transformation_name,
                transformation_schema_name,
                dependencies,
                execution_frequency,
                source_schema,
                source_table,
                target_schema,
                target_table,
                execution_sequence,
                sql_statement,
                statement_type,
                version
            ) VALUES (
                '{request.transformation_stage}',
                {existing_transformation_id},
                '{transformation_name}',
                'metadata',
                '[]',
                '{request.execution_frequency}',
                '{request.source_schema or ''}',
                '{request.source_table or ''}',
                '{request.target_schema or ''}',
                '{request.target_table or ''}',
                {statement.execution_sequence},
                '{escaped_sql}',
                '{statement.statement_type}',
                {new_version}
            )
            """
            
            db_manager.execute_query_dict(upsert_sql)
            
            upserted_statements.append({
                "execution_sequence": statement.execution_sequence,
                "statement_type": statement.statement_type,
                "description": statement.description,
                "version": new_version
            })
        
        # Get final count of statements for this transformation
        count_sql = f"""
        SELECT COUNT(*) as count 
        FROM metadata.transformation1 
        WHERE transformation_name = '{transformation_name}'
        """
        count_result = db_manager.execute_query_dict(count_sql)
        total_statements = count_result[0]['count'] if count_result else 0
        
        return {
            "status": "success",
            "message": f"Transformation '{transformation_name}' updated successfully",
            "transformation_id": existing_transformation_id,
            "transformation_name": transformation_name,
            "transformation_stage": request.transformation_stage,
            "statements_upserted": len(upserted_statements),
            "total_statements": total_statements,
            "statements": upserted_statements,
            "validation_results": validation_results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating transformation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.post("/statements")
async def create_statement(request: StatementRequest):
    """
    Create a single SQL statement for an existing transformation.
    
    This endpoint adds a new SQL statement to an existing transformation
    or creates a new transformation with a single statement.
    
    Parameters:
        - transformation_id: Unique identifier for the transformation
        - execution_sequence: Order of execution (1, 2, 3, etc.)
        - sql_statement: Single SQL statement to execute
        - statement_type: Type of statement (DROP, CREATE, INSERT, UPDATE, etc.)
        - description: Human-readable description of the statement
    
    Returns:
        Dict containing creation results
    """
    try:
        db_manager = DatabaseManager()
        
        # Validate SQL statement
        validation = validate_sql(request.sql_statement, "unknown")
        if not validation["is_valid"]:
            raise HTTPException(
                status_code=400, 
                detail=f"SQL validation failed: {'; '.join(validation['errors'])}"
            )
        
        # Generate new version for upsert
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Escape SQL statement
        escaped_sql = request.sql_statement.replace("'", "''")
        escaped_desc = (request.description or f"{request.statement_type} statement").replace("'", "''")
        
        insert_sql = f"""
        INSERT INTO metadata.transformation1 (
            transformation_stage,
            udf_name,
            udf_number,
            udf_logic,
            transformation_schema_name,
            dependencies,
            execution_frequency,
            source_schema,
            source_table,
            target_schema,
            target_table,
            transformation_id,
            execution_sequence,
            sql_statement,
            statement_type,
            version
        ) VALUES (
            'stage1',
            '{request.transformation_id}',
            1,
            '{escaped_sql}',
            'metadata',
            '[]',
            'daily',
            '',
            '',
            '',
            '',
            '{request.transformation_id}',
            {request.execution_sequence},
            '{escaped_sql}',
            '{request.statement_type}',
            {new_version}
        )
        """
        
        db_manager.execute_query_dict(insert_sql)
        
        return {
            "status": "success",
            "message": f"Statement {request.execution_sequence} created for transformation '{request.transformation_id}'",
            "transformation_id": request.transformation_id,
            "execution_sequence": request.execution_sequence,
            "statement_type": request.statement_type,
            "version": new_version,
            "validation": validation
        }
        
    except Exception as e:
        logger.error(f"Error creating statement: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.get("/transformations/{transformation_id}")
async def get_transformation(transformation_id: str):
    """
    Get all statements for a specific transformation.
    
    This endpoint retrieves all SQL statements for a transformation
    ordered by execution_sequence.
    
    Parameters:
        - transformation_id: Unique identifier for the transformation
    
    Returns:
        Dict containing all statements for the transformation
    """
    try:
        db_manager = DatabaseManager()
        
        query = f"""
        SELECT 
            transformation_stage,
            transformation_id,
            transformation_name,
            execution_sequence,
            sql_statement,
            statement_type,
            source_schema,
            source_table,
            target_schema,
            target_table,
            execution_frequency,
            created_at,
            updated_at,
            version
        FROM metadata.transformation1
        WHERE transformation_name = '{transformation_id}'
        ORDER BY execution_sequence
        """
        
        results = db_manager.execute_query_dict(query)
        
        if not results:
            raise HTTPException(status_code=404, detail=f"Transformation '{transformation_id}' not found")
        
        return {
            "status": "success",
            "transformation_id": transformation_id,
            "transformation_stage": results[0]["transformation_stage"],
            "total_statements": len(results),
            "statements": results
        }
        
    except Exception as e:
        logger.error(f"Error getting transformation '{transformation_id}': {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.delete("/transformations/{transformation_id}")
async def delete_transformation(transformation_id: str):
    """
    Delete an entire transformation and all its statements.
    
    This endpoint removes all SQL statements for a transformation
    from the metadata.transformation1 table.
    
    Parameters:
        - transformation_id: Unique identifier for the transformation
    
    Returns:
        Dict containing deletion results
    """
    try:
        db_manager = DatabaseManager()
        
        # Check if transformation exists
        existing_query = f"""
        SELECT COUNT(*) as count FROM metadata.transformation1 
        WHERE transformation_id = '{transformation_id}'
        """
        existing = db_manager.execute_query_dict(existing_query)
        statement_count = existing[0]["count"] if existing else 0
        
        if statement_count == 0:
            raise HTTPException(status_code=404, detail=f"Transformation '{transformation_id}' not found")
        
        # Delete all statements for the transformation
        delete_sql = f"""
        DELETE FROM metadata.transformation1
        WHERE transformation_id = '{transformation_id}'
        """
        
        db_manager.execute_query_dict(delete_sql)
        
        return {
            "status": "success",
            "message": f"Transformation '{transformation_id}' deleted successfully",
            "statements_deleted": statement_count
        }
        
    except Exception as e:
        logger.error(f"Error deleting transformation '{transformation_id}': {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.post("/transformations/{transformation_id}/execute")
async def execute_transformation(transformation_id: str):
    """
    Execute a transformation using the TransformEngine.
    
    This endpoint executes all statements for a transformation
    in sequence using the TransformEngine class.
    
    Parameters:
        - transformation_id: Unique identifier for the transformation
    
    Returns:
        Dict containing execution results and metrics
    """
    try:
        from ..core.transform_engine import TransformEngine
        
        engine = TransformEngine()
        result = engine.execute_transformation(transformation_id)
        
        return result
        
    except Exception as e:
        logger.error(f"Error executing transformation '{transformation_id}': {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.post("/transformations/execute/parallel")
async def execute_transformations_parallel(request: dict):
    """
    Execute multiple transformations in parallel.
    
    This endpoint executes multiple transformations simultaneously
    using the TransformEngine's parallel execution capability.
    
    Parameters:
        - transformation_names: List of transformation names to execute
    
    Returns:
        Dict containing results for all transformations
    """
    try:
        from ..core.transform_engine import TransformEngine
        
        # Extract transformation names from request
        transformation_names = request.get('transformation_names', [])
        if not transformation_names:
            raise HTTPException(status_code=400, detail="transformation_names is required")
        
        engine = TransformEngine()
        result = engine.execute_transformations_parallel(transformation_names)
        
        return result
        
    except Exception as e:
        logger.error(f"Error executing transformations in parallel: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@transform_router.get("/transformations")
async def get_transformations(
    stage: Optional[str] = Query(None, description="Filter by transformation stage"),
    limit: int = Query(100, description="Maximum number of transformations to return")
):
    """
    Get all transformations or filter by stage.
    
    This endpoint retrieves all transformations from the metadata.transformation1 table
    with optional filtering by transformation stage.
    
    Parameters:
        - stage: Filter transformations by stage (e.g., 'stage1', 'stage2', 'stage3', 'stage4')
        - limit: Maximum number of transformations to return (default: 100)
    
    Returns:
        Dict containing transformations grouped by transformation_id
    """
    try:
        db_manager = DatabaseManager()
        
        where_conditions = []
        if stage:
            where_conditions.append(f"transformation_stage = '{stage}'")
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query = f"""
        SELECT 
            transformation_stage,
            transformation_id,
            execution_sequence,
            sql_statement,
            statement_type,
            source_schema,
            source_table,
            target_schema,
            target_table,
            execution_frequency,
            created_at,
            updated_at,
            version
        FROM metadata.transformation1
        {where_clause}
        ORDER BY transformation_id, execution_sequence
        LIMIT {limit}
        """
        
        results = db_manager.execute_query_dict(query)
        
        # Group results by transformation_id
        transformations = {}
        for row in results:
            trans_id = row["transformation_id"]
            if trans_id not in transformations:
                transformations[trans_id] = {
                    "transformation_id": trans_id,
                    "transformation_stage": row["transformation_stage"],
                    "statements": []
                }
            transformations[trans_id]["statements"].append(row)
        
        return {
            "status": "success",
            "transformations": list(transformations.values()),
            "total_transformations": len(transformations),
            "filtered_by_stage": stage
        }
        
    except Exception as e:
        logger.error(f"Error getting transformations: {e}")
        raise HTTPException(status_code=500, detail=str(e))