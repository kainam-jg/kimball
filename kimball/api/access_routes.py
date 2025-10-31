#!/usr/bin/env python3
"""
KIMBALL Access Phase API Routes

This module provides API endpoints for querying the gold schema.
Provides a query engine interface for accessing dimensional model data.

Features:
- Execute SELECT queries against gold schema tables
- Validate queries to ensure they only access gold schema
- List available tables and columns in gold schema
- Return query results in JSON format
"""

import logging
import re
from typing import Dict, List, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..core.database import DatabaseManager
from ..core.sql_parser import SQLParser

logger = logging.getLogger(__name__)

# Create router
access_router = APIRouter(prefix="/api/v1/access", tags=["Access"])

# Pydantic models for request/response
class QueryRequest(BaseModel):
    """
    Request model for SQL query execution.
    
    Attributes:
        query: SQL SELECT query to execute against gold schema tables
        limit: Optional limit override (defaults to query's LIMIT clause if present)
    """
    query: str
    limit: Optional[int] = None

class QueryResponse(BaseModel):
    """
    Response model for query results.
    
    Attributes:
        status: Response status ("success" or "error")
        message: Human-readable message describing the result
        row_count: Number of rows returned
        columns: List of column names in the result set
        data: Array of result rows, each as a dictionary
        execution_time_ms: Query execution time in milliseconds
    """
    status: str
    message: str
    row_count: int
    columns: List[str]
    data: List[Dict[str, Any]]
    execution_time_ms: Optional[float] = None

def validate_gold_schema_query(query: str) -> Dict[str, Any]:
    """
    Validate that the query only accesses gold schema tables.
    
    Uses SQLParser.extract_source_tables() to get referenced tables,
    then validates they are all in the gold schema.
    
    Args:
        query: SQL query string
        
    Returns:
        Dict with 'valid' bool and 'message' str
    """
    try:
        # Extract referenced tables using existing SQLParser (read-only usage)
        referenced_tables = SQLParser.extract_source_tables(query)
        
        # Also check for tables mentioned in FROM/JOIN clauses with different patterns
        # Handle cases like "FROM gold.table_name", "FROM table_name", etc.
        from_pattern = r'FROM\s+(?:gold\.)?([a-zA-Z_][a-zA-Z0-9_]*)'
        join_pattern = r'JOIN\s+(?:gold\.)?([a-zA-Z_][a-zA-Z0-9_]*)'
        
        from_matches = re.findall(from_pattern, query, re.IGNORECASE)
        join_matches = re.findall(join_pattern, query, re.IGNORECASE)
        
        all_tables = set()
        for table in referenced_tables:
            # Extract table name (remove schema prefix if present)
            if '.' in table:
                schema, table_name = table.split('.', 1)
                if schema.lower() != 'gold':
                    return {
                        'valid': False,
                        'message': f"Query references non-gold schema table: {table}. Only 'gold' schema is allowed."
                    }
                all_tables.add(table_name.lower())
            else:
                all_tables.add(table.lower())
        
        # Add tables from FROM/JOIN patterns
        for table in from_matches + join_matches:
            all_tables.add(table.lower())
        
        # Get list of actual gold schema tables
        db_manager = DatabaseManager()
        gold_tables_query = """
        SELECT name as table_name
        FROM system.tables
        WHERE database = 'gold'
        ORDER BY name
        """
        
        try:
            gold_tables_result = db_manager.execute_query_dict(gold_tables_query)
            gold_table_names = {row['table_name'].lower() for row in gold_tables_result if gold_tables_result}
        except Exception as e:
            logger.warning(f"Could not verify gold schema tables: {e}")
            # If we can't verify, still allow query but log warning
            gold_table_names = set()
        
        # Validate all referenced tables exist in gold schema
        if gold_table_names and all_tables:
            invalid_tables = all_tables - gold_table_names
            if invalid_tables:
                return {
                    'valid': False,
                    'message': f"Query references tables not in gold schema: {', '.join(invalid_tables)}. Available tables: {', '.join(sorted(gold_table_names))}"
                }
        
        # Check for dangerous operations (non-SELECT statements)
        query_upper = query.strip().upper()
        if not query_upper.startswith('SELECT'):
            return {
                'valid': False,
                'message': "Only SELECT queries are allowed. DML/DDL operations are not permitted."
            }
        
        # Check for dangerous keywords
        dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE', 'REPLACE']
        for keyword in dangerous_keywords:
            # Only check if keyword appears as a statement (not in comments or strings)
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, query_upper):
                # Check if it's in a SELECT context (e.g., SELECT DROP FROM ... would be weird)
                if keyword not in ['SELECT', 'FROM', 'WHERE', 'ORDER', 'GROUP', 'HAVING', 'LIMIT']:
                    return {
                        'valid': False,
                        'message': f"Query contains dangerous keyword: {keyword}. Only SELECT queries are allowed."
                    }
        
        return {
            'valid': True,
            'message': 'Query validated successfully',
            'tables_accessed': list(all_tables)
        }
        
    except Exception as e:
        logger.error(f"Error validating query: {e}")
        return {
            'valid': False,
            'message': f"Query validation error: {str(e)}"
        }

@access_router.get(
    "/status",
    summary="Get Access Phase Status",
    description="Returns the status of the Access Phase and lists all available tables in the gold schema with their row counts.",
    response_description="Access phase status with available tables"
)
async def get_access_status():
    """
    Get Access Phase status and available tables in gold schema.
    
    Returns status information including:
    - Phase status (active/inactive)
    - Total number of tables in gold schema
    - List of tables with row counts
    
    Returns:
        Dict[str, Any]: Access phase status and available tables
    """
    try:
        db_manager = DatabaseManager()
        
        # Get gold schema tables
        tables_query = """
        SELECT 
            name as table_name,
            total_rows as row_count
        FROM system.tables
        WHERE database = 'gold'
        ORDER BY name
        """
        
        tables_result = db_manager.execute_query_dict(tables_query)
        tables = tables_result if tables_result else []
        
        return {
            "status": "active",
            "phase": "Access",
            "description": "Query engine for gold schema dimensional model",
            "schema": "gold",
            "total_tables": len(tables),
            "tables": [{"table_name": t["table_name"], "row_count": t.get("row_count", 0)} for t in tables]
        }
        
    except Exception as e:
        logger.error(f"Error getting Access Phase status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@access_router.get(
    "/tables",
    summary="List Gold Schema Tables",
    description="Returns a list of all tables in the gold schema with their metadata including row counts and size information.",
    response_description="List of tables with metadata"
)
async def get_gold_tables():
    """
    Get list of all tables in the gold schema.
    
    Returns metadata for all tables including:
    - Table name
    - Row count
    - Size in bytes
    
    Returns:
        Dict[str, Any]: List of tables with metadata
    """
    try:
        db_manager = DatabaseManager()
        
        tables_query = """
        SELECT 
            name as table_name,
            total_rows as row_count,
            total_bytes as size_bytes
        FROM system.tables
        WHERE database = 'gold'
        ORDER BY name
        """
        
        tables_result = db_manager.execute_query_dict(tables_query)
        
        return {
            "status": "success",
            "schema": "gold",
            "count": len(tables_result) if tables_result else 0,
            "tables": tables_result if tables_result else []
        }
        
    except Exception as e:
        logger.error(f"Error getting gold schema tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@access_router.get(
    "/table/{table_name}/columns",
    summary="Get Table Columns",
    description="Returns detailed column information for a specific table in the gold schema, including column names, types, positions, and key information.",
    response_description="Column information for the specified table"
)
async def get_table_columns(table_name: str):
    """
    Get column information for a specific gold schema table.
    
    Args:
        table_name: Name of the table (without schema prefix, e.g., "daily_sales_fact")
        
    Returns:
        Dict[str, Any]: Column information including:
        - Column names and data types
        - Column positions
        - Primary key indicators
        - Default value information
        
    Raises:
        HTTPException: 404 if table not found in gold schema
    """
    try:
        db_manager = DatabaseManager()
        
        # First verify table exists in gold schema
        verify_query = f"""
        SELECT name
        FROM system.tables
        WHERE database = 'gold' AND name = '{table_name}'
        """
        
        verify_result = db_manager.execute_query_dict(verify_query)
        if not verify_result:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in gold schema")
        
        # Get columns
        columns_query = f"""
        SELECT 
            name as column_name,
            type as column_type,
            position,
            default_kind,
            is_in_primary_key
        FROM system.columns
        WHERE database = 'gold' AND table = '{table_name}'
        ORDER BY position
        """
        
        columns_result = db_manager.execute_query_dict(columns_query)
        
        return {
            "status": "success",
            "schema": "gold",
            "table_name": table_name,
            "column_count": len(columns_result) if columns_result else 0,
            "columns": columns_result if columns_result else []
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting table columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@access_router.post(
    "/query",
    summary="Execute SQL Query",
    description="Executes a SELECT query against the gold schema with comprehensive security validation. Only SELECT queries accessing gold schema tables are permitted.",
    response_description="Query results with data, metadata, and execution metrics"
)
async def execute_query(request: QueryRequest):
    """
    Execute a SELECT query against the gold schema.
    
    This endpoint provides a secure query interface for the gold schema dimensional model:
    - Validates that the query only accesses gold schema tables
    - Ensures only SELECT queries are allowed (blocks DML/DDL operations)
    - Validates table existence before execution
    - Supports optional LIMIT override in the request
    - Returns results in JSON format with execution metrics
    
    Security Features:
    - Blocks non-SELECT statements (INSERT, UPDATE, DELETE, DROP, etc.)
    - Validates all referenced tables exist in gold schema
    - Rejects queries referencing other schemas (bronze, silver)
    - Blocks dangerous SQL keywords
    
    Args:
        request: QueryRequest containing:
            - query: SQL SELECT query (e.g., "SELECT * FROM gold.daily_sales_fact LIMIT 10")
            - limit: Optional limit override (applied if query doesn't have LIMIT)
        
    Returns:
        Dict[str, Any]: Query results including:
        - status: "success" or "error"
        - message: Execution status message
        - row_count: Number of rows returned
        - columns: List of column names
        - data: Array of result rows
        - execution_time_ms: Query execution time
        - tables_accessed: List of tables accessed by the query
        
    Raises:
        HTTPException: 400 if query validation fails
        HTTPException: 500 if query execution fails
        
    Example Request:
        {
            "query": "SELECT f.amount_sales, c.calendar_year FROM gold.daily_sales_fact f LEFT JOIN gold.calendar_dim c ON f.sales_date = c.calendar_date LIMIT 100",
            "limit": 50
        }
    """
    try:
        import time
        start_time = time.time()
        
        # Validate query
        validation = validate_gold_schema_query(request.query)
        if not validation['valid']:
            raise HTTPException(status_code=400, detail=validation['message'])
        
        # Apply limit if specified in request
        query = request.query.strip()
        if request.limit and request.limit > 0:
            # Check if query already has a LIMIT clause
            query_upper = query.upper()
            if 'LIMIT' in query_upper:
                # Replace existing limit
                query = re.sub(r'\bLIMIT\s+\d+', f'LIMIT {request.limit}', query, flags=re.IGNORECASE)
            else:
                # Add limit at the end
                query = f"{query} LIMIT {request.limit}"
        
        # Execute query
        db_manager = DatabaseManager()
        results = db_manager.execute_query_dict(query)
        
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Extract column names from first row if results exist
        columns = []
        if results and len(results) > 0:
            columns = list(results[0].keys())
        
        return {
            "status": "success",
            "message": "Query executed successfully",
            "row_count": len(results) if results else 0,
            "columns": columns,
            "data": results if results else [],
            "execution_time_ms": round(execution_time, 2),
            "tables_accessed": validation.get('tables_accessed', [])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"Query execution error: {str(e)}")

