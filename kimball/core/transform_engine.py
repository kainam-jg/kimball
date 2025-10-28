#!/usr/bin/env python3
"""
Transform Engine for Sequential Statement Execution

This module handles the execution of multi-statement transformations
where each transformation consists of multiple SQL statements that
must be executed in sequence.

Key Features:
- Sequential execution of related SQL statements
- Parallel execution of different transformations
- Error handling and rollback capabilities
- Progress tracking and logging
- Support for ClickHouse single-statement limitations
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor

from ..core.database import DatabaseManager

# Configure logging
logger = logging.getLogger(__name__)

class TransformEngine:
    """
    Engine for executing multi-statement transformations.
    
    This class handles the sequential execution of SQL statements
    that belong to the same transformation, ensuring proper order
    and error handling.
    """
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.executor = ThreadPoolExecutor(max_workers=10)
    
    def get_transformation_statements(self, transformation_id: str) -> List[Dict[str, Any]]:
        """
        Get all statements for a transformation ordered by execution_sequence.
        
        Args:
            transformation_id: Unique identifier for the transformation
        
        Returns:
            List of statement dictionaries ordered by execution_sequence
        """
        try:
            query = f"""
            SELECT 
                transformation_id,
                transformation_name,
                execution_sequence,
                sql_statement,
                statement_type,
                created_at,
                updated_at,
                version
            FROM metadata.transformation1
            WHERE transformation_name = '{transformation_id}'
            ORDER BY execution_sequence
            """
            
            results = self.db_manager.execute_query_dict(query)
            return results if results else []
            
        except Exception as e:
            logger.error(f"Error getting transformation statements for {transformation_id}: {e}")
            return []
    
    def execute_transformation(self, transformation_id: str) -> Dict[str, Any]:
        """
        Execute all statements for a transformation in sequence.
        
        Args:
            transformation_id: Unique identifier for the transformation
        
        Returns:
            Dict containing execution results and metrics
        """
        try:
            statements = self.get_transformation_statements(transformation_id)
            if not statements:
                return {
                    "status": "error",
                    "message": f"No statements found for transformation {transformation_id}",
                    "transformation_id": transformation_id,
                    "statements_executed": 0
                }
            
            start_time = datetime.now()
            execution_results = []
            total_records_processed = 0
            
            logger.info(f"Starting transformation {transformation_id} with {len(statements)} statements")
            
            for statement in statements:
                sequence = statement["execution_sequence"]
                sql = statement["sql_statement"]
                stmt_type = statement["statement_type"]
                description = statement.get("description", f"{stmt_type} statement")
                
                try:
                    stmt_start_time = datetime.now()
                    result = self.db_manager.execute_query(sql)
                    stmt_execution_time = (datetime.now() - stmt_start_time).total_seconds()
                    
                    # Try to get record count from result
                    records_processed = 0
                    if result and isinstance(result, list) and len(result) > 0:
                        if isinstance(result[0], tuple) and len(result[0]) > 0:
                            if hasattr(result[0][0], 'get') and isinstance(result[0][0], dict):
                                if "written_rows" in result[0][0]:
                                    records_processed = result[0][0]["written_rows"]
                    
                    execution_results.append({
                        "execution_sequence": sequence,
                        "statement_type": stmt_type,
                        "description": description,
                        "status": "success",
                        "records_processed": records_processed,
                        "execution_time": stmt_execution_time,
                        "sql_preview": sql[:100] + "..." if len(sql) > 100 else sql
                    })
                    
                    total_records_processed += records_processed
                    
                    logger.info(f"Statement {sequence} executed successfully: {stmt_type}")
                    
                except Exception as stmt_error:
                    execution_results.append({
                        "execution_sequence": sequence,
                        "statement_type": stmt_type,
                        "description": description,
                        "status": "error",
                        "error": str(stmt_error),
                        "sql_preview": sql[:100] + "..." if len(sql) > 100 else sql
                    })
                    
                    logger.error(f"Statement {sequence} failed: {stmt_error}")
                    # Continue with next statement instead of failing entire transformation
                    # This allows partial success scenarios
            
            total_execution_time = (datetime.now() - start_time).total_seconds()
            
            # Determine overall status
            failed_statements = [r for r in execution_results if r["status"] == "error"]
            overall_status = "success" if not failed_statements else "partial_success" if len(failed_statements) < len(statements) else "error"
            
            return {
                "status": overall_status,
                "message": f"Transformation {transformation_id} completed",
                "transformation_id": transformation_id,
                "statements_executed": len(statements),
                "statements_failed": len(failed_statements),
                "total_records_processed": total_records_processed,
                "total_execution_time": total_execution_time,
                "execution_results": execution_results
            }
            
        except Exception as e:
            logger.error(f"Error executing transformation {transformation_id}: {e}")
            return {
                "status": "error",
                "message": f"Transformation {transformation_id} failed: {str(e)}",
                "transformation_id": transformation_id,
                "statements_executed": 0
            }
    
    def _execute_transformation_with_separate_db(self, transformation_id: str) -> Dict[str, Any]:
        """
        Execute a single transformation with its own database manager instance.
        
        This method creates a separate DatabaseManager instance to avoid
        concurrent query issues when running transformations in parallel.
        
        Args:
            transformation_id: Unique identifier for the transformation
        
        Returns:
            Dict containing execution results
        """
        try:
            # Create a separate database manager for this thread
            db_manager = DatabaseManager()
            
            # Get transformation statements
            statements = self.get_transformation_statements(transformation_id)
            
            if not statements:
                return {
                    "status": "error",
                    "message": f"No statements found for transformation {transformation_id}",
                    "transformation_id": transformation_id,
                    "statements_executed": 0
                }
            
            logger.info(f"Starting transformation {transformation_id} with {len(statements)} statements")
            
            start_time = datetime.now()
            execution_results = []
            statements_executed = 0
            statements_failed = 0
            total_records_processed = 0
            
            # Execute statements sequentially
            for statement in statements:
                try:
                    stmt_start_time = datetime.now()
                    
                    # Execute the SQL statement
                    result = db_manager.execute_query_dict(statement['sql_statement'])
                    
                    stmt_execution_time = (datetime.now() - stmt_start_time).total_seconds()
                    
                    execution_results.append({
                        "execution_sequence": statement['execution_sequence'],
                        "statement_type": statement['statement_type'],
                        "description": f"{statement['statement_type']} statement",
                        "status": "success",
                        "records_processed": len(result) if result else 0,
                        "execution_time": stmt_execution_time,
                        "sql_preview": statement['sql_statement'][:100] + "..." if len(statement['sql_statement']) > 100 else statement['sql_statement']
                    })
                    
                    statements_executed += 1
                    total_records_processed += len(result) if result else 0
                    
                    logger.info(f"Statement {statement['execution_sequence']} executed successfully: {statement['statement_type']}")
                    
                except Exception as e:
                    logger.error(f"Error executing statement {statement['execution_sequence']}: {e}")
                    statements_failed += 1
                    
                    execution_results.append({
                        "execution_sequence": statement['execution_sequence'],
                        "statement_type": statement['statement_type'],
                        "description": f"{statement['statement_type']} statement",
                        "status": "error",
                        "error_message": str(e),
                        "records_processed": 0,
                        "execution_time": 0,
                        "sql_preview": statement['sql_statement'][:100] + "..." if len(statement['sql_statement']) > 100 else statement['sql_statement']
                    })
            
            total_execution_time = (datetime.now() - start_time).total_seconds()
            
            # Determine overall status
            if statements_failed == 0:
                status = "success"
                message = f"Transformation {transformation_id} completed"
            elif statements_executed > 0:
                status = "partial_success"
                message = f"Transformation {transformation_id} completed with {statements_failed} failed statements"
            else:
                status = "error"
                message = f"Transformation {transformation_id} failed - no statements executed successfully"
            
            return {
                "status": status,
                "message": message,
                "transformation_id": transformation_id,
                "statements_executed": statements_executed,
                "statements_failed": statements_failed,
                "total_records_processed": total_records_processed,
                "total_execution_time": total_execution_time,
                "execution_results": execution_results
            }
            
        except Exception as e:
            logger.error(f"Error executing transformation {transformation_id}: {e}")
            return {
                "status": "error",
                "message": f"Transformation {transformation_id} failed: {str(e)}",
                "transformation_id": transformation_id,
                "statements_executed": 0
            }
    
    def execute_transformations_parallel(self, transformation_ids: List[str]) -> Dict[str, Any]:
        """
        Execute multiple transformations in parallel.
        
        Args:
            transformation_ids: List of transformation IDs to execute
        
        Returns:
            Dict containing results for all transformations
        """
        try:
            start_time = datetime.now()
            
            # Submit all transformations to thread pool with separate database managers
            future_to_id = {
                self.executor.submit(self._execute_transformation_with_separate_db, tid): tid 
                for tid in transformation_ids
            }
            
            results = {}
            for future in future_to_id:
                transformation_id = future_to_id[future]
                try:
                    result = future.result()
                    results[transformation_id] = result
                except Exception as e:
                    results[transformation_id] = {
                        "status": "error",
                        "message": f"Transformation {transformation_id} failed: {str(e)}",
                        "transformation_id": transformation_id
                    }
            
            total_execution_time = (datetime.now() - start_time).total_seconds()
            
            # Calculate summary statistics
            successful_transformations = [r for r in results.values() if r["status"] == "success"]
            partial_transformations = [r for r in results.values() if r["status"] == "partial_success"]
            failed_transformations = [r for r in results.values() if r["status"] == "error"]
            
            total_statements = sum(r.get("statements_executed", 0) for r in results.values())
            total_records = sum(r.get("total_records_processed", 0) for r in results.values())
            
            return {
                "status": "success",
                "message": f"Parallel execution completed for {len(transformation_ids)} transformations",
                "transformations_executed": len(transformation_ids),
                "successful_transformations": len(successful_transformations),
                "partial_transformations": len(partial_transformations),
                "failed_transformations": len(failed_transformations),
                "total_statements_executed": total_statements,
                "total_records_processed": total_records,
                "total_execution_time": total_execution_time,
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Error in parallel execution: {e}")
            return {
                "status": "error",
                "message": f"Parallel execution failed: {str(e)}",
                "transformations_executed": 0
            }
    
    def get_transformation_status(self, transformation_id: str) -> Dict[str, Any]:
        """
        Get the current status of a transformation.
        
        Args:
            transformation_id: Unique identifier for the transformation
        
        Returns:
            Dict containing transformation status and metadata
        """
        try:
            statements = self.get_transformation_statements(transformation_id)
            if not statements:
                return {
                    "status": "not_found",
                    "message": f"Transformation {transformation_id} not found"
                }
            
            return {
                "status": "found",
                "transformation_id": transformation_id,
                "total_statements": len(statements),
                "statements": [
                    {
                        "execution_sequence": stmt["execution_sequence"],
                        "statement_type": stmt["statement_type"],
                        "description": stmt.get("description", f"{stmt['statement_type']} statement"),
                        "created_at": stmt["created_at"]
                    }
                    for stmt in statements
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting transformation status for {transformation_id}: {e}")
            return {
                "status": "error",
                "message": f"Error getting status: {str(e)}"
            }
    
    def __del__(self):
        """Cleanup thread pool executor."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
