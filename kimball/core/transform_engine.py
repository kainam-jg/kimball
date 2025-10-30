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
    
    Uses separate tables for each stage:
    - transformation1 for stage1
    - transformation2 for stage2
    - transformation3 for stage3
    - transformation4 for stage4
    """
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.executor = ThreadPoolExecutor(max_workers=10)
    
    def _get_table_name(self, stage: str) -> str:
        """
        Get the transformation table name for a given stage.
        
        Args:
            stage: Transformation stage (stage1, stage2, stage3, stage4)
            
        Returns:
            Table name (e.g., 'metadata.transformation1')
        """
        stage_map = {
            'stage1': 'transformation1',
            'stage2': 'transformation2',
            'stage3': 'transformation3',
            'stage4': 'transformation4'
        }
        table_name = stage_map.get(stage, 'transformation1')
        return f'metadata.{table_name}'
    
    def _find_transformation_stage(self, transformation_id: str) -> Optional[str]:
        """
        Find which stage/table contains a transformation.
        
        Args:
            transformation_id: Transformation name/ID to find
            
        Returns:
            Stage name (e.g., 'stage1') or None if not found
        """
        for stage in ['stage1', 'stage2', 'stage3', 'stage4']:
            table_name = self._get_table_name(stage)
            try:
                query = f"""
                SELECT transformation_stage
                FROM {table_name}
                WHERE transformation_name = '{transformation_id}'
                LIMIT 1
                """
                result = self.db_manager.execute_query_dict(query)
                if result:
                    return result[0].get('transformation_stage')
            except Exception:
                # Table might not exist yet, try next one
                continue
        return None
    
    def get_transformation_statements(self, transformation_id: str, stage: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all statements for a transformation ordered by execution_sequence.
        
        Args:
            transformation_id: Unique identifier for the transformation (can be ID number or name)
            stage: Optional stage name. If not provided, will search all tables.
        
        Returns:
            List of statement dictionaries ordered by execution_sequence
        """
        try:
            # If stage not provided, find it
            if not stage:
                stage = self._find_transformation_stage(transformation_id)
                if not stage:
                    logger.warning(f"Transformation {transformation_id} not found in any table")
                    return []
            
            table_name = self._get_table_name(stage)
            
            from ..core.sql_transformation import SQLTransformation
            import json
            
            # Try to parse as integer for transformation_id, otherwise use as transformation_name
            try:
                trans_id_int = int(transformation_id)
                query = f"""
                SELECT 
                    transformation_id,
                    transformation_name,
                    execution_sequence,
                    sql_data,
                    statement_type,
                    created_at,
                    updated_at,
                    version
                FROM {table_name}
                WHERE transformation_id = {trans_id_int}
                ORDER BY execution_sequence
                """
            except ValueError:
                # If not a number, treat as transformation_name
                query = f"""
                SELECT 
                    transformation_id,
                    transformation_name,
                    execution_sequence,
                    sql_data,
                    statement_type,
                    created_at,
                    updated_at,
                    version
                FROM {table_name}
                WHERE transformation_name = '{transformation_id}'
                ORDER BY execution_sequence
                """
            
            results = self.db_manager.execute_query_dict(query)
            if not results:
                return []
            
            # Parse sql_data JSON and extract SQL statements
            statements = []
            for row in results:
                try:
                    # Parse the JSON from sql_data
                    sql_data_json = json.loads(row['sql_data'])
                    transformation = SQLTransformation.from_json(sql_data_json)
                    
                    # Get the SQL statement from the transformation
                    sql_statement = transformation.to_sql()
                    
                    statements.append({
                        "transformation_id": row['transformation_id'],
                        "transformation_name": row['transformation_name'],
                        "execution_sequence": row['execution_sequence'],
                        "sql_statement": sql_statement,  # For backward compatibility
                        "statement_type": row['statement_type'],
                        "created_at": row['created_at'],
                        "updated_at": row['updated_at'],
                        "version": row['version']
                    })
                except Exception as parse_error:
                    logger.error(f"Error parsing sql_data for transformation {row['transformation_id']}, sequence {row['execution_sequence']}: {parse_error}")
                    continue
            
            return statements
            
        except Exception as e:
            logger.error(f"Error getting transformation statements for {transformation_id}: {e}")
            return []
    
    def execute_transformation(self, transformation_id: str, stage: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute all statements for a transformation in sequence.
        
        Args:
            transformation_id: Unique identifier for the transformation
            stage: Optional stage name. If not provided, will search all tables.
        
        Returns:
            Dict containing execution results and metrics
        """
        try:
            statements = self.get_transformation_statements(transformation_id, stage)
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
            
            # Add record count validation after all statements are executed
            logger.info(f"Starting record count validation for transformation {transformation_id}")
            validation_result = self._validate_record_counts(transformation_id, statements, self.db_manager)
            if validation_result:
                logger.info(f"Validation result: {validation_result}")
                execution_results.append(validation_result)
            else:
                logger.info(f"No validation result for transformation {transformation_id}")
            
            total_execution_time = (datetime.now() - start_time).total_seconds()
            
            # Determine overall status
            validation_failed = any(r.get("statement_type") == "VALIDATION" and r.get("status") == "error" for r in execution_results)
            failed_statements = [r for r in execution_results if r["status"] == "error"]
            
            if not failed_statements and not validation_failed:
                overall_status = "success"
            elif not failed_statements and validation_failed:
                overall_status = "error"
            elif len(failed_statements) < len(statements):
                overall_status = "partial_success"
            else:
                overall_status = "error"
            
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
    
    def _execute_transformation_with_separate_db(self, transformation_id: str, stage: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a single transformation with its own database manager instance.
        
        This method creates a separate DatabaseManager instance to avoid
        concurrent query issues when running transformations in parallel.
        
        Args:
            transformation_id: Unique identifier for the transformation
            stage: Optional stage name. If not provided, will search all tables.
        
        Returns:
            Dict containing execution results
        """
        try:
            # Create a separate database manager for this thread
            db_manager = DatabaseManager()
            
            # Create a new TransformEngine instance for this thread
            engine = TransformEngine()
            engine.db_manager = db_manager
            
            # Get transformation statements (using the new engine instance)
            statements = engine.get_transformation_statements(transformation_id, stage)
            
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
            
            # Add record count validation after all statements are executed
            logger.info(f"Starting record count validation for transformation {transformation_id}")
            validation_result = self._validate_record_counts(transformation_id, statements, db_manager)
            if validation_result:
                logger.info(f"Validation result: {validation_result}")
                execution_results.append(validation_result)
            else:
                logger.info(f"No validation result for transformation {transformation_id}")
            
            total_execution_time = (datetime.now() - start_time).total_seconds()
            
            # Determine overall status
            validation_failed = any(r.get("statement_type") == "VALIDATION" and r.get("status") == "error" for r in execution_results)
            
            if statements_failed == 0 and not validation_failed:
                status = "success"
                message = f"Transformation {transformation_id} completed"
            elif statements_failed == 0 and validation_failed:
                status = "error"
                message = f"Transformation {transformation_id} completed but record count validation failed"
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
    
    def execute_transformations_parallel(self, transformation_ids: List[str], stage: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute multiple transformations in parallel.
        
        Args:
            transformation_ids: List of transformation IDs to execute
            stage: Optional stage name. If not provided, will search for each transformation.
        
        Returns:
            Dict containing results for all transformations
        """
        try:
            start_time = datetime.now()
            
            # Submit all transformations to thread pool with separate database managers
            future_to_id = {
                self.executor.submit(self._execute_transformation_with_separate_db, tid, stage): tid 
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
    
    def get_all_transformation_ids_for_stage(self, stage: str) -> List[int]:
        """
        Get all transformation IDs for a given stage.
        
        Args:
            stage: Transformation stage (stage1, stage2, stage3, stage4)
        
        Returns:
            List of transformation IDs
        """
        try:
            table_name = self._get_table_name(stage)
            
            query = f"""
            SELECT DISTINCT transformation_id
            FROM {table_name}
            ORDER BY transformation_id
            """
            
            results = self.db_manager.execute_query_dict(query)
            if not results:
                return []
            
            return [int(row["transformation_id"]) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting transformation IDs for stage {stage}: {e}")
            return []
    
    def execute_all_transformations_for_stage(self, stage: str, parallel: bool = True) -> Dict[str, Any]:
        """
        Execute all transformations for a given stage.
        
        Args:
            stage: Transformation stage (stage1, stage2, stage3, stage4)
            parallel: Whether to execute transformations in parallel (default: True)
        
        Returns:
            Dict containing execution results for all transformations
        """
        try:
            # Get all transformation IDs for the stage
            transformation_ids = self.get_all_transformation_ids_for_stage(stage)
            
            if not transformation_ids:
                return {
                    "status": "no_transformations",
                    "message": f"No transformations found for stage {stage}",
                    "stage": stage,
                    "transformations_executed": 0
                }
            
            # Convert to strings for the execution methods
            transformation_id_strings = [str(tid) for tid in transformation_ids]
            
            if parallel:
                # Execute in parallel
                return self.execute_transformations_parallel(transformation_id_strings, stage)
            else:
                # Execute sequentially
                results = {}
                for trans_id in transformation_id_strings:
                    result = self.execute_transformation(trans_id, stage)
                    results[trans_id] = result
                
                # Calculate summary statistics
                successful = sum(1 for r in results.values() if r.get("status") == "success")
                failed = sum(1 for r in results.values() if r.get("status") == "error")
                
                return {
                    "status": "success",
                    "message": f"Sequential execution completed for {len(transformation_ids)} transformations",
                    "stage": stage,
                    "transformations_executed": len(transformation_ids),
                    "successful_transformations": successful,
                    "failed_transformations": failed,
                    "results": results
                }
            
        except Exception as e:
            logger.error(f"Error executing all transformations for stage {stage}: {e}")
            return {
                "status": "error",
                "message": f"Error executing transformations for stage {stage}: {str(e)}",
                "stage": stage,
                "transformations_executed": 0
            }
    
    def get_transformation_status(self, transformation_id: str, stage: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the current status of a transformation.
        
        Args:
            transformation_id: Unique identifier for the transformation
            stage: Optional stage name. If not provided, will search all tables.
        
        Returns:
            Dict containing transformation status and metadata
        """
        try:
            statements = self.get_transformation_statements(transformation_id, stage)
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
    
    def _validate_record_counts(self, transformation_id: str, statements: List[Dict], db_manager) -> Optional[Dict]:
        """
        Validate that source and target record counts match for stage1 transformations.
        
        Args:
            transformation_id: Transformation ID
            statements: List of transformation statements
            db_manager: Database manager instance
        
        Returns:
            Validation result dictionary or None if not applicable
        """
        try:
            # Only validate stage1 transformations with INSERT statements
            insert_statements = [stmt for stmt in statements if stmt['statement_type'] == 'INSERT']
            if not insert_statements:
                return None
            
            # Extract source and target table information from the first INSERT statement
            insert_sql = insert_statements[0]['sql_statement']
            
            # Parse the INSERT statement to get source and target tables
            # Format: INSERT INTO silver.table_name SELECT ... FROM bronze.table_name
            if 'INSERT INTO silver.' not in insert_sql or 'FROM bronze.' not in insert_sql:
                return None
            
            # Extract target table (silver)
            target_table = insert_sql.split('INSERT INTO silver.')[1].split(' ')[0].split('\n')[0]
            
            # Extract source table (bronze) 
            source_table = insert_sql.split('FROM bronze.')[1].split(';')[0].split(' ')[0].split('\n')[0]
            
            # Get record counts
            source_count_query = f"SELECT COUNT(*) as count FROM bronze.{source_table}"
            target_count_query = f"SELECT COUNT(*) as count FROM silver.{target_table}"
            
            source_result = db_manager.execute_query_dict(source_count_query)
            target_result = db_manager.execute_query_dict(target_count_query)
            
            source_count = source_result[0]['count'] if source_result else 0
            target_count = target_result[0]['count'] if target_result else 0
            
            # Determine validation status
            counts_match = source_count == target_count
            validation_status = "success" if counts_match else "error"
            
            validation_message = f"Record count validation: Source={source_count}, Target={target_count}"
            if not counts_match:
                validation_message += f" - MISMATCH! Expected {source_count} records, got {target_count}"
            
            return {
                "execution_sequence": 999,  # Use high number to appear last
                "statement_type": "VALIDATION",
                "description": "Record count validation",
                "status": validation_status,
                "records_processed": 0,
                "execution_time": 0,
                "sql_preview": f"Source: {source_table} ({source_count}), Target: {target_table} ({target_count})",
                "validation_details": {
                    "source_table": f"bronze.{source_table}",
                    "target_table": f"silver.{target_table}",
                    "source_count": source_count,
                    "target_count": target_count,
                    "counts_match": counts_match
                }
            }
            
        except Exception as e:
            logger.error(f"Error validating record counts for transformation {transformation_id}: {e}")
            return {
                "execution_sequence": 999,
                "statement_type": "VALIDATION",
                "description": "Record count validation",
                "status": "error",
                "records_processed": 0,
                "execution_time": 0,
                "sql_preview": f"Validation error: {str(e)}",
                "validation_details": {
                    "error": str(e)
                }
            }

    def __del__(self):
        """Cleanup thread pool executor."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
