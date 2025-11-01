"""
KIMBALL Administration API Routes

This module provides API endpoints for administrative tasks such as:
- Log management and pruning
- System configuration
- Maintenance tasks
- Health monitoring
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel

from ..core.logger import Logger
from ..core.config import Config
from ..core.log_pruner import LogPruner
from ..core.database import DatabaseManager
from ..core.table_initializer import TableInitializer

# Initialize router
admin_router = APIRouter(prefix="/api/v1/admin", tags=["Administration"])
logger = Logger("admin_api")

# Initialize services
log_pruner = LogPruner()
config = Config()
db_manager = DatabaseManager()
table_initializer = TableInitializer()

# Pydantic models
class LogPruningRequest(BaseModel):
    """Request model for manual log pruning."""
    ttl_days: Optional[int] = None  # Override TTL if provided

class LogQueryRequest(BaseModel):
    """Request model for querying logs."""
    logger_name: Optional[str] = None
    log_level: Optional[str] = None  # INFO, WARNING, ERROR, DEBUG, CRITICAL
    phase: Optional[str] = None
    start_time: Optional[str] = None  # ISO format datetime
    end_time: Optional[str] = None    # ISO format datetime
    limit: int = 1000
    offset: int = 0

class ClickHouseConfigRequest(BaseModel):
    """Request model for ClickHouse configuration."""
    host: str
    port: int
    username: str
    password: str
    database: str

class ConfigUpdateRequest(BaseModel):
    """Request model for configuration updates."""
    value: Any

# Administration Status
@admin_router.get("/status")
async def get_admin_status():
    """Get overall status of administration services."""
    try:
        logger.log_api_call("/admin/status", "GET")
        
        # Get log pruning status
        pruning_enabled = log_pruner.is_enabled()
        ttl_days = log_pruner.get_ttl_days()
        interval_minutes = log_pruner.get_interval_minutes()
        pruning_running = log_pruner.running
        
        return {
            "status": "success",
            "service": "Administration",
            "log_pruning": {
                "enabled": pruning_enabled,
                "running": pruning_running,
                "ttl_days": ttl_days,
                "interval_minutes": interval_minutes
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting admin status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Log Management Endpoints
@admin_router.get("/logs")
async def query_logs(
    table: Optional[str] = None,  # logs.application, logs.acquire, etc.
    logger_name: Optional[str] = None,
    log_level: Optional[str] = None,
    phase: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0
):
    """Query logs from log tables. Can query specific table or all tables."""
    try:
        request_data = {
            "table": table,
            "logger_name": logger_name,
            "log_level": log_level,
            "phase": phase,
            "start_time": start_time,
            "end_time": end_time,
            "limit": limit,
            "offset": offset
        }
        logger.log_api_call("/admin/logs", "GET", request_data=request_data, phase="Administration")
        
        # Determine which table(s) to query
        if table:
            # Query specific table
            log_tables = [table]
        else:
            # Query all log tables (union)
            log_tables = [
                "logs.application",
                "logs.acquire",
                "logs.discover",
                "logs.model",
                "logs.transform",
                "logs.pipeline",
                "logs.administration"
            ]
        
        # Build WHERE clause
        where_clauses = []
        
        if logger_name:
            escaped_name = logger_name.replace("'", "''")
            where_clauses.append(f"logger_name = '{escaped_name}'")
        
        if log_level:
            escaped_level = log_level.replace("'", "''")
            where_clauses.append(f"log_level = '{escaped_level}'")
        
        if phase:
            escaped_phase = phase.replace("'", "''")
            where_clauses.append(f"phase = '{escaped_phase}'")
        
        if start_time:
            where_clauses.append(f"timestamp >= '{start_time}'")
        
        if end_time:
            where_clauses.append(f"timestamp <= '{end_time}'")
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        
        # Build query - if single table, query directly; if multiple, use UNION ALL
        if len(log_tables) == 1:
            query = f"""
            SELECT timestamp, logger_name, log_level, message, module, function,
                   line_number, phase, endpoint, method, error_type, metadata
            FROM {log_tables[0]}
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT {limit} OFFSET {offset}
            """
        else:
            # Union all tables
            union_queries = []
            for log_table in log_tables:
                union_queries.append(f"""
                SELECT timestamp, logger_name, log_level, message, module, function,
                       line_number, phase, endpoint, method, error_type, metadata
                FROM {log_table}
                {where_clause}
                """)
            
            query = f"""
            SELECT * FROM (
                {' UNION ALL '.join(union_queries)}
            )
            ORDER BY timestamp DESC
            LIMIT {limit} OFFSET {offset}
            """
        
        results = db_manager.execute_query_dict(query)
        
        return {
            "status": "success",
            "logs": results,
            "count": len(results),
            "limit": limit,
            "offset": offset,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error querying logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.post("/logs/prune")
async def prune_logs_manual(request: LogPruningRequest):
    """Manually trigger log pruning."""
    try:
        logger.log_api_call("/admin/logs/prune", "POST", request_data=request.dict())
        
        # Use provided TTL or default from config
        if request.ttl_days:
            # Temporarily override TTL for this operation
            original_ttl = log_pruner.get_ttl_days()
            config.set('logging.ttl_days', request.ttl_days)
            result = await log_pruner.prune_logs()
            # Restore original TTL
            config.set('logging.ttl_days', original_ttl)
        else:
            result = await log_pruner.prune_logs()
        
        return {
            "status": "success",
            "pruning_result": result,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error pruning logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.get("/logs/stats")
async def get_log_stats():
    """Get statistics about logs in the system."""
    try:
        logger.log_api_call("/admin/logs/stats", "GET", phase="Administration")
        
        # Get stats from all log tables
        log_tables = [
            "logs.application",
            "logs.acquire",
            "logs.discover",
            "logs.model",
            "logs.transform",
            "logs.pipeline",
            "logs.administration"
        ]
        
        # Total log count across all tables
        total_count_queries = [f"SELECT count() as total FROM {table}" for table in log_tables]
        total_query = f"""
        SELECT sum(total) as total FROM (
            {' UNION ALL '.join(total_count_queries)}
        )
        """
        total_result = db_manager.execute_query_dict(total_query)
        total_logs = total_result[0]['total'] if total_result else 0
        
        # Logs by level (across all tables)
        level_queries = [f"""
        SELECT log_level, count() as count
        FROM {table}
        GROUP BY log_level
        """ for table in log_tables]
        level_query = f"""
        SELECT log_level, sum(count) as count
        FROM (
            {' UNION ALL '.join(level_queries)}
        )
        GROUP BY log_level
        ORDER BY count DESC
        """
        level_results = db_manager.execute_query_dict(level_query)
        
        # Logs by logger name (across all tables)
        logger_queries = [f"""
        SELECT logger_name, count() as count
        FROM {table}
        GROUP BY logger_name
        """ for table in log_tables]
        logger_query = f"""
        SELECT logger_name, sum(count) as count
        FROM (
            {' UNION ALL '.join(logger_queries)}
        )
        GROUP BY logger_name
        ORDER BY count DESC
        LIMIT 10
        """
        logger_results = db_manager.execute_query_dict(logger_query)
        
        # Oldest and newest logs (across all tables)
        date_queries = [f"""
        SELECT 
            min(timestamp) as oldest,
            max(timestamp) as newest
        FROM {table}
        """ for table in log_tables]
        date_query = f"""
        SELECT 
            min(oldest) as oldest,
            max(newest) as newest
        FROM (
            {' UNION ALL '.join(date_queries)}
        )
        """
        date_result = db_manager.execute_query_dict(date_query)
        date_info = date_result[0] if date_result else {}
        
        return {
            "status": "success",
            "stats": {
                "total_logs": total_logs,
                "by_level": {row['log_level']: row['count'] for row in level_results},
                "top_loggers": {row['logger_name']: row['count'] for row in logger_results},
                "oldest_log": date_info.get('oldest'),
                "newest_log": date_info.get('newest')
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting log stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.post("/logs/pruning/start")
async def start_log_pruning_service():
    """Start the log pruning service (runs periodically)."""
    try:
        logger.log_api_call("/admin/logs/pruning/start", "POST")
        
        if log_pruner.running:
            return {
                "status": "success",
                "message": "Log pruning service is already running",
                "running": True,
                "timestamp": datetime.now().isoformat()
            }
        
        log_pruner.start()
        
        return {
            "status": "success",
            "message": "Log pruning service started",
            "running": log_pruner.running,
            "interval_minutes": log_pruner.get_interval_minutes(),
            "ttl_days": log_pruner.get_ttl_days(),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error starting log pruning service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.post("/logs/pruning/stop")
async def stop_log_pruning_service():
    """Stop the log pruning service."""
    try:
        logger.log_api_call("/admin/logs/pruning/stop", "POST")
        
        if not log_pruner.running:
            return {
                "status": "success",
                "message": "Log pruning service is not running",
                "running": False,
                "timestamp": datetime.now().isoformat()
            }
        
        await log_pruner.stop_async()
        
        return {
            "status": "success",
            "message": "Log pruning service stopped",
            "running": log_pruner.running,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error stopping log pruning service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Configuration Management
@admin_router.get("/config")
async def get_configuration():
    """Get current system configuration (non-sensitive values only)."""
    try:
        logger.log_api_call("/admin/config", "GET")
        
        full_config = config.get_config()
        
        # Remove sensitive data before returning
        safe_config = full_config.copy()
        if 'clickhouse' in safe_config:
            clickhouse_config = safe_config['clickhouse'].copy()
            if 'password' in clickhouse_config:
                clickhouse_config['password'] = "***REDACTED***"
            safe_config['clickhouse'] = clickhouse_config
        
        return {
            "status": "success",
            "configuration": safe_config,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting configuration: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Setup and Initialization Endpoints
@admin_router.post("/setup/init")
async def initialize_all_tables():
    """Initialize all KIMBALL schemas and tables from DDL files.
    
    STRICT Initialization order (must complete each step before proceeding):
    1. Create logs database in ClickHouse
    2. Create logging tables in ClickHouse
    3. Create metadata database in ClickHouse
    4. Create metadata tables in ClickHouse
    5. Create bronze, silver, gold databases in ClickHouse
    
    If ANY step fails, the entire process fails and stops immediately.
    """
    # Note: We can't use logger yet since logs tables don't exist
    # Use TableInitializer without logger to avoid recursion during initialization
    init_without_logger = TableInitializer(use_logger=False)
    
    initialization_results = {
        "logs_database": {},
        "logs_tables": {},
        "metadata_database": {},
        "metadata_tables": {},
        "bronze_database": {},
        "silver_database": {},
        "gold_database": {}
    }
    
    try:
        # Handle Windows console encoding issues
        import sys
        
        # Safe print function that handles encoding errors
        def safe_print(msg):
            try:
                print(msg)
            except UnicodeEncodeError:
                # Replace problematic characters for Windows console
                safe_msg = msg.encode('ascii', 'replace').decode('ascii')
                print(safe_msg)
        
        safe_print("Starting KIMBALL initialization...")
        safe_print("=" * 60)
        
        # STEP 1: Create logs database
        safe_print("\n[STEP 1] Creating logs database...")
        if not init_without_logger.schema_exists("logs"):
            success = init_without_logger.create_schema("logs")
            if not success:
                error_msg = "FAILED: Could not create logs database"
                safe_print(f"ERROR: {error_msg}")
                return {
                    "status": "error",
                    "message": error_msg,
                    "failed_step": "1 - Create logs database",
                    "results": initialization_results,
                    "timestamp": datetime.now().isoformat()
                }
            safe_print("[OK] logs database created successfully")
        else:
            safe_print("[OK] logs database already exists")
        
        initialization_results["logs_database"] = {
            "success": True,
            "exists": init_without_logger.schema_exists("logs"),
            "message": "Created or already exists"
        }
        
        # STEP 2: Create logging tables
        safe_print("\n[STEP 2] Creating logging tables...")
        logs_tables = [
            "logs.application",
            "logs.acquire",
            "logs.discover",
            "logs.model",
            "logs.transform",
            "logs.pipeline",
            "logs.administration"
        ]
        
        for table_name in logs_tables:
            safe_print(f"  Creating {table_name}...")
            if not init_without_logger.table_exists(table_name):
                success = init_without_logger.create_table(table_name)
                if not success:
                    error_msg = f"FAILED: Could not create table {table_name}"
                    safe_print(f"ERROR: {error_msg}")
                    return {
                        "status": "error",
                        "message": error_msg,
                        "failed_step": f"2 - Create logging tables (failed on {table_name})",
                        "results": initialization_results,
                        "timestamp": datetime.now().isoformat()
                    }
                safe_print(f"  [OK] {table_name} created")
            else:
                safe_print(f"  [OK] {table_name} already exists")
            
            initialization_results["logs_tables"][table_name] = {
                "success": True,
                "exists": init_without_logger.table_exists(table_name),
                "message": "Created or already exists"
            }
        
        safe_print("[OK] All logging tables created successfully")
        
        # STEP 3: Create metadata database
        safe_print("\n[STEP 3] Creating metadata database...")
        if not init_without_logger.schema_exists("metadata"):
            success = init_without_logger.create_schema("metadata")
            if not success:
                error_msg = "FAILED: Could not create metadata database"
                safe_print(f"ERROR: {error_msg}")
                return {
                    "status": "error",
                    "message": error_msg,
                    "failed_step": "3 - Create metadata database",
                    "results": initialization_results,
                    "timestamp": datetime.now().isoformat()
                }
            safe_print("[OK] metadata database created successfully")
        else:
            safe_print("[OK] metadata database already exists")
        
        initialization_results["metadata_database"] = {
            "success": True,
            "exists": init_without_logger.schema_exists("metadata"),
            "message": "Created or already exists"
        }
        
        # STEP 4: Create metadata tables
        safe_print("\n[STEP 4] Creating metadata tables...")
        metadata_tables = [
            "metadata.acquire",
            "metadata.transformation0",
            "metadata.transformation1",
            "metadata.transformation2",
            "metadata.transformation3",
            "metadata.transformation4",
            "metadata.definitions",
            "metadata.discover",
            "metadata.erd",
            "metadata.hierarchies",
            "metadata.dimensional_model"
        ]
        
        for table_name in metadata_tables:
            safe_print(f"  Creating {table_name}...")
            if not init_without_logger.table_exists(table_name):
                success = init_without_logger.create_table(table_name)
                if not success:
                    error_msg = f"FAILED: Could not create table {table_name}"
                    safe_print(f"ERROR: {error_msg}")
                    return {
                        "status": "error",
                        "message": error_msg,
                        "failed_step": f"4 - Create metadata tables (failed on {table_name})",
                        "results": initialization_results,
                        "timestamp": datetime.now().isoformat()
                    }
                safe_print(f"  [OK] {table_name} created")
            else:
                safe_print(f"  [OK] {table_name} already exists")
            
            initialization_results["metadata_tables"][table_name] = {
                "success": True,
                "exists": init_without_logger.table_exists(table_name),
                "message": "Created or already exists"
            }
        
        safe_print("[OK] All metadata tables created successfully")
        
        # STEP 5: Create bronze, silver, gold databases
        safe_print("\n[STEP 5] Creating bronze, silver, gold databases...")
        data_schemas = ["bronze", "silver", "gold"]
        
        for schema_name in data_schemas:
            safe_print(f"  Creating {schema_name} database...")
            if not init_without_logger.schema_exists(schema_name):
                success = init_without_logger.create_schema(schema_name)
                if not success:
                    error_msg = f"FAILED: Could not create {schema_name} database"
                    safe_print(f"ERROR: {error_msg}")
                    return {
                        "status": "error",
                        "message": error_msg,
                        "failed_step": f"5 - Create {schema_name} database",
                        "results": initialization_results,
                        "timestamp": datetime.now().isoformat()
                    }
                safe_print(f"  [OK] {schema_name} database created")
            else:
                safe_print(f"  [OK] {schema_name} database already exists")
            
            initialization_results[f"{schema_name}_database"] = {
                "success": True,
                "exists": init_without_logger.schema_exists(schema_name),
                "message": "Created or already exists"
            }
        
        safe_print("[OK] All data databases (bronze, silver, gold) created successfully")
        
        safe_print("\n" + "=" * 60)
        safe_print("[OK] INITIALIZATION COMPLETE - All steps succeeded")
        safe_print("=" * 60)
        
        return {
            "status": "success",
            "message": "Initialization complete - all steps succeeded",
            "results": initialization_results,
            "summary": {
                "logs_database": "created",
                "logs_tables_count": len(logs_tables),
                "metadata_database": "created",
                "metadata_tables_count": len(metadata_tables),
                "bronze_database": "created",
                "silver_database": "created",
                "gold_database": "created"
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        error_msg = f"FATAL ERROR during initialization: {str(e)}"
        try:
            print(f"\nERROR: {error_msg}")
        except:
            print(f"\nERROR: {error_msg.encode('ascii', 'replace').decode('ascii')}")
        import traceback
        try:
            traceback.print_exc()
        except:
            pass  # Skip traceback if encoding fails
        return {
            "status": "error",
            "message": error_msg,
            "failed_step": "Unknown - exception occurred",
            "exception": str(e),
            "results": initialization_results,
            "timestamp": datetime.now().isoformat()
        }

@admin_router.post("/setup/schemas")
async def initialize_schemas():
    """Create all standard KIMBALL schemas (logs, metadata, bronze, silver, gold)."""
    try:
        # Can't use logger yet - schemas need to be created first
        print("Creating KIMBALL schemas...")
        
        schema_results = table_initializer.create_all_schemas()
        all_schemas_ok = all(r.get("success") or r.get("exists") for r in schema_results.values())
        
        return {
            "status": "success" if all_schemas_ok else "partial",
            "message": "All schemas created" if all_schemas_ok else "Some schemas failed to create",
            "schemas": schema_results,
            "total_schemas": len(schema_results),
            "created_count": sum(1 for r in schema_results.values() if r.get("success")),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        error_msg = f"Error creating schemas: {e}"
        print(error_msg)
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.post("/setup/init/{table_name}")
async def initialize_table(table_name: str):
    """Initialize a specific table from DDL file."""
    try:
        logger.log_api_call(f"/admin/setup/init/{table_name}", "POST")
        
        success = table_initializer.create_table(table_name)
        exists = table_initializer.table_exists(table_name)
        
        return {
            "status": "success" if (success or exists) else "error",
            "table_name": table_name,
            "created": success,
            "exists": exists,
            "message": "Table created successfully" if success else ("Table already exists" if exists else "Failed to create table"),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error initializing table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.post("/setup/init/{table_name}/force")
async def force_recreate_table(table_name: str):
    """Force recreate a table (drop and recreate)."""
    try:
        logger.log_api_call(f"/admin/setup/init/{table_name}/force", "POST")
        
        success = table_initializer.create_table(table_name, force=True)
        exists = table_initializer.table_exists(table_name)
        
        return {
            "status": "success" if (success and exists) else "error",
            "table_name": table_name,
            "recreated": success and exists,
            "exists": exists,
            "message": "Table recreated successfully" if (success and exists) else "Failed to recreate table",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error force recreating table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.get("/setup/status")
async def get_setup_status():
    """Get initialization status of all tables."""
    try:
        logger.log_api_call("/admin/setup/status", "GET")
        
        # Define all schemas and tables
        schemas = table_initializer.STANDARD_SCHEMAS
        
        logs_tables = [
            "logs.application",
            "logs.acquire",
            "logs.discover",
            "logs.model",
            "logs.transform",
            "logs.pipeline",
            "logs.administration"
        ]
        
        metadata_tables = [
            "metadata.acquire",
            "metadata.transformation0",
            "metadata.transformation1",
            "metadata.transformation2",
            "metadata.transformation3",
            "metadata.transformation4",
            "metadata.definitions",
            "metadata.discover",
            "metadata.erd",
            "metadata.hierarchies",
            "metadata.dimensional_model"
        ]
        
        # Check schemas
        schema_status = {}
        for schema_name in schemas:
            exists = table_initializer.schema_exists(schema_name)
            schema_status[schema_name] = {
                "exists": exists,
                "status": "initialized" if exists else "not_initialized"
            }
        
        # Check logs tables
        logs_status = {}
        for table_name in logs_tables:
            exists = table_initializer.table_exists(table_name)
            logs_status[table_name] = {
                "exists": exists,
                "status": "initialized" if exists else "not_initialized"
            }
        
        # Check metadata tables
        metadata_status = {}
        for table_name in metadata_tables:
            exists = table_initializer.table_exists(table_name)
            metadata_status[table_name] = {
                "exists": exists,
                "status": "initialized" if exists else "not_initialized"
            }
        
        all_schemas_initialized = all(s["exists"] for s in schema_status.values())
        all_logs_initialized = all(s["exists"] for s in logs_status.values())
        all_metadata_initialized = all(s["exists"] for s in metadata_status.values())
        all_initialized = all_schemas_initialized and all_logs_initialized and all_metadata_initialized
        
        total_tables = len(logs_tables) + len(metadata_tables)
        initialized_count = (sum(1 for s in logs_status.values() if s["exists"]) + 
                            sum(1 for s in metadata_status.values() if s["exists"]))
        
        return {
            "status": "success",
            "all_initialized": all_initialized,
            "schemas": {
                "status": schema_status,
                "all_initialized": all_schemas_initialized,
                "initialized_count": sum(1 for s in schema_status.values() if s["exists"]),
                "total": len(schemas)
            },
            "logs_tables": {
                "status": logs_status,
                "all_initialized": all_logs_initialized,
                "initialized_count": sum(1 for s in logs_status.values() if s["exists"]),
                "total": len(logs_tables)
            },
            "metadata_tables": {
                "status": metadata_status,
                "all_initialized": all_metadata_initialized,
                "initialized_count": sum(1 for s in metadata_status.values() if s["exists"]),
                "total": len(metadata_tables)
            },
            "total_tables": total_tables,
            "initialized_count": initialized_count,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting setup status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ClickHouse Configuration Management
@admin_router.get("/config/clickhouse")
async def get_clickhouse_config(include_password: bool = False):
    """Get ClickHouse configuration."""
    try:
        logger.log_api_call("/admin/config/clickhouse", "GET")
        
        clickhouse_config = config.get('clickhouse', {})
        
        # Redact password unless explicitly requested
        if not include_password and 'password' in clickhouse_config:
            safe_config = clickhouse_config.copy()
            safe_config['password'] = "***REDACTED***"
            return {
                "status": "success",
                "configuration": safe_config,
                "message": "Password redacted. Set include_password=true to view.",
                "timestamp": datetime.now().isoformat()
            }
        
        return {
            "status": "success",
            "configuration": clickhouse_config,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting ClickHouse config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.put("/config/clickhouse")
async def update_clickhouse_config(request: ClickHouseConfigRequest):
    """Update ClickHouse configuration."""
    try:
        logger.log_api_call("/admin/config/clickhouse", "PUT")
        
        # Validate required fields
        if not all([request.host, request.port, request.username, request.password, request.database]):
            raise HTTPException(status_code=400, detail="All fields (host, port, username, password, database) are required")
        
        # Update configuration
        config.set('clickhouse.host', request.host)
        config.set('clickhouse.port', request.port)
        config.set('clickhouse.username', request.username)
        config.set('clickhouse.password', request.password)
        config.set('clickhouse.database', request.database)
        
        # Save to file
        saved = config.save()
        
        if not saved:
            raise HTTPException(status_code=500, detail="Failed to save configuration to file")
        
        return {
            "status": "success",
            "message": "ClickHouse configuration updated successfully. Application restart may be required.",
            "configuration": {
                "host": request.host,
                "port": request.port,
                "username": request.username,
                "password": "***REDACTED***",
                "database": request.database
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating ClickHouse config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@admin_router.post("/config/clickhouse/test")
async def test_clickhouse_connection():
    """Test ClickHouse connection with current configuration."""
    try:
        logger.log_api_call("/admin/config/clickhouse/test", "POST")
        
        # Create a temporary database manager to test connection
        test_db_manager = DatabaseManager()
        
        # Try a simple query
        result = test_db_manager.execute_query("SELECT 1")
        
        if result:
            return {
                "status": "success",
                "connected": True,
                "message": "ClickHouse connection successful",
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "status": "error",
                "connected": False,
                "message": "ClickHouse connection failed - no response",
                "timestamp": datetime.now().isoformat()
            }
        
    except Exception as e:
        logger.error(f"Error testing ClickHouse connection: {e}")
        return {
            "status": "error",
            "connected": False,
            "message": f"ClickHouse connection failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }

@admin_router.put("/config/{path:path}")
async def update_config_value(path: str, request: ConfigUpdateRequest):
    """Update a configuration value at a nested path (e.g., logging.ttl_days)."""
    try:
        logger.log_api_call(f"/admin/config/{path}", "PUT", request_data={"path": path, "value": request.value})
        
        # Set the value using dot notation
        config.set(path, request.value)
        
        # Save to file
        saved = config.save()
        
        if not saved:
            raise HTTPException(status_code=500, detail="Failed to save configuration to file")
        
        return {
            "status": "success",
            "message": f"Configuration at path '{path}' updated successfully",
            "path": path,
            "value": request.value if path != "clickhouse.password" else "***REDACTED***",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error updating config value at {path}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

