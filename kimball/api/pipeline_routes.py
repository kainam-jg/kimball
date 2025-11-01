"""
KIMBALL Pipeline API Routes

This module provides API endpoints for orchestrating full data pipelines across all transformation stages.
Pipelines coordinate the execution of transformations from stage0 through stage4.

Features:
- Pipeline definition and management
- Time-based scheduling (daily, hourly, weekly, etc.)
- Event-based scheduling (manual triggers via API)
- Pipeline execution orchestration
- Stage dependency management
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel
from enum import Enum

from ..core.logger import Logger
from ..core.scheduler import SchedulerService

# Initialize router
pipeline_router = APIRouter(prefix="/api/v1/pipeline", tags=["Pipeline"])
logger = Logger("pipeline_api")

# Initialize scheduler service
scheduler_service = SchedulerService()

# Enums for pipeline scheduling
class ScheduleType(str, Enum):
    """Types of pipeline schedules."""
    TIME_BASED = "time_based"
    EVENT_BASED = "event_based"
    MANUAL = "manual"

class ExecutionFrequency(str, Enum):
    """Execution frequency options."""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ON_DEMAND = "on_demand"

# Pydantic models for request bodies
class PipelineCreateRequest(BaseModel):
    """Request model for creating a new pipeline."""
    pipeline_name: str
    description: Optional[str] = ""
    schedule_type: ScheduleType = ScheduleType.MANUAL
    execution_frequency: Optional[ExecutionFrequency] = None
    cron_expression: Optional[str] = None  # For advanced scheduling
    enabled: bool = True
    stage_configs: Dict[str, Any] = {}  # Configuration for each stage

class PipelineUpdateRequest(BaseModel):
    """Request model for updating an existing pipeline."""
    pipeline_name: Optional[str] = None
    description: Optional[str] = None
    schedule_type: Optional[ScheduleType] = None
    execution_frequency: Optional[ExecutionFrequency] = None
    cron_expression: Optional[str] = None
    enabled: Optional[bool] = None
    stage_configs: Optional[Dict[str, Any]] = None

class PipelineTriggerRequest(BaseModel):
    """Request model for manually triggering a pipeline."""
    pipeline_id: Optional[int] = None
    pipeline_name: Optional[str] = None
    execute_stages: Optional[List[str]] = None  # If None, execute all stages
    force: bool = False  # Force execution even if disabled

# Pipeline Status Endpoint
@pipeline_router.get("/status")
async def get_pipeline_status():
    """Get overall status of the Pipeline orchestration system."""
    try:
        logger.log_api_call("/pipeline/status", "GET")
        
        return {
            "status": "success",
            "service": "Pipeline Orchestration",
            "features": {
                "time_based_scheduling": "prepared",
                "event_based_scheduling": "active",
                "pipeline_management": "active",
                "stage_orchestration": "prepared"
            },
            "supported_schedule_types": [st.value for st in ScheduleType],
            "supported_frequencies": [ef.value for ef in ExecutionFrequency],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting pipeline status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Pipeline Management Endpoints (Structure created, full implementation deferred)
@pipeline_router.post("/pipelines")
async def create_pipeline(request: PipelineCreateRequest):
    """Create a new pipeline definition."""
    try:
        logger.log_api_call("/pipeline/pipelines", "POST", request_data=request.dict())
        
        # TODO: Implement pipeline creation in metadata table
        # For now, return structure
        return {
            "status": "success",
            "message": "Pipeline creation endpoint prepared",
            "note": "Full pipeline orchestration will be implemented after all stages are built",
            "request": request.dict(),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error creating pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@pipeline_router.get("/pipelines")
async def list_pipelines():
    """List all pipeline definitions."""
    try:
        logger.log_api_call("/pipeline/pipelines", "GET")
        
        # TODO: Implement pipeline listing from metadata
        return {
            "status": "success",
            "pipelines": [],
            "count": 0,
            "note": "Pipeline management will be fully implemented after all stages are built",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error listing pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@pipeline_router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: int):
    """Get a specific pipeline definition."""
    try:
        logger.log_api_call(f"/pipeline/pipelines/{pipeline_id}", "GET")
        
        # TODO: Implement pipeline retrieval
        raise HTTPException(status_code=501, detail="Pipeline retrieval not yet implemented")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@pipeline_router.put("/pipelines/{pipeline_id}")
async def update_pipeline(pipeline_id: int, request: PipelineUpdateRequest):
    """Update an existing pipeline definition."""
    try:
        logger.log_api_call(f"/pipeline/pipelines/{pipeline_id}", "PUT", request_data=request.dict())
        
        # TODO: Implement pipeline update
        raise HTTPException(status_code=501, detail="Pipeline update not yet implemented")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@pipeline_router.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(pipeline_id: int):
    """Delete a pipeline definition."""
    try:
        logger.log_api_call(f"/pipeline/pipelines/{pipeline_id}", "DELETE")
        
        # TODO: Implement pipeline deletion
        raise HTTPException(status_code=501, detail="Pipeline deletion not yet implemented")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Event-Based Scheduling - Trigger Pipeline Execution
@pipeline_router.post("/trigger")
async def trigger_pipeline(request: PipelineTriggerRequest):
    """
    Manually trigger a pipeline execution (event-based scheduling).
    
    This endpoint allows on-demand execution of pipelines without waiting for scheduled time.
    """
    try:
        logger.log_api_call("/pipeline/trigger", "POST", request_data=request.dict())
        
        # TODO: Full pipeline orchestration will be implemented later
        # For now, return structure showing how it will work
        
        pipeline_identifier = request.pipeline_id or request.pipeline_name
        if not pipeline_identifier:
            raise HTTPException(
                status_code=400,
                detail="Either pipeline_id or pipeline_name must be provided"
            )
        
        stages_to_execute = request.execute_stages or ["stage0", "stage1", "stage2", "stage3", "stage4"]
        
        return {
            "status": "success",
            "message": f"Pipeline trigger endpoint prepared for {pipeline_identifier}",
            "note": "Full pipeline orchestration will execute stages in order: stage0 → stage1 → stage2 → stage3 → stage4",
            "requested_stages": stages_to_execute,
            "force": request.force,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Time-Based Scheduling Support
@pipeline_router.get("/schedule/time-based")
async def get_time_based_schedules():
    """
    Get all time-based scheduled Data Contracts grouped by frequency.
    
    Returns contracts from metadata.transformation0 that have execution_frequency set.
    """
    try:
        logger.log_api_call("/pipeline/schedule/time-based", "GET")
        
        # Get all scheduled contracts grouped by frequency
        grouped_contracts = scheduler_service.get_all_scheduled_contracts()
        
        # Get total counts
        total_count = sum(len(contracts) for contracts in grouped_contracts.values())
        
        return {
            "status": "success",
            "scheduled_contracts": grouped_contracts,
            "total_scheduled": total_count,
            "by_frequency": {
                freq: len(contracts) for freq, contracts in grouped_contracts.items()
            },
            "note": "Scheduler service will automatically execute contracts based on execution_frequency",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting time-based schedules: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@pipeline_router.post("/schedule/time-based/execute/{frequency}")
async def execute_scheduled_contracts(frequency: str):
    """
    Manually trigger execution of all contracts scheduled for a specific frequency.
    
    This is useful for testing or on-demand execution of scheduled contracts.
    The scheduler daemon will call this automatically based on time intervals.
    
    Args:
        frequency: Execution frequency to execute (hourly, daily, weekly, monthly)
    """
    try:
        logger.log_api_call(f"/pipeline/schedule/time-based/execute/{frequency}", "POST")
        
        # Validate frequency
        valid_frequencies = ["hourly", "daily", "weekly", "monthly"]
        if frequency.lower() not in valid_frequencies:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid frequency '{frequency}'. Must be one of: {valid_frequencies}"
            )
        
        # Execute all contracts for this frequency
        result = await scheduler_service.execute_scheduled_contracts(frequency.lower())
        
        return {
            "status": "success",
            "execution_result": result,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing scheduled contracts for {frequency}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@pipeline_router.post("/schedule/time-based/register")
async def register_time_based_schedule(transformation_id: int):
    """
    Register a Data Contract for time-based scheduling.
    
    The contract's execution_frequency in metadata.transformation0 will be monitored by the scheduler service.
    Note: Contracts are automatically registered when they have execution_frequency set.
    """
    try:
        logger.log_api_call(f"/pipeline/schedule/time-based/register/{transformation_id}", "POST")
        
        # Validate contract exists
        contract = scheduler_service.contract_manager.get_contract(transformation_id)
        if not contract:
            raise HTTPException(
                status_code=404,
                detail=f"Data Contract {transformation_id} not found"
            )
        
        # Check if it has execution_frequency set
        execution_frequency = contract.get('execution_frequency', 'on_demand')
        
        if execution_frequency == 'on_demand':
            return {
                "status": "success",
                "message": f"Data Contract {transformation_id} is set to on_demand",
                "note": "Set execution_frequency to hourly, daily, weekly, or monthly for automatic scheduling",
                "current_frequency": execution_frequency,
                "transformation_id": transformation_id,
                "timestamp": datetime.now().isoformat()
            }
        
        return {
            "status": "success",
            "message": f"Data Contract {transformation_id} is registered for time-based scheduling",
            "execution_frequency": execution_frequency,
            "note": "Scheduler service will automatically execute this contract based on execution_frequency",
            "transformation_id": transformation_id,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering time-based schedule: {e}")
        raise HTTPException(status_code=500, detail=str(e))

