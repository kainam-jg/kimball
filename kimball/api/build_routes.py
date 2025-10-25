"""
KIMBALL Build Phase API Routes

This module provides FastAPI routes for the Build phase:
- DAG generation
- SQL generation
- Pipeline orchestration
- Monitoring and logging
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import json
from datetime import datetime

from ..build.dag_builder import DAGBuilder
from ..build.sql_generator import SQLGenerator
from ..build.pipeline_orchestrator import PipelineOrchestrator
from ..build.monitor import PipelineMonitor
from ..core.logger import Logger

# Initialize router
build_router = APIRouter()
logger = Logger()

# Pydantic models for request/response
class DAGRequest(BaseModel):
    """Request model for DAG generation."""
    transformation_id: str
    schedule: str = "daily"
    dependencies: List[str] = []
    config: Dict[str, Any] = {}

class SQLRequest(BaseModel):
    """Request model for SQL generation."""
    transformation_id: str
    target_layer: str  # silver, gold
    optimization_level: str = "standard"

class PipelineRequest(BaseModel):
    """Request model for pipeline creation."""
    dag_id: str
    environment: str = "development"
    config: Dict[str, Any] = {}

@build_router.post("/dag/generate")
async def generate_dag(request: DAGRequest):
    """
    Generate Apache Airflow DAG for data pipeline.
    
    This endpoint creates production-ready DAGs for orchestrating
    data transformations and loading processes.
    """
    try:
        logger.log_api_call("/build/dag/generate", "POST")
        
        # Initialize DAG builder
        dag_builder = DAGBuilder()
        
        # Generate DAG
        dag_result = dag_builder.generate_dag(
            transformation_id=request.transformation_id,
            schedule=request.schedule,
            dependencies=request.dependencies,
            config=request.config
        )
        
        return {
            "status": "success",
            "dag_id": dag_result["dag_id"],
            "dag_file": dag_result["dag_file"],
            "tasks": dag_result["tasks"],
            "schedule": request.schedule,
            "message": "DAG generated successfully"
        }
        
    except Exception as e:
        logger.error(f"Error generating DAG: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@build_router.post("/sql/generate")
async def generate_sql(request: SQLRequest):
    """
    Generate SQL transformation scripts.
    
    This endpoint creates optimized SQL scripts for data transformations
    between bronze, silver, and gold layers.
    """
    try:
        logger.log_api_call("/build/sql/generate", "POST")
        
        # Initialize SQL generator
        sql_generator = SQLGenerator()
        
        # Generate SQL
        sql_result = sql_generator.generate_sql(
            transformation_id=request.transformation_id,
            target_layer=request.target_layer,
            optimization_level=request.optimization_level
        )
        
        return {
            "status": "success",
            "sql_id": sql_result["sql_id"],
            "scripts": sql_result["scripts"],
            "target_layer": request.target_layer,
            "optimization_level": request.optimization_level,
            "message": "SQL scripts generated successfully"
        }
        
    except Exception as e:
        logger.error(f"Error generating SQL: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@build_router.post("/pipeline/create")
async def create_pipeline(request: PipelineRequest):
    """
    Create and deploy data pipeline.
    
    This endpoint creates a complete data pipeline with orchestration,
    monitoring, and logging capabilities.
    """
    try:
        logger.log_api_call("/build/pipeline/create", "POST")
        
        # Initialize pipeline orchestrator
        orchestrator = PipelineOrchestrator()
        
        # Create pipeline
        pipeline_result = orchestrator.create_pipeline(
            dag_id=request.dag_id,
            environment=request.environment,
            config=request.config
        )
        
        return {
            "status": "success",
            "pipeline_id": pipeline_result["pipeline_id"],
            "environment": request.environment,
            "status": pipeline_result["status"],
            "message": "Pipeline created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@build_router.post("/pipeline/deploy")
async def deploy_pipeline(pipeline_id: str, environment: str = "production"):
    """
    Deploy pipeline to target environment.
    
    This endpoint deploys the created pipeline to the specified
    environment with proper configuration and monitoring.
    """
    try:
        logger.log_api_call(f"/build/pipeline/deploy/{pipeline_id}", "POST")
        
        # Initialize pipeline orchestrator
        orchestrator = PipelineOrchestrator()
        
        # Deploy pipeline
        deploy_result = orchestrator.deploy_pipeline(
            pipeline_id=pipeline_id,
            environment=environment
        )
        
        return {
            "status": "success",
            "pipeline_id": pipeline_id,
            "environment": environment,
            "deployment_status": deploy_result["status"],
            "message": "Pipeline deployed successfully"
        }
        
    except Exception as e:
        logger.error(f"Error deploying pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@build_router.get("/pipeline/{pipeline_id}/status")
async def get_pipeline_status(pipeline_id: str):
    """
    Get pipeline execution status.
    
    This endpoint provides real-time status of pipeline execution
    including task status, logs, and performance metrics.
    """
    try:
        logger.log_api_call(f"/build/pipeline/{pipeline_id}/status", "GET")
        
        # Initialize pipeline monitor
        monitor = PipelineMonitor()
        
        # Get pipeline status
        status_result = monitor.get_pipeline_status(pipeline_id)
        
        return {
            "pipeline_id": pipeline_id,
            "status": status_result["status"],
            "tasks": status_result["tasks"],
            "metrics": status_result["metrics"],
            "logs": status_result["logs"]
        }
        
    except Exception as e:
        logger.error(f"Error getting pipeline status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@build_router.post("/pipeline/{pipeline_id}/trigger")
async def trigger_pipeline(pipeline_id: str):
    """
    Manually trigger pipeline execution.
    
    This endpoint allows manual triggering of pipeline execution
    for testing or on-demand processing.
    """
    try:
        logger.log_api_call(f"/build/pipeline/{pipeline_id}/trigger", "POST")
        
        # Initialize pipeline orchestrator
        orchestrator = PipelineOrchestrator()
        
        # Trigger pipeline
        trigger_result = orchestrator.trigger_pipeline(pipeline_id)
        
        return {
            "status": "success",
            "pipeline_id": pipeline_id,
            "execution_id": trigger_result["execution_id"],
            "message": "Pipeline triggered successfully"
        }
        
    except Exception as e:
        logger.error(f"Error triggering pipeline: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@build_router.get("/pipelines")
async def list_pipelines():
    """
    List all available pipelines.
    """
    try:
        logger.log_api_call("/build/pipelines", "GET")
        
        # Return available pipeline types
        pipelines = {
            "bronze_to_silver": "Bronze to Silver Layer Pipeline",
            "silver_to_gold": "Silver to Gold Layer Pipeline", 
            "full_pipeline": "Complete Bronze to Gold Pipeline",
            "incremental": "Incremental Data Pipeline",
            "real_time": "Real-time Data Pipeline"
        }
        
        return {"pipelines": pipelines}
        
    except Exception as e:
        logger.error(f"Error listing pipelines: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
