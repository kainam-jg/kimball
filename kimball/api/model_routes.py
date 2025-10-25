"""
KIMBALL Model Phase API Routes

This module provides FastAPI routes for the Model phase:
- ERD generation and editing
- Hierarchy modeling
- Star schema design
- Schema transformation
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import json
from datetime import datetime

from ..model.erd_generator import ERDGenerator
from ..model.hierarchy_modeler import HierarchyModeler
from ..model.star_schema_designer import StarSchemaDesigner
from ..model.schema_transformer import SchemaTransformer
from ..core.logger import Logger

# Initialize router
model_router = APIRouter()
logger = Logger()

# Pydantic models for request/response
class ERDRequest(BaseModel):
    """Request model for ERD generation."""
    catalog_id: str
    include_relationships: bool = True
    include_attributes: bool = True

class HierarchyRequest(BaseModel):
    """Request model for hierarchy modeling."""
    catalog_id: str
    hierarchy_config: Dict[str, Any]

class StarSchemaRequest(BaseModel):
    """Request model for star schema design."""
    catalog_id: str
    fact_tables: List[str]
    dimension_tables: List[str]
    relationships: List[Dict[str, Any]]

@model_router.post("/erd/generate")
async def generate_erd(request: ERDRequest):
    """
    Generate Entity Relationship Diagram from catalog.
    
    This endpoint creates an ERD based on the discovered metadata
    and allows for interactive editing.
    """
    try:
        logger.log_api_call("/model/erd/generate", "POST")
        
        # Initialize ERD generator
        erd_generator = ERDGenerator()
        
        # Generate ERD
        erd_result = erd_generator.generate_erd(
            catalog_id=request.catalog_id,
            include_relationships=request.include_relationships,
            include_attributes=request.include_attributes
        )
        
        return {
            "status": "success",
            "erd_id": erd_result["erd_id"],
            "entities": erd_result["entities"],
            "relationships": erd_result["relationships"],
            "message": "ERD generated successfully"
        }
        
    except Exception as e:
        logger.error(f"Error generating ERD: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/hierarchy/model")
async def model_hierarchies(request: HierarchyRequest):
    """
    Model dimensional hierarchies from catalog.
    
    This endpoint discovers and models hierarchical relationships
    following OLAP standards.
    """
    try:
        logger.log_api_call("/model/hierarchy/model", "POST")
        
        # Initialize hierarchy modeler
        hierarchy_modeler = HierarchyModeler()
        
        # Model hierarchies
        hierarchy_result = hierarchy_modeler.model_hierarchies(
            catalog_id=request.catalog_id,
            config=request.hierarchy_config
        )
        
        return {
            "status": "success",
            "hierarchy_id": hierarchy_result["hierarchy_id"],
            "hierarchies": hierarchy_result["hierarchies"],
            "levels": hierarchy_result["levels"],
            "message": "Hierarchies modeled successfully"
        }
        
    except Exception as e:
        logger.error(f"Error modeling hierarchies: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/star-schema/design")
async def design_star_schema(request: StarSchemaRequest):
    """
    Design star schema for data warehouse.
    
    This endpoint creates a star schema design with fact and dimension tables
    based on the discovered metadata and relationships.
    """
    try:
        logger.log_api_call("/model/star-schema/design", "POST")
        
        # Initialize star schema designer
        star_designer = StarSchemaDesigner()
        
        # Design star schema
        star_result = star_designer.design_schema(
            catalog_id=request.catalog_id,
            fact_tables=request.fact_tables,
            dimension_tables=request.dimension_tables,
            relationships=request.relationships
        )
        
        return {
            "status": "success",
            "schema_id": star_result["schema_id"],
            "fact_tables": star_result["fact_tables"],
            "dimension_tables": star_result["dimension_tables"],
            "relationships": star_result["relationships"],
            "message": "Star schema designed successfully"
        }
        
    except Exception as e:
        logger.error(f"Error designing star schema: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/transform/silver")
async def transform_to_silver(catalog_id: str, erd_id: str):
    """
    Transform bronze schema to silver layer (3NF).
    
    This endpoint generates the transformation logic to convert
    bronze layer data into normalized silver layer.
    """
    try:
        logger.log_api_call("/model/transform/silver", "POST")
        
        # Initialize schema transformer
        transformer = SchemaTransformer()
        
        # Transform to silver
        silver_result = transformer.transform_to_silver(
            catalog_id=catalog_id,
            erd_id=erd_id
        )
        
        return {
            "status": "success",
            "transformation_id": silver_result["transformation_id"],
            "sql_scripts": silver_result["sql_scripts"],
            "tables": silver_result["tables"],
            "message": "Silver layer transformation generated successfully"
        }
        
    except Exception as e:
        logger.error(f"Error transforming to silver: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/transform/gold")
async def transform_to_gold(schema_id: str, hierarchy_id: str):
    """
    Transform silver schema to gold layer (star schema).
    
    This endpoint generates the transformation logic to convert
    silver layer data into star schema gold layer.
    """
    try:
        logger.log_api_call("/model/transform/gold", "POST")
        
        # Initialize schema transformer
        transformer = SchemaTransformer()
        
        # Transform to gold
        gold_result = transformer.transform_to_gold(
            schema_id=schema_id,
            hierarchy_id=hierarchy_id
        )
        
        return {
            "status": "success",
            "transformation_id": gold_result["transformation_id"],
            "sql_scripts": gold_result["sql_scripts"],
            "fact_tables": gold_result["fact_tables"],
            "dimension_tables": gold_result["dimension_tables"],
            "message": "Gold layer transformation generated successfully"
        }
        
    except Exception as e:
        logger.error(f"Error transforming to gold: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.get("/models")
async def list_models():
    """
    List all available models.
    """
    try:
        logger.log_api_call("/model/models", "GET")
        
        # Return available model types
        models = {
            "erd": "Entity Relationship Diagrams",
            "hierarchy": "Dimensional Hierarchies", 
            "star_schema": "Star Schema Designs",
            "silver": "Silver Layer (3NF) Models",
            "gold": "Gold Layer (Star Schema) Models"
        }
        
        return {"models": models}
        
    except Exception as e:
        logger.error(f"Error listing models: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
