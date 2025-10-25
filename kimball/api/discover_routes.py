"""
KIMBALL Discover Phase API Routes

This module provides FastAPI routes for the Discover phase:
- Metadata analysis
- Catalog generation
- Data quality assessment
- Relationship discovery
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import json
from datetime import datetime

from ..discover.metadata_analyzer import MetadataAnalyzer
from ..discover.catalog_builder import CatalogBuilder
from ..discover.quality_assessor import QualityAssessor
from ..discover.relationship_finder import RelationshipFinder
from ..core.logger import Logger

# Initialize router
discover_router = APIRouter()
logger = Logger()

# Pydantic models for request/response
class DiscoverRequest(BaseModel):
    """Request model for discover operations."""
    schema_name: str = "bronze"
    include_quality: bool = True
    include_relationships: bool = True
    include_hierarchies: bool = True

class DiscoverResponse(BaseModel):
    """Response model for discover operations."""
    status: str
    message: str
    catalog_id: Optional[str] = None
    analysis_timestamp: str
    total_tables: int
    total_columns: int
    fact_columns: int
    dimension_columns: int

class QualityReport(BaseModel):
    """Data quality report model."""
    overall_score: float
    high_quality_tables: int
    medium_quality_tables: int
    low_quality_tables: int
    issues: List[Dict[str, Any]]

class RelationshipReport(BaseModel):
    """Relationship discovery report model."""
    total_relationships: int
    high_confidence_joins: int
    primary_key_candidates: int
    relationships: List[Dict[str, Any]]

@discover_router.post("/analyze", response_model=DiscoverResponse)
async def analyze_schema(request: DiscoverRequest):
    """
    Analyze a schema and generate metadata catalog.
    
    This endpoint performs comprehensive analysis of the bronze schema,
    including metadata discovery, quality assessment, and relationship analysis.
    """
    try:
        logger.log_api_call("/discover/analyze", "POST")
        
        # Initialize analyzer
        analyzer = MetadataAnalyzer()
        
        # Connect to database
        if not analyzer.connect():
            raise HTTPException(status_code=500, detail="Failed to connect to database")
        
        # Perform analysis
        catalog = analyzer.build_catalog(request.schema_name)
        
        # Generate catalog ID
        catalog_id = f"catalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Save catalog
        catalog_filename = f"metadata_catalog_{catalog_id}.json"
        analyzer.save_catalog(catalog, catalog_filename)
        
        # Disconnect
        analyzer.disconnect()
        
        # Prepare response
        schema_summary = catalog.get('schema_summary', {})
        
        response = DiscoverResponse(
            status="success",
            message="Schema analysis completed successfully",
            catalog_id=catalog_id,
            analysis_timestamp=catalog.get('analysis_timestamp', datetime.now().isoformat()),
            total_tables=catalog.get('total_tables', 0),
            total_columns=schema_summary.get('total_columns', 0),
            fact_columns=schema_summary.get('total_fact_columns', 0),
            dimension_columns=schema_summary.get('total_dimension_columns', 0)
        )
        
        logger.log_phase_complete("discover", catalog_id=catalog_id)
        return response
        
    except Exception as e:
        logger.error(f"Error in schema analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.get("/catalog/{catalog_id}")
async def get_catalog(catalog_id: str):
    """
    Retrieve a specific metadata catalog by ID.
    """
    try:
        logger.log_api_call(f"/discover/catalog/{catalog_id}", "GET")
        
        catalog_filename = f"metadata_catalog_{catalog_id}.json"
        
        try:
            with open(catalog_filename, 'r') as f:
                catalog = json.load(f)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Catalog not found")
        
        return catalog
        
    except Exception as e:
        logger.error(f"Error retrieving catalog: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.get("/catalogs")
async def list_catalogs():
    """
    List all available metadata catalogs.
    """
    try:
        logger.log_api_call("/discover/catalogs", "GET")
        
        import glob
        catalog_files = glob.glob("metadata_catalog_*.json")
        catalogs = []
        
        for file in catalog_files:
            try:
                with open(file, 'r') as f:
                    catalog = json.load(f)
                    catalogs.append({
                        "catalog_id": file.replace("metadata_catalog_", "").replace(".json", ""),
                        "schema_name": catalog.get('schema_name', 'unknown'),
                        "analysis_timestamp": catalog.get('analysis_timestamp', ''),
                        "total_tables": catalog.get('total_tables', 0)
                    })
            except Exception as e:
                logger.warning(f"Error reading catalog file {file}: {str(e)}")
        
        return {"catalogs": catalogs}
        
    except Exception as e:
        logger.error(f"Error listing catalogs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.post("/quality", response_model=QualityReport)
async def assess_quality(catalog_id: str):
    """
    Assess data quality for a specific catalog.
    """
    try:
        logger.log_api_call(f"/discover/quality/{catalog_id}", "POST")
        
        # Load catalog
        catalog_filename = f"metadata_catalog_{catalog_id}.json"
        try:
            with open(catalog_filename, 'r') as f:
                catalog = json.load(f)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Catalog not found")
        
        # Assess quality
        assessor = QualityAssessor()
        quality_report = assessor.assess_catalog_quality(catalog)
        
        return quality_report
        
    except Exception as e:
        logger.error(f"Error assessing quality: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.post("/relationships", response_model=RelationshipReport)
async def find_relationships(catalog_id: str):
    """
    Discover relationships for a specific catalog.
    """
    try:
        logger.log_api_call(f"/discover/relationships/{catalog_id}", "POST")
        
        # Load catalog
        catalog_filename = f"metadata_catalog_{catalog_id}.json"
        try:
            with open(catalog_filename, 'r') as f:
                catalog = json.load(f)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Catalog not found")
        
        # Find relationships
        finder = RelationshipFinder()
        relationship_report = finder.find_relationships(catalog)
        
        return relationship_report
        
    except Exception as e:
        logger.error(f"Error finding relationships: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.get("/summary/{catalog_id}")
async def get_summary(catalog_id: str):
    """
    Get a summary report for a specific catalog.
    """
    try:
        logger.log_api_call(f"/discover/summary/{catalog_id}", "GET")
        
        # Load catalog
        catalog_filename = f"metadata_catalog_{catalog_id}.json"
        try:
            with open(catalog_filename, 'r') as f:
                catalog = json.load(f)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Catalog not found")
        
        # Generate summary
        builder = CatalogBuilder()
        summary = builder.generate_summary(catalog)
        
        return summary
        
    except Exception as e:
        logger.error(f"Error generating summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
