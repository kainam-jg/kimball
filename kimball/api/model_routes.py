#!/usr/bin/env python3
"""
KIMBALL Model Phase API Routes

This module provides API endpoints for the Model Phase, including:
- ERD analysis and discovery
- Hierarchy analysis and discovery
- Metadata storage and retrieval
- Dimensional modeling support

Based on the archive analysis code with enhancements for Stage 2 data.
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

from ..model.erd_analyzer import ERDAnalyzer
from ..model.hierarchy_analyzer import HierarchyAnalyzer
from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)

# Create router
model_router = APIRouter(prefix="/api/v1/model", tags=["Model"])

# Pydantic models for request/response
class ERDAnalysisRequest(BaseModel):
    """Request model for ERD analysis."""
    schema_name: str = "silver"
    include_relationships: bool = True
    min_confidence: float = 0.5

class HierarchyAnalysisRequest(BaseModel):
    """Request model for hierarchy analysis."""
    schema_name: str = "silver"
    include_cross_hierarchies: bool = True
    min_confidence: float = 0.5

class MetadataQueryRequest(BaseModel):
    """Request model for metadata queries."""
    table_name: Optional[str] = None
    schema_name: str = "silver"
    limit: int = 100

class MetadataUpdateRequest(BaseModel):
    """Request model for metadata updates."""
    table_name: str
    field_name: str
    new_value: Any
    schema_name: str = "silver"

# Dependency for database manager
def get_db_manager():
    return DatabaseManager()

@model_router.get("/status")
async def get_model_status():
    """
    Get the current status of the Model Phase.
    
    Returns:
        Dict[str, Any]: Model Phase status information
    """
    try:
        db_manager = DatabaseManager()
        
        # Check if metadata tables exist
        erd_table_exists = False
        hierarchy_table_exists = False
        
        try:
            erd_query = "SELECT COUNT(*) as count FROM metadata.erd LIMIT 1"
            db_manager.execute_query(erd_query)
            erd_table_exists = True
        except:
            pass
        
        try:
            hierarchy_query = "SELECT COUNT(*) as count FROM metadata.hierarchies LIMIT 1"
            db_manager.execute_query(hierarchy_query)
            hierarchy_table_exists = True
        except:
            pass
        
        # Get Stage 2 table count
        stage2_query = """
        SELECT COUNT(*) as count 
        FROM system.tables 
        WHERE database = 'silver' 
        AND name LIKE '%_stage2'
        """
        stage2_result = db_manager.execute_query(stage2_query)
        stage2_count = stage2_result[0]['count'] if stage2_result else 0
        
        return {
            "status": "active",
            "phase": "Model",
            "stage2_tables": stage2_count,
            "metadata_tables": {
                "erd": erd_table_exists,
                "hierarchies": hierarchy_table_exists
            },
            "analysis_available": erd_table_exists and hierarchy_table_exists,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting Model Phase status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/erd/analyze")
async def analyze_erd(request: ERDAnalysisRequest):
    """
    Perform ERD analysis on Stage 2 tables.
    
    Args:
        request (ERDAnalysisRequest): ERD analysis parameters
        
    Returns:
        Dict[str, Any]: ERD analysis results
    """
    try:
        logger.info(f"Starting ERD analysis for schema: {request.schema_name}")
        
        # Initialize ERD analyzer
        erd_analyzer = ERDAnalyzer()
        
        # Generate ERD metadata
        erd_metadata = erd_analyzer.generate_erd_metadata()
        
        # Filter relationships by confidence if specified
        if request.min_confidence > 0:
            erd_metadata['relationships'] = [
                rel for rel in erd_metadata['relationships']
                if rel['join_confidence'] >= request.min_confidence
            ]
        
        # Store metadata if analysis was successful
        if erd_metadata['total_tables'] > 0:
            store_success = erd_analyzer.store_erd_metadata(erd_metadata)
            erd_metadata['stored'] = store_success
        else:
            erd_metadata['stored'] = False
        
        return {
            "status": "success",
            "message": "ERD analysis completed",
            "analysis_timestamp": erd_metadata['analysis_timestamp'],
            "total_tables": erd_metadata['total_tables'],
            "total_relationships": erd_metadata['total_relationships'],
            "summary": erd_metadata['summary'],
            "stored": erd_metadata['stored'],
            "metadata": erd_metadata
        }
        
    except Exception as e:
        logger.error(f"Error during ERD analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/hierarchies/analyze")
async def analyze_hierarchies(request: HierarchyAnalysisRequest):
    """
    Perform hierarchy analysis on Stage 2 tables.
    
    Args:
        request (HierarchyAnalysisRequest): Hierarchy analysis parameters
        
    Returns:
        Dict[str, Any]: Hierarchy analysis results
    """
    try:
        logger.info(f"Starting hierarchy analysis for schema: {request.schema_name}")
        
        # Initialize hierarchy analyzer
        hierarchy_analyzer = HierarchyAnalyzer()
        
        # Generate hierarchy metadata
        hierarchy_metadata = hierarchy_analyzer.generate_hierarchy_metadata()
        
        # Filter cross-hierarchy relationships by confidence if specified
        if request.min_confidence > 0:
            hierarchy_metadata['cross_hierarchy_relationships'] = [
                rel for rel in hierarchy_metadata['cross_hierarchy_relationships']
                if rel['relationship_confidence'] >= request.min_confidence
            ]
        
        # Store metadata if analysis was successful
        if hierarchy_metadata['total_hierarchies'] > 0:
            store_success = hierarchy_analyzer.store_hierarchy_metadata(hierarchy_metadata)
            hierarchy_metadata['stored'] = store_success
        else:
            hierarchy_metadata['stored'] = False
        
        return {
            "status": "success",
            "message": "Hierarchy analysis completed",
            "analysis_timestamp": hierarchy_metadata['analysis_timestamp'],
            "total_tables": hierarchy_metadata['total_tables'],
            "total_hierarchies": hierarchy_metadata['total_hierarchies'],
            "total_cross_relationships": hierarchy_metadata['total_cross_relationships'],
            "summary": hierarchy_metadata['summary'],
            "stored": hierarchy_metadata['stored'],
            "metadata": hierarchy_metadata
        }
        
    except Exception as e:
        logger.error(f"Error during hierarchy analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.get("/erd/metadata")
async def get_erd_metadata(table_name: Optional[str] = None, limit: int = 100):
    """
    Get ERD metadata from the metadata.erd table.
    
    Args:
        table_name (Optional[str]): Filter by specific table name
        limit (int): Maximum number of records to return
        
    Returns:
        Dict[str, Any]: ERD metadata
    """
    try:
        db_manager = DatabaseManager()
        
        # Build query
        query = f"""
        SELECT 
            table_name,
            table_type,
            row_count,
            column_count,
            primary_key_candidates,
            fact_columns,
            dimension_columns,
            relationships,
            analysis_timestamp
        FROM metadata.erd
        """
        
        if table_name:
            query += f" WHERE table_name = '{table_name}'"
        
        query += f" ORDER BY analysis_timestamp DESC LIMIT {limit}"
        
        results = db_manager.execute_query(query)
        
        if not results:
            return {
                "status": "success",
                "message": "No ERD metadata found",
                "count": 0,
                "data": []
            }
        
        return {
            "status": "success",
            "message": "ERD metadata retrieved",
            "count": len(results),
            "data": results
        }
        
    except Exception as e:
        logger.error(f"Error retrieving ERD metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.get("/hierarchies/metadata")
async def get_hierarchy_metadata(table_name: Optional[str] = None, limit: int = 100):
    """
    Get hierarchy metadata from the metadata.hierarchies table.
    
    Args:
        table_name (Optional[str]): Filter by specific table name
        limit (int): Maximum number of records to return
        
    Returns:
        Dict[str, Any]: Hierarchy metadata
    """
    try:
        db_manager = DatabaseManager()
        
        # Build query
        query = f"""
        SELECT 
            table_name,
            original_table_name,
            hierarchy_name,
            total_levels,
            root_column,
            root_cardinality,
            leaf_column,
            leaf_cardinality,
            intermediate_levels,
            parent_child_relationships,
            sibling_relationships,
            cross_hierarchy_relationships,
            analysis_timestamp
        FROM metadata.hierarchies
        """
        
        if table_name:
            query += f" WHERE table_name = '{table_name}'"
        
        query += f" ORDER BY analysis_timestamp DESC LIMIT {limit}"
        
        results = db_manager.execute_query(query)
        
        if not results:
            return {
                "status": "success",
                "message": "No hierarchy metadata found",
                "count": 0,
                "data": []
            }
        
        return {
            "status": "success",
            "message": "Hierarchy metadata retrieved",
            "count": len(results),
            "data": results
        }
        
    except Exception as e:
        logger.error(f"Error retrieving hierarchy metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.get("/erd/relationships")
async def get_erd_relationships(min_confidence: float = 0.5, limit: int = 100):
    """
    Get ERD relationships with confidence filtering.
    
    Args:
        min_confidence (float): Minimum confidence threshold
        limit (int): Maximum number of relationships to return
        
    Returns:
        Dict[str, Any]: ERD relationships
    """
    try:
        db_manager = DatabaseManager()
        
        # Get relationships from metadata.discover (if available) or analyze on-the-fly
        query = """
        SELECT 
            original_table_name,
            original_column_name,
            new_column_name,
            inferred_type,
            classification,
            cardinality
        FROM metadata.discover
        WHERE classification = 'dimension'
        ORDER BY original_table_name, cardinality
        """
        
        results = db_manager.execute_query(query)
        
        if not results:
            return {
                "status": "success",
                "message": "No relationship data found",
                "count": 0,
                "relationships": []
            }
        
        # Group by table and find potential relationships
        relationships = []
        table_columns = {}
        
        for row in results:
            table_name = row['original_table_name']
            if table_name not in table_columns:
                table_columns[table_name] = []
            table_columns[table_name].append(row)
        
        # Find relationships within tables (hierarchies)
        for table_name, columns in table_columns.items():
            if len(columns) > 1:
                # Sort by cardinality
                sorted_columns = sorted(columns, key=lambda x: x['cardinality'])
                
                # Create hierarchy relationships
                for i in range(len(sorted_columns) - 1):
                    parent = sorted_columns[i]
                    child = sorted_columns[i + 1]
                    
                    confidence = min(parent['cardinality'], child['cardinality']) / max(parent['cardinality'], child['cardinality']) if max(parent['cardinality'], child['cardinality']) > 0 else 0
                    
                    if confidence >= min_confidence:
                        relationships.append({
                            'table1': table_name,
                            'column1': parent['new_column_name'],
                            'table2': table_name,
                            'column2': child['new_column_name'],
                            'relationship_type': 'hierarchy',
                            'confidence': confidence,
                            'parent_cardinality': parent['cardinality'],
                            'child_cardinality': child['cardinality']
                        })
        
        # Sort by confidence and limit
        relationships.sort(key=lambda x: x['confidence'], reverse=True)
        relationships = relationships[:limit]
        
        return {
            "status": "success",
            "message": "ERD relationships retrieved",
            "count": len(relationships),
            "relationships": relationships
        }
        
    except Exception as e:
        logger.error(f"Error retrieving ERD relationships: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.get("/hierarchies/levels")
async def get_hierarchy_levels(table_name: Optional[str] = None):
    """
    Get hierarchy level information for dimensional modeling.
    
    Args:
        table_name (Optional[str]): Filter by specific table name
        
    Returns:
        Dict[str, Any]: Hierarchy level information
    """
    try:
        db_manager = DatabaseManager()
        
        # Get hierarchy data from metadata.discover
        query = """
        SELECT 
            original_table_name,
            original_column_name,
            new_column_name,
            inferred_type,
            classification,
            cardinality,
            null_count
        FROM metadata.discover
        WHERE classification = 'dimension'
        """
        
        if table_name:
            query += f" AND original_table_name = '{table_name}'"
        
        query += " ORDER BY original_table_name, cardinality ASC"
        
        results = db_manager.execute_query(query)
        
        if not results:
            return {
                "status": "success",
                "message": "No hierarchy data found",
                "count": 0,
                "hierarchies": {}
            }
        
        # Group by table and build hierarchy levels
        hierarchies = {}
        current_table = None
        current_hierarchy = None
        
        for row in results:
            table_name = row['original_table_name']
            
            if table_name != current_table:
                if current_hierarchy:
                    hierarchies[current_table] = current_hierarchy
                
                current_table = table_name
                current_hierarchy = {
                    'table_name': table_name,
                    'levels': [],
                    'total_levels': 0,
                    'root_column': None,
                    'leaf_column': None
                }
            
            level_info = {
                'column_name': row['new_column_name'],
                'original_column_name': row['original_column_name'],
                'data_type': row['inferred_type'],
                'cardinality': row['cardinality'],
                'null_count': row['null_count'],
                'level': len(current_hierarchy['levels'])
            }
            
            current_hierarchy['levels'].append(level_info)
            current_hierarchy['total_levels'] += 1
            
            # Set root and leaf
            if current_hierarchy['total_levels'] == 1:
                current_hierarchy['root_column'] = row['new_column_name']
            current_hierarchy['leaf_column'] = row['new_column_name']
        
        # Add the last hierarchy
        if current_hierarchy:
            hierarchies[current_table] = current_hierarchy
        
        return {
            "status": "success",
            "message": "Hierarchy levels retrieved",
            "count": len(hierarchies),
            "hierarchies": hierarchies
        }
        
    except Exception as e:
        logger.error(f"Error retrieving hierarchy levels: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/analyze/all")
async def analyze_all():
    """
    Perform both ERD and hierarchy analysis in sequence.
    
    Returns:
        Dict[str, Any]: Combined analysis results
    """
    try:
        logger.info("Starting comprehensive Model Phase analysis")
        
        # Perform ERD analysis
        erd_request = ERDAnalysisRequest()
        erd_result = await analyze_erd(erd_request)
        
        # Perform hierarchy analysis
        hierarchy_request = HierarchyAnalysisRequest()
        hierarchy_result = await analyze_hierarchies(hierarchy_request)
        
        return {
            "status": "success",
            "message": "Comprehensive Model Phase analysis completed",
            "analysis_timestamp": datetime.now().isoformat(),
            "erd_analysis": erd_result,
            "hierarchy_analysis": hierarchy_result,
            "summary": {
                "erd_tables": erd_result.get("total_tables", 0),
                "erd_relationships": erd_result.get("total_relationships", 0),
                "hierarchy_tables": hierarchy_result.get("total_tables", 0),
                "hierarchies": hierarchy_result.get("total_hierarchies", 0),
                "cross_relationships": hierarchy_result.get("total_cross_relationships", 0)
            }
        }
        
    except Exception as e:
        logger.error(f"Error during comprehensive analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))