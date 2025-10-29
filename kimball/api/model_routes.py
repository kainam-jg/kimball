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
from ..model.calendar_generator import CalendarGenerator
from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)

# Create router
model_router = APIRouter(prefix="/api/v1/model", tags=["Model"])

# Pydantic models for request/response
class ERDAnalysisRequest(BaseModel):
    """Request model for ERD analysis."""
    schema_name: str = "silver"
    include_relationships: bool = True
    min_confidence: float = 0.8

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

class ERDRelationshipEditRequest(BaseModel):
    """Request model for editing ERD relationships."""
    table_name: str
    relationship_id: Optional[str] = None
    table_type: Optional[str] = None
    primary_key_candidates: Optional[List[str]] = None
    fact_columns: Optional[List[str]] = None
    dimension_columns: Optional[List[str]] = None
    relationships: Optional[List[Dict[str, Any]]] = None

class HierarchyEditRequest(BaseModel):
    """Request model for editing hierarchies."""
    table_name: str
    hierarchy_name: Optional[str] = None
    root_column: Optional[str] = None
    leaf_column: Optional[str] = None
    intermediate_levels: Optional[List[Dict[str, Any]]] = None
    parent_child_relationships: Optional[List[Dict[str, Any]]] = None
    sibling_relationships: Optional[List[Dict[str, Any]]] = None
    cross_hierarchy_relationships: Optional[List[Dict[str, Any]]] = None

class HierarchyCreateRequest(BaseModel):
    """Request model for creating custom hierarchies."""
    table_name: str
    hierarchy_name: str
    levels: List[Dict[str, Any]]  # List of level definitions
    description: Optional[str] = None

class ERDRelationshipCreateRequest(BaseModel):
    """Request model for creating custom ERD relationships."""
    table1: str
    column1: str
    table2: str
    column2: str
    relationship_type: str  # 'foreign_key', 'hierarchy', 'join', etc.
    confidence: float
    description: Optional[str] = None

class CalendarGenerationRequest(BaseModel):
    """Request model for calendar dimension generation."""
    start_date: str  # Format: YYYY-MM-DD
    end_date: str    # Format: YYYY-MM-DD

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
        
        # Get Stage 1 table count
        stage1_query = """
        SELECT COUNT(*) as count 
        FROM system.tables 
        WHERE database = 'silver' 
        AND name LIKE '%_stage1'
        """
        stage1_result = db_manager.execute_query_dict(stage1_query)
        stage1_count = stage1_result[0]['count'] if stage1_result else 0
        
        return {
            "status": "active",
            "phase": "Model",
            "stage1_tables": stage1_count,
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
        
        # Truncate ERD metadata table before analysis
        db_manager = DatabaseManager()
        try:
            db_manager.execute_command("TRUNCATE TABLE metadata.erd")
            logger.info("Truncated metadata.erd table")
        except Exception as e:
            logger.warning(f"Could not truncate metadata.erd table: {e}")
        
        # Initialize ERD analyzer
        erd_analyzer = ERDAnalyzer()
        
        # Generate ERD metadata with confidence threshold (default 0.8)
        erd_metadata = erd_analyzer.generate_erd_metadata(confidence_threshold=request.min_confidence)
        
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
        
        # Truncate hierarchy metadata table before analysis
        db_manager = DatabaseManager()
        try:
            success = db_manager.execute_command("TRUNCATE TABLE metadata.hierarchies")
            if success:
                logger.info("Truncated metadata.hierarchies table")
            else:
                logger.warning("Failed to truncate metadata.hierarchies table")
        except Exception as e:
            logger.warning(f"Could not truncate metadata.hierarchies table: {e}")
        
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

@model_router.put("/erd/edit")
async def edit_erd_relationship(request: ERDRelationshipEditRequest):
    """
    Edit ERD relationship metadata for a specific table.
    
    Args:
        request: ERDRelationshipEditRequest containing table and relationship updates
        
    Returns:
        Dict[str, Any]: Updated ERD metadata
    """
    try:
        db_manager = DatabaseManager()
        
        # Check if ERD metadata exists for this table
        check_query = f"""
        SELECT table_name, version
        FROM metadata.erd
        WHERE table_name = '{request.table_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        existing = db_manager.execute_query(check_query)
        if not existing:
            raise HTTPException(status_code=404, detail=f"ERD metadata for table '{request.table_name}' not found")
        
        existing_record = existing[0]
        current_version = existing_record["version"]
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Build update fields
        update_fields = []
        if request.table_type is not None:
            update_fields.append(f"table_type = '{request.table_type}'")
        if request.primary_key_candidates is not None:
            update_fields.append(f"primary_key_candidates = {repr(request.primary_key_candidates)}")
        if request.fact_columns is not None:
            update_fields.append(f"fact_columns = {repr(request.fact_columns)}")
        if request.dimension_columns is not None:
            update_fields.append(f"dimension_columns = {repr(request.dimension_columns)}")
        if request.relationships is not None:
            update_fields.append(f"relationships = {repr(request.relationships)}")
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields provided for update")
        
        # Insert updated record (upsert)
        insert_sql = f"""
        INSERT INTO metadata.erd (
            table_name, table_type, row_count, column_count,
            primary_key_candidates, fact_columns, dimension_columns,
            relationships, analysis_timestamp, version
        ) VALUES (
            '{request.table_name}',
            '{request.table_type or 'unknown'}',
            0, 0,
            {repr(request.primary_key_candidates) if request.primary_key_candidates else "'[]'"},
            {repr(request.fact_columns) if request.fact_columns else "'[]'"},
            {repr(request.dimension_columns) if request.dimension_columns else "'[]'"},
            {repr(request.relationships) if request.relationships else "'[]'"},
            '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
            {new_version}
        )
        """
        
        db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"ERD metadata updated for table '{request.table_name}'",
            "table_name": request.table_name,
            "version": new_version,
            "updated_fields": [field.split(' = ')[0] for field in update_fields]
        }
        
    except Exception as e:
        logger.error(f"Error editing ERD relationship: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.put("/hierarchies/edit")
async def edit_hierarchy(request: HierarchyEditRequest):
    """
    Edit hierarchy metadata for a specific table.
    
    Args:
        request: HierarchyEditRequest containing hierarchy updates
        
    Returns:
        Dict[str, Any]: Updated hierarchy metadata
    """
    try:
        db_manager = DatabaseManager()
        
        # Check if hierarchy metadata exists for this table
        check_query = f"""
        SELECT table_name, version
        FROM metadata.hierarchies
        WHERE table_name = '{request.table_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        existing = db_manager.execute_query(check_query)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Hierarchy metadata for table '{request.table_name}' not found")
        
        existing_record = existing[0]
        current_version = existing_record["version"]
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Build update fields
        update_fields = []
        if request.hierarchy_name is not None:
            update_fields.append(f"hierarchy_name = '{request.hierarchy_name}'")
        if request.root_column is not None:
            update_fields.append(f"root_column = '{request.root_column}'")
        if request.leaf_column is not None:
            update_fields.append(f"leaf_column = '{request.leaf_column}'")
        if request.intermediate_levels is not None:
            update_fields.append(f"intermediate_levels = {repr(request.intermediate_levels)}")
        if request.parent_child_relationships is not None:
            update_fields.append(f"parent_child_relationships = {repr(request.parent_child_relationships)}")
        if request.sibling_relationships is not None:
            update_fields.append(f"sibling_relationships = {repr(request.sibling_relationships)}")
        if request.cross_hierarchy_relationships is not None:
            update_fields.append(f"cross_hierarchy_relationships = {repr(request.cross_hierarchy_relationships)}")
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields provided for update")
        
        # Insert updated record (upsert)
        insert_sql = f"""
        INSERT INTO metadata.hierarchies (
            table_name, original_table_name, hierarchy_name, total_levels,
            root_column, root_cardinality, leaf_column, leaf_cardinality,
            intermediate_levels, parent_child_relationships, sibling_relationships,
            cross_hierarchy_relationships, analysis_timestamp, version
        ) VALUES (
            '{request.table_name}',
            '{request.table_name}',
            '{request.hierarchy_name or 'custom_hierarchy'}',
            0,
            '{request.root_column or ''}',
            0,
            '{request.leaf_column or ''}',
            0,
            {repr(request.intermediate_levels) if request.intermediate_levels else "'[]'"},
            {repr(request.parent_child_relationships) if request.parent_child_relationships else "'[]'"},
            {repr(request.sibling_relationships) if request.sibling_relationships else "'[]'"},
            {repr(request.cross_hierarchy_relationships) if request.cross_hierarchy_relationships else "'[]'"},
            '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
            {new_version}
        )
        """
        
        db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"Hierarchy metadata updated for table '{request.table_name}'",
            "table_name": request.table_name,
            "version": new_version,
            "updated_fields": [field.split(' = ')[0] for field in update_fields]
        }
        
    except Exception as e:
        logger.error(f"Error editing hierarchy: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/hierarchies/create")
async def create_hierarchy(request: HierarchyCreateRequest):
    """
    Create a custom hierarchy for a specific table.
    
    Args:
        request: HierarchyCreateRequest containing hierarchy definition
        
    Returns:
        Dict[str, Any]: Created hierarchy metadata
    """
    try:
        db_manager = DatabaseManager()
        
        # Validate that the table exists in Stage 2
        table_check = f"""
        SELECT name FROM system.tables 
        WHERE database = 'silver' AND name LIKE '%_stage2'
        AND name = '{request.table_name}_stage2'
        """
        
        table_exists = db_manager.execute_query(table_check)
        if not table_exists:
            raise HTTPException(status_code=404, detail=f"Stage 2 table '{request.table_name}_stage2' not found")
        
        # Calculate hierarchy properties
        total_levels = len(request.levels)
        root_column = request.levels[0]['column_name'] if request.levels else None
        leaf_column = request.levels[-1]['column_name'] if request.levels else None
        
        # Build relationships from levels
        parent_child_relationships = []
        for i in range(len(request.levels) - 1):
            parent_child_relationships.append({
                'parent_column': request.levels[i]['column_name'],
                'child_column': request.levels[i + 1]['column_name'],
                'level': i + 1
            })
        
        # Generate version number
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Insert new hierarchy
        insert_sql = f"""
        INSERT INTO metadata.hierarchies (
            table_name, original_table_name, hierarchy_name, total_levels,
            root_column, root_cardinality, leaf_column, leaf_cardinality,
            intermediate_levels, parent_child_relationships, sibling_relationships,
            cross_hierarchy_relationships, analysis_timestamp, version
        ) VALUES (
            '{request.table_name}',
            '{request.table_name}',
            '{request.hierarchy_name}',
            {total_levels},
            '{root_column or ''}',
            0,
            '{leaf_column or ''}',
            0,
            {repr(request.levels)},
            {repr(parent_child_relationships)},
            '[]',
            '[]',
            '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
            {new_version}
        )
        """
        
        db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"Custom hierarchy '{request.hierarchy_name}' created for table '{request.table_name}'",
            "table_name": request.table_name,
            "hierarchy_name": request.hierarchy_name,
            "total_levels": total_levels,
            "root_column": root_column,
            "leaf_column": leaf_column,
            "version": new_version
        }
        
    except Exception as e:
        logger.error(f"Error creating hierarchy: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.post("/erd/create")
async def create_erd_relationship(request: ERDRelationshipCreateRequest):
    """
    Create a custom ERD relationship between two tables/columns.
    
    Args:
        request: ERDRelationshipCreateRequest containing relationship definition
        
    Returns:
        Dict[str, Any]: Created ERD relationship metadata
    """
    try:
        db_manager = DatabaseManager()
        
        # Validate that both tables exist in Stage 2
        table_check = f"""
        SELECT name FROM system.tables 
        WHERE database = 'silver' AND name LIKE '%_stage2'
        AND (name = '{request.table1}_stage2' OR name = '{request.table2}_stage2')
        """
        
        tables_exist = db_manager.execute_query(table_check)
        if len(tables_exist) < 2:
            raise HTTPException(status_code=404, detail=f"One or both Stage 2 tables not found: {request.table1}_stage2, {request.table2}_stage2")
        
        # Generate version number
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Create relationship object
        relationship = {
            'table1': request.table1,
            'column1': request.column1,
            'table2': request.table2,
            'column2': request.column2,
            'relationship_type': request.relationship_type,
            'confidence': request.confidence,
            'description': request.description or '',
            'created_by': 'user',
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Check if ERD metadata exists for table1
        check_query = f"""
        SELECT table_name, primary_key_candidates, fact_columns, dimension_columns, relationships
        FROM metadata.erd
        WHERE table_name = '{request.table1}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        existing = db_manager.execute_query(check_query)
        
        if existing:
            # Update existing ERD metadata
            existing_record = existing[0]
            existing_relationships = existing_record.get('relationships', [])
            existing_relationships.append(relationship)
            
            insert_sql = f"""
            INSERT INTO metadata.erd (
                table_name, table_type, row_count, column_count,
                primary_key_candidates, fact_columns, dimension_columns,
                relationships, analysis_timestamp, version
            ) VALUES (
                '{request.table1}',
                '{existing_record.get('table_type', 'unknown')}',
                {existing_record.get('row_count', 0)},
                {existing_record.get('column_count', 0)},
                {repr(existing_record.get('primary_key_candidates', []))},
                {repr(existing_record.get('fact_columns', []))},
                {repr(existing_record.get('dimension_columns', []))},
                {repr(existing_relationships)},
                '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
                {new_version}
            )
            """
        else:
            # Create new ERD metadata
            insert_sql = f"""
            INSERT INTO metadata.erd (
                table_name, table_type, row_count, column_count,
                primary_key_candidates, fact_columns, dimension_columns,
                relationships, analysis_timestamp, version
            ) VALUES (
                '{request.table1}',
                'unknown',
                0, 0,
                '[]',
                '[]',
                '[]',
                {repr([relationship])},
                '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
                {new_version}
            )
            """
        
        db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"ERD relationship created between {request.table1}.{request.column1} and {request.table2}.{request.column2}",
            "relationship": relationship,
            "version": new_version
        }
        
    except Exception as e:
        logger.error(f"Error creating ERD relationship: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.delete("/erd/relationships/{table_name}")
async def delete_erd_relationships(table_name: str, relationship_id: Optional[str] = None):
    """
    Delete ERD relationships for a table or specific relationship.
    
    Args:
        table_name: Name of the table
        relationship_id: Optional specific relationship ID to delete
        
    Returns:
        Dict[str, Any]: Deletion confirmation
    """
    try:
        db_manager = DatabaseManager()
        
        # Get current ERD metadata
        check_query = f"""
        SELECT table_name, relationships, version
        FROM metadata.erd
        WHERE table_name = '{table_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        existing = db_manager.execute_query(check_query)
        if not existing:
            raise HTTPException(status_code=404, detail=f"ERD metadata for table '{table_name}' not found")
        
        existing_record = existing[0]
        current_relationships = existing_record.get('relationships', [])
        
        if relationship_id:
            # Delete specific relationship
            filtered_relationships = [
                rel for rel in current_relationships 
                if rel.get('id') != relationship_id
            ]
            deleted_count = len(current_relationships) - len(filtered_relationships)
        else:
            # Delete all relationships
            filtered_relationships = []
            deleted_count = len(current_relationships)
        
        # Generate new version
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Insert updated record
        insert_sql = f"""
        INSERT INTO metadata.erd (
            table_name, table_type, row_count, column_count,
            primary_key_candidates, fact_columns, dimension_columns,
            relationships, analysis_timestamp, version
        ) VALUES (
            '{table_name}',
            '{existing_record.get('table_type', 'unknown')}',
            {existing_record.get('row_count', 0)},
            {existing_record.get('column_count', 0)},
            {repr(existing_record.get('primary_key_candidates', []))},
            {repr(existing_record.get('fact_columns', []))},
            {repr(existing_record.get('dimension_columns', []))},
            {repr(filtered_relationships)},
            '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
            {new_version}
        )
        """
        
        db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"Deleted {deleted_count} relationship(s) from table '{table_name}'",
            "table_name": table_name,
            "deleted_count": deleted_count,
            "remaining_relationships": len(filtered_relationships),
            "version": new_version
        }
        
    except Exception as e:
        logger.error(f"Error deleting ERD relationships: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@model_router.delete("/hierarchies/{table_name}")
async def delete_hierarchy(table_name: str, hierarchy_name: Optional[str] = None):
    """
    Delete hierarchy metadata for a table or specific hierarchy.
    
    Args:
        table_name: Name of the table
        hierarchy_name: Optional specific hierarchy name to delete
        
    Returns:
        Dict[str, Any]: Deletion confirmation
    """
    try:
        db_manager = DatabaseManager()
        
        # Get current hierarchy metadata
        check_query = f"""
        SELECT table_name, hierarchy_name, version
        FROM metadata.hierarchies
        WHERE table_name = '{table_name}'
        ORDER BY version DESC
        LIMIT 1
        """
        
        existing = db_manager.execute_query(check_query)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Hierarchy metadata for table '{table_name}' not found")
        
        existing_record = existing[0]
        
        if hierarchy_name and existing_record.get('hierarchy_name') != hierarchy_name:
            raise HTTPException(status_code=404, detail=f"Hierarchy '{hierarchy_name}' not found for table '{table_name}'")
        
        # Generate new version
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Insert "deleted" record (soft delete by setting empty values)
        insert_sql = f"""
        INSERT INTO metadata.hierarchies (
            table_name, original_table_name, hierarchy_name, total_levels,
            root_column, root_cardinality, leaf_column, leaf_cardinality,
            intermediate_levels, parent_child_relationships, sibling_relationships,
            cross_hierarchy_relationships, analysis_timestamp, version
        ) VALUES (
            '{table_name}',
            '{table_name}',
            'DELETED',
            0,
            '',
            0,
            '',
            0,
            '[]',
            '[]',
            '[]',
            '[]',
            '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
            {new_version}
        )
        """
        
        db_manager.execute_query(insert_sql)
        
        return {
            "status": "success",
            "message": f"Hierarchy metadata deleted for table '{table_name}'",
            "table_name": table_name,
            "version": new_version
        }
        
    except Exception as e:
        logger.error(f"Error deleting hierarchy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@model_router.post("/calendar/generate")
async def generate_calendar_dimension(request: CalendarGenerationRequest):
    """
    Generate calendar dimension table in gold schema.
    
    Args:
        request: CalendarGenerationRequest with start_date and end_date
        
    Returns:
        Dict containing generation results and statistics
    """
    try:
        logger.info(f"Starting calendar dimension generation from {request.start_date} to {request.end_date}")
        
        # Validate date format
        try:
            datetime.strptime(request.start_date, '%Y-%m-%d')
            datetime.strptime(request.end_date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
        
        # Validate date range
        if request.start_date >= request.end_date:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Initialize calendar generator
        db_manager = DatabaseManager()
        calendar_generator = CalendarGenerator(db_manager)
        
        # Generate calendar dimension
        result = calendar_generator.generate_calendar_dimension(
            request.start_date, 
            request.end_date
        )
        
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["message"])
        
        logger.info(f"Calendar dimension generation completed successfully")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating calendar dimension: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate calendar dimension: {str(e)}")


@model_router.get("/calendar/status")
async def get_calendar_status():
    """
    Get status of the calendar dimension table.
    
    Returns:
        Dict containing calendar dimension statistics
    """
    try:
        db_manager = DatabaseManager()
        
        # Check if table exists
        check_table_sql = """
        SELECT COUNT(*) as table_exists
        FROM system.tables 
        WHERE database = 'silver' AND name = 'calendar_stage1'
        """
        
        result = db_manager.execute_query_dict(check_table_sql)
        table_exists = result[0]['table_exists'] > 0 if result else False
        
        if not table_exists:
            return {
                "status": "not_exists",
                "message": "Calendar dimension table does not exist",
                "table_name": "silver.calendar_stage1"
            }
        
        # Get statistics
        stats_sql = """
        SELECT 
            COUNT(*) as total_records,
            MIN(calendar_date) as min_date,
            MAX(calendar_date) as max_date,
            COUNT(DISTINCT calendar_year) as years_covered
        FROM silver.calendar_stage1
        """
        
        result = db_manager.execute_query_dict(stats_sql)
        stats = result[0] if result else {}
        
        return {
            "status": "exists",
            "message": "Calendar dimension table exists",
            "table_name": "silver.calendar_stage1",
            "statistics": {
                "total_records": stats.get('total_records', 0),
                "date_range": {
                    "min_date": str(stats.get('min_date', '')),
                    "max_date": str(stats.get('max_date', ''))
                },
                "years_covered": stats.get('years_covered', 0)
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting calendar status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get calendar status: {str(e)}")