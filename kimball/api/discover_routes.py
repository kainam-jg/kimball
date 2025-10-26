"""
KIMBALL Discovery Phase API Routes

This module provides comprehensive data discovery and metadata analysis
for the bronze layer in ClickHouse, including:
- Data type inference from string values
- Fact vs dimension classification
- Primary key candidate identification
- Join relationship discovery
- Data quality assessment
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from pydantic import BaseModel
import json
import re
import logging
from collections import defaultdict

from ..core.database import DatabaseManager
from ..core.logger import Logger
from ..discover.intelligent_type_inference import IntelligentTypeInference, TypeInferenceResult

# Initialize router and logger
discover_router = APIRouter(prefix="/api/v1/discover", tags=["Discover"])
logger = Logger(__name__)

# Initialize intelligent type inference engine
type_inference_engine = IntelligentTypeInference()

# Pydantic models for request/response
class DiscoveryRequest(BaseModel):
    """Request model for discovery operations."""
    schema_name: str = "bronze"
    include_sample_data: bool = True
    sample_size: int = 10

class TableAnalysisRequest(BaseModel):
    """Request model for single table analysis."""
    table_name: str
    include_sample_data: bool = True
    sample_size: int = 10

class ColumnAnalysisRequest(BaseModel):
    """Request model for single column analysis."""
    table_name: str
    column_name: str
    sample_size: int = 100

class MetadataEditRequest(BaseModel):
    """Request model for editing metadata."""
    original_table_name: str
    original_column_name: str
    new_table_name: Optional[str] = None
    new_column_name: Optional[str] = None
    inferred_type: Optional[str] = None
    classification: Optional[str] = None

# Data type inference patterns
DATA_TYPE_PATTERNS = {
    'integer': [
        r'^-?\d+$',  # Basic integer
        r'^-?\d{1,3}(,\d{3})*$',  # Comma-separated integers
    ],
    'decimal': [
        r'^-?\d+\.\d+$',  # Basic decimal
        r'^-?\d{1,3}(,\d{3})*\.\d+$',  # Comma-separated decimals
        r'^-?\d+\.\d{1,2}$',  # Currency format
    ],
    'date': [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
        r'^\d{2}-\d{2}-\d{4}$',  # MM-DD-YYYY
        r'^\d{4}\d{2}\d{2}$',  # YYYYMMDD
    ],
    'datetime': [
        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',  # YYYY-MM-DD HH:MM:SS
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
        r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$',  # MM/DD/YYYY HH:MM:SS
    ],
    'boolean': [
        r'^(true|false|yes|no|1|0|y|n)$',  # Boolean values
    ],
    'email': [
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',  # Email format
    ],
    'url': [
        r'^https?://[^\s/$.?#].[^\s]*$',  # URL format
    ]
}

class DataTypeInference:
    """Handles data type inference from string values."""
    
    @staticmethod
    def infer_data_type(values: List[str]) -> Dict[str, Any]:
        """
        Infer the most likely data type from a list of string values.
        
        Args:
            values: List of string values to analyze
            
        Returns:
            Dict containing inferred type and confidence score
        """
        if not values:
            return {"type": "string", "confidence": 0.0, "pattern": None}
        
        # Remove null/empty values for analysis
        non_empty_values = [str(v).strip() for v in values if v and str(v).strip()]
        
        if not non_empty_values:
            return {"type": "string", "confidence": 0.0, "pattern": None}
        
        type_scores = {}
        
        # Test each data type pattern
        for data_type, patterns in DATA_TYPE_PATTERNS.items():
            matches = 0
            total_tested = min(len(non_empty_values), 100)  # Test up to 100 values
            
            for value in non_empty_values[:total_tested]:
                for pattern in patterns:
                    if re.match(pattern, value, re.IGNORECASE):
                        matches += 1
                        break
            
            confidence = matches / total_tested if total_tested > 0 else 0
            type_scores[data_type] = confidence
        
        # Find the best match
        best_type = max(type_scores.items(), key=lambda x: x[1])
        
        # If confidence is too low, default to string
        if best_type[1] < 0.3:
            return {"type": "string", "confidence": 0.0, "pattern": None}
        
        return {
            "type": best_type[0],
            "confidence": best_type[1],
            "pattern": DATA_TYPE_PATTERNS[best_type[0]][0] if DATA_TYPE_PATTERNS[best_type[0]] else None
        }

class FactDimensionClassifier:
    """Classifies columns as fact (measures) or dimension (attributes)."""
    
    @staticmethod
    def classify_column(column_name: str, data_type: str, cardinality: int, 
                       sample_values: List[str], row_count: int) -> Dict[str, Any]:
        """
        Classify a column as fact or dimension based on multiple criteria.
        
        Args:
            column_name: Name of the column
            data_type: Inferred data type
            cardinality: Number of unique values
            sample_values: Sample values from the column
            row_count: Total number of rows in table
            
        Returns:
            Dict containing classification and reasoning
        """
        # Calculate cardinality ratio
        cardinality_ratio = cardinality / row_count if row_count > 0 else 0
        
        # Initialize classification factors
        factors = {
            "is_numeric": data_type in ['integer', 'decimal', 'numeric'],
            "is_date": data_type in ['date', 'datetime'],
            "is_boolean": data_type == 'boolean',
            "is_identifier": _is_identifier_column(column_name),
            "high_cardinality": cardinality_ratio > 0.8,
            "low_cardinality": cardinality_ratio < 0.1,
            "medium_cardinality": 0.1 <= cardinality_ratio <= 0.8
        }
        
        # Classification logic
        classification = "dimension"  # Default to dimension
        reasoning = []
        
        # Fact indicators (measures) - Numeric types should default to fact
        if factors["is_numeric"]:
            classification = "fact"
            reasoning.append("Numeric data type (typically a measure/fact)")
            
            if factors["high_cardinality"]:
                reasoning.append("High cardinality confirms measure classification")
            
            if _is_measure_column(column_name):
                reasoning.append("Column name suggests measure (amount, cost, quantity, etc.)")
        
        # Dimension indicators (attributes)
        if factors["is_date"]:
            classification = "dimension"
            reasoning.append("Date/time column (typically dimension)")
        
        if factors["is_boolean"]:
            classification = "dimension"
            reasoning.append("Boolean column (typically dimension)")
        
        if factors["is_identifier"]:
            classification = "dimension"
            reasoning.append("Column name suggests identifier/key")
        
        if factors["low_cardinality"] and not factors["is_numeric"]:
            classification = "dimension"
            reasoning.append("Low cardinality non-numeric column (likely dimension)")
        
        # Confidence calculation
        confidence = _calculate_classification_confidence(factors, classification)
        
        return {
            "classification": classification,
            "confidence": confidence,
            "reasoning": reasoning,
            "factors": factors
        }

def _is_identifier_column(column_name: str) -> bool:
    """Check if column name suggests it's an identifier."""
    identifier_patterns = [
        r'.*id$', r'.*_id$', r'.*key$', r'.*_key$', r'.*code$', r'.*_code$',
        r'.*num$', r'.*_num$', r'.*no$', r'.*_no$', r'.*name$', r'.*_name$'
    ]
    
    column_lower = column_name.lower()
    return any(re.match(pattern, column_lower) for pattern in identifier_patterns)

def _is_measure_column(column_name: str) -> bool:
    """Check if column name suggests it's a measure."""
    measure_patterns = [
        r'.*amount$', r'.*cost$', r'.*price$', r'.*value$', r'.*quantity$',
        r'.*qty$', r'.*count$', r'.*total$', r'.*sum$', r'.*avg$', r'.*rate$'
    ]
    
    column_lower = column_name.lower()
    return any(re.match(pattern, column_lower) for pattern in measure_patterns)

def _calculate_classification_confidence(factors: Dict[str, bool], classification: str) -> float:
    """Calculate confidence score for classification."""
    confidence = 0.5  # Base confidence
    
    if classification == "fact":
        if factors["is_numeric"]:
            confidence += 0.3
        if factors["high_cardinality"]:
            confidence += 0.2
        if not factors["is_identifier"]:
            confidence += 0.1
    else:  # dimension
        if factors["is_date"] or factors["is_boolean"]:
            confidence += 0.3
        if factors["is_identifier"]:
            confidence += 0.2
        if factors["low_cardinality"]:
            confidence += 0.1
    
    return min(confidence, 1.0)

@discover_router.get("/debug/{table_name}")
async def debug_table_analysis(table_name: str):
    """Debug endpoint to test table analysis step by step."""
    try:
        db_manager = DatabaseManager()
        
        # Test 1: Get table schema
        schema_query = f"DESCRIBE TABLE bronze.{table_name}"
        schema_result = db_manager.execute_query(schema_query)
        
        if not schema_result:
            return {"error": f"Table {table_name} not found", "step": "schema_query"}
        
        # Test 2: Get row count
        count_query = f"SELECT COUNT(*) FROM bronze.{table_name}"
        count_result = db_manager.execute_query(count_query)
        
        if not count_result:
            return {"error": "Failed to get row count", "step": "count_query"}
        
        # Test 3: Get first column info
        first_col = schema_result[0]
        if isinstance(first_col, dict):
            col_name = first_col.get('name', '')
            col_type = first_col.get('type', 'String')
        else:
            col_name = first_col[0]
            col_type = first_col[1]
        
        # Test 4: Get cardinality for first column
        cardinality_query = f"SELECT COUNT(DISTINCT `{col_name}`) FROM bronze.{table_name}"
        cardinality_result = db_manager.execute_query(cardinality_query)
        
        return {
            "status": "success",
            "table_name": table_name,
            "schema_result_type": type(schema_result[0]).__name__,
            "schema_sample": schema_result[0],
            "count_result_type": type(count_result[0]).__name__,
            "count_sample": count_result[0],
            "first_column": {"name": col_name, "type": col_type},
            "cardinality_result_type": type(cardinality_result[0]).__name__ if cardinality_result else "None",
            "cardinality_sample": cardinality_result[0] if cardinality_result else None
        }
        
    except Exception as e:
        return {"error": str(e), "step": "exception"}

@discover_router.get("/status")
async def get_discovery_status():
    """Get discovery phase status and available operations."""
    try:
        db_manager = DatabaseManager()
        
        # Test basic connection first
        test_query = "SELECT 1 as test"
        test_result = db_manager.execute_query(test_query)
        
        if not test_result:
            return {
                "phase": "discover",
                "status": "error",
                "error": "Database connection failed",
                "bronze_tables": 0,
                "available_operations": []
            }
        
        # Get bronze schema tables
        tables_query = "SHOW TABLES FROM bronze"
        tables_result = db_manager.execute_query(tables_query)
        
        # Handle the result properly - it should be a list of dictionaries
        tables = []
        if tables_result:
            for row in tables_result:
                if isinstance(row, dict):
                    # Get the first value from the dictionary (table name)
                    table_name = list(row.values())[0]
                    tables.append(table_name)
                else:
                    # Handle tuple/list format
                    tables.append(row[0])
        
        return {
            "phase": "discover",
            "status": "active",
            "bronze_tables": len(tables),
            "available_operations": [
                "analyze_schema",
                "analyze_table", 
                "analyze_column",
                "infer_data_types",
                "classify_columns",
                "find_relationships",
                "export_metadata"
            ],
            "bronze_schema_tables": tables
        }
    except Exception as e:
        logger.error(f"Error getting discovery status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.post("/analyze/schema")
async def analyze_bronze_schema(request: DiscoveryRequest):
    """Analyze the entire bronze schema for metadata discovery."""
    try:
        db_manager = DatabaseManager()
        
        # Get all tables in bronze schema
        tables_query = "SHOW TABLES FROM bronze"
        tables_result = db_manager.execute_query(tables_query)
        
        # Handle the result properly - it should be a list of dictionaries
        tables = []
        if tables_result:
            for row in tables_result:
                if isinstance(row, dict):
                    # Get the first value from the dictionary (table name)
                    table_name = list(row.values())[0]
                    tables.append(table_name)
                else:
                    # Handle tuple/list format
                    tables.append(row[0])
        
        if not tables:
            return {
                "schema_name": request.schema_name,
                "analysis_timestamp": datetime.now().isoformat(),
                "total_tables": 0,
                "tables": {},
                "schema_summary": {}
            }
        
        schema_analysis = {
            "schema_name": request.schema_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "total_tables": len(tables),
            "tables": {},
            "schema_summary": {}
        }
        
        # Analyze each table
        for table_name in tables:
            logger.info(f"Analyzing table: {table_name}")
            try:
                table_analysis = await analyze_single_table(table_name, request.include_sample_data, request.sample_size)
                schema_analysis["tables"][table_name] = table_analysis
            except Exception as e:
                logger.error(f"Failed to analyze table {table_name}: {e}")
                schema_analysis["tables"][table_name] = {"error": str(e)}
        
        # Generate schema summary
        schema_analysis["schema_summary"] = _generate_schema_summary(schema_analysis["tables"])
        
        return schema_analysis
        
    except Exception as e:
        logger.error(f"Error analyzing bronze schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.post("/analyze/table")
async def analyze_table(request: TableAnalysisRequest):
    """Analyze a single table for metadata discovery."""
    try:
        table_analysis = await analyze_single_table(
            request.table_name, 
            request.include_sample_data, 
            request.sample_size
        )
        return table_analysis
    except Exception as e:
        logger.error(f"Error analyzing table {request.table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def analyze_single_table(table_name: str, include_sample_data: bool = True, sample_size: int = 10) -> Dict[str, Any]:
    """Analyze a single table and return comprehensive metadata."""
    db_manager = DatabaseManager()
    
    # Get table schema
    schema_query = f"DESCRIBE TABLE bronze.{table_name}"
    schema_result = db_manager.execute_query(schema_query)
    
    if not schema_result:
        raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
    
    # Get row count
    count_query = f"SELECT COUNT(*) FROM bronze.{table_name}"
    count_result = db_manager.execute_query(count_query)
    
    # Handle dictionary format from DatabaseManager.execute_query
    if count_result and isinstance(count_result[0], dict):
        row_count = count_result[0]['COUNT()']
    elif count_result:
        row_count = count_result[0][0]
    else:
        row_count = 0
    
    # Analyze each column
    columns_analysis = []
    for col_info in schema_result:
        # Handle dictionary format from DatabaseManager.execute_query
        if isinstance(col_info, dict):
            col_name = col_info.get('name', '')
            col_type = col_info.get('type', 'String')
        else:
            # Fallback for tuple format
            col_name = col_info[0]
            col_type = col_info[1]
        
        logger.info(f"Analyzing column: {col_name}")
        
        # Get cardinality
        cardinality_query = f"SELECT COUNT(DISTINCT `{col_name}`) FROM bronze.{table_name}"
        cardinality_result = db_manager.execute_query(cardinality_query)
        
        # Handle dictionary format
        if cardinality_result and isinstance(cardinality_result[0], dict):
            cardinality = cardinality_result[0][f'COUNTDistinct({col_name})']
        elif cardinality_result:
            cardinality = cardinality_result[0][0]
        else:
            cardinality = 0
        
        # Get null count
        null_query = f"SELECT COUNT(*) FROM bronze.{table_name} WHERE `{col_name}` = '' OR `{col_name}` IS NULL"
        null_result = db_manager.execute_query(null_query)
        
        # Handle dictionary format
        if null_result and isinstance(null_result[0], dict):
            null_count = null_result[0]['COUNT()']
        elif null_result:
            null_count = null_result[0][0]
        else:
            null_count = 0
        
        # Get sample values for type inference
        sample_query = f"SELECT DISTINCT `{col_name}` FROM bronze.{table_name} WHERE `{col_name}` != '' AND `{col_name}` IS NOT NULL LIMIT {sample_size}"
        sample_result = db_manager.execute_query(sample_query)
        
        # Handle dictionary format
        sample_values = []
        if sample_result:
            for row in sample_result:
                if isinstance(row, dict):
                    # Get the first value from the dictionary (the column value)
                    sample_values.append(list(row.values())[0])
                else:
                    sample_values.append(row[0])
        
        # Infer data type from string values using intelligent inference
        type_inference_result = type_inference_engine.infer_column_type(sample_values, col_name)
        
        # Convert to legacy format for compatibility
        type_inference = {
            "type": type_inference_result.inferred_type,
            "confidence": type_inference_result.confidence,
            "pattern": type_inference_result.pattern_matched,
            "reasoning": type_inference_result.reasoning,
            "sample_values": type_inference_result.sample_values
        }
        
        # Classify as fact or dimension
        classification = FactDimensionClassifier.classify_column(
            col_name, type_inference["type"], cardinality, sample_values, row_count
        )
        
        # Check if primary key candidate
        is_pk_candidate = _is_primary_key_candidate(
            col_name, type_inference["type"], cardinality, null_count, row_count, classification["classification"]
        )
        
        # Calculate data quality score
        quality_score = _calculate_data_quality_score(null_count, cardinality, row_count)
        
        column_analysis = {
            "name": col_name,
            "bronze_type": col_type,  # Always String in bronze
            "inferred_type": type_inference["type"],
            "type_confidence": type_inference["confidence"],
            "cardinality": cardinality,
            "null_count": null_count,
            "null_percentage": (null_count / row_count) * 100 if row_count > 0 else 0,
            "classification": classification["classification"],
            "classification_confidence": classification["confidence"],
            "classification_reasoning": classification["reasoning"],
            "is_primary_key_candidate": is_pk_candidate,
            "data_quality_score": quality_score,
            "cardinality_ratio": cardinality / row_count if row_count > 0 else 0,
            "sample_values": sample_values if include_sample_data else []
        }
        
        columns_analysis.append(column_analysis)
    
    # Generate table summary
    table_summary = _generate_table_summary(columns_analysis, row_count)
    
    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": len(columns_analysis),
        "columns": columns_analysis,
        "analysis_timestamp": datetime.now().isoformat(),
        "fact_columns": [col["name"] for col in columns_analysis if col["classification"] == "fact"],
        "dimension_columns": [col["name"] for col in columns_analysis if col["classification"] == "dimension"],
        "primary_key_candidates": [col["name"] for col in columns_analysis if col["is_primary_key_candidate"]],
        "summary": table_summary
    }

def _is_primary_key_candidate(col_name: str, data_type: str, cardinality: int, 
                            null_count: int, row_count: int, classification: str) -> bool:
    """Determine if a column could be a primary key candidate."""
    # Primary keys should be dimension columns
    if classification != "dimension":
        return False
    
    # No null values
    if null_count > 0:
        return False
    
    # High cardinality (at least 80% uniqueness)
    cardinality_ratio = cardinality / row_count if row_count > 0 else 0
    if cardinality_ratio < 0.8:
        return False
    
    # Minimum cardinality threshold
    if cardinality < 100:
        return False
    
    # Suitable data types for keys
    suitable_types = ['string', 'integer', 'date', 'datetime']
    if data_type not in suitable_types:
        return False
    
    return True

def _calculate_data_quality_score(null_count: int, cardinality: int, row_count: int) -> float:
    """Calculate data quality score for a column."""
    if row_count == 0:
        return 0.0
    
    # Null percentage penalty
    null_percentage = null_count / row_count
    null_score = max(0, 1.0 - null_percentage)
    
    # Cardinality score (higher is better, up to a point)
    cardinality_score = min(cardinality / 1000, 1.0)
    
    return (null_score + cardinality_score) / 2

def _generate_table_summary(columns: List[Dict[str, Any]], row_count: int) -> Dict[str, Any]:
    """Generate a summary of table characteristics."""
    fact_cols = [col for col in columns if col["classification"] == "fact"]
    dim_cols = [col for col in columns if col["classification"] == "dimension"]
    
    return {
        "total_columns": len(columns),
        "fact_columns": len(fact_cols),
        "dimension_columns": len(dim_cols),
        "avg_data_quality": sum(col["data_quality_score"] for col in columns) / len(columns) if columns else 0,
        "high_quality_columns": len([col for col in columns if col["data_quality_score"] > 0.8]),
        "primary_key_candidates": len([col for col in columns if col["is_primary_key_candidate"]]),
        "columns_with_nulls": len([col for col in columns if col["null_count"] > 0]),
        "avg_type_confidence": sum(col["type_confidence"] for col in columns) / len(columns) if columns else 0
    }

def _generate_schema_summary(tables: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a summary of the entire schema."""
    total_tables = len(tables)
    total_columns = sum(len(table.get("columns", [])) for table in tables.values() if "error" not in table)
    total_fact_cols = sum(len(table.get("fact_columns", [])) for table in tables.values() if "error" not in table)
    total_dim_cols = sum(len(table.get("dimension_columns", [])) for table in tables.values() if "error" not in table)
    total_pk_candidates = sum(len(table.get("primary_key_candidates", [])) for table in tables.values() if "error" not in table)
    
    return {
        "total_tables": total_tables,
        "total_columns": total_columns,
        "total_fact_columns": total_fact_cols,
        "total_dimension_columns": total_dim_cols,
        "total_primary_key_candidates": total_pk_candidates,
        "avg_columns_per_table": total_columns / total_tables if total_tables > 0 else 0,
        "fact_dimension_ratio": total_fact_cols / total_dim_cols if total_dim_cols > 0 else 0
    }

@discover_router.post("/test/intelligent-inference")
async def test_intelligent_inference(request: ColumnAnalysisRequest):
    """Test the intelligent type inference system on a specific column."""
    try:
        db_manager = DatabaseManager()
        
        # Get sample values from the column
        sample_query = f"SELECT DISTINCT `{request.column_name}` FROM bronze.{request.table_name} WHERE `{request.column_name}` != '' AND `{request.column_name}` IS NOT NULL LIMIT {request.sample_size}"
        sample_result = db_manager.execute_query(sample_query)
        
        # Handle dictionary format
        sample_values = []
        if sample_result:
            for row in sample_result:
                if isinstance(row, dict):
                    sample_values.append(list(row.values())[0])
                else:
                    sample_values.append(row[0])
        
        # Use intelligent type inference
        inference_result = type_inference_engine.infer_column_type(sample_values, request.column_name)
        
        # Get performance stats
        performance_stats = type_inference_engine.get_performance_stats()
        
        return {
            "table_name": request.table_name,
            "column_name": request.column_name,
            "sample_size": len(sample_values),
            "sample_values": sample_values,
            "inference_result": {
                "inferred_type": inference_result.inferred_type,
                "confidence": inference_result.confidence,
                "pattern_matched": inference_result.pattern_matched,
                "reasoning": inference_result.reasoning
            },
            "performance_stats": performance_stats
        }
        
    except Exception as e:
        logger.error(f"Error testing intelligent inference for {request.table_name}.{request.column_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.post("/learn/correction")
async def learn_from_correction(correction_data: dict):
    """Learn from user corrections to improve future predictions."""
    try:
        column_name = correction_data.get("column_name")
        predicted_type = correction_data.get("predicted_type")
        actual_type = correction_data.get("actual_type")
        confidence = correction_data.get("confidence", 0.0)
        
        if not all([column_name, predicted_type, actual_type]):
            raise HTTPException(status_code=400, detail="Missing required fields: column_name, predicted_type, actual_type")
        
        # Learn from the correction
        type_inference_engine.learn_from_correction(column_name, predicted_type, actual_type, confidence)
        
        return {
            "status": "success",
            "message": f"Learned from correction: {column_name} predicted={predicted_type}, actual={actual_type}",
            "performance_stats": type_inference_engine.get_performance_stats()
        }
        
    except Exception as e:
        logger.error(f"Error learning from correction: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@discover_router.post("/store/discover-metadata")
async def store_discover_metadata(request: DiscoveryRequest):
    """Store Discovery phase results in the metadata.discover table."""
    try:
        db_manager = DatabaseManager()
        
        # Perform schema analysis to get the data
        schema_analysis = await analyze_bronze_schema(request)
        
        # Prepare data for insertion
        metadata_records = []
        analysis_timestamp = datetime.now()
        
        for table_name, table_data in schema_analysis["tables"].items():
            if "error" in table_data:
                continue  # Skip tables with errors
                
            for column_data in table_data.get("columns", []):
                # Convert sample_values list to string for storage
                sample_values_str = json.dumps(column_data.get("sample_values", []))
                classification_reasoning_str = json.dumps(column_data.get("classification_reasoning", []))
                
                # Generate version based on timestamp for upsert functionality
                version = int(analysis_timestamp.timestamp() * 1000000)  # Microsecond precision
                
                record = {
                    "original_table_name": table_name,
                    "new_table_name": table_name,  # Initially same as original
                    "original_column_name": column_data["name"],
                    "new_column_name": column_data["name"],  # Initially same as original
                    "bronze_type": column_data["bronze_type"],
                    "inferred_type": column_data["inferred_type"],
                    "type_confidence": column_data["type_confidence"],
                    "pattern_matched": column_data.get("pattern", ""),
                    "reasoning": column_data.get("reasoning", ""),
                    "cardinality": column_data["cardinality"],
                    "null_count": column_data["null_count"],
                    "null_percentage": column_data["null_percentage"],
                    "classification": column_data["classification"],
                    "classification_confidence": column_data["classification_confidence"],
                    "classification_reasoning": classification_reasoning_str,
                    "is_primary_key_candidate": 1 if column_data["is_primary_key_candidate"] else 0,
                    "data_quality_score": column_data["data_quality_score"],
                    "cardinality_ratio": column_data["cardinality_ratio"],
                    "sample_values": sample_values_str,
                    "analysis_timestamp": analysis_timestamp,
                    "version": version
                }
                metadata_records.append(record)
        
        # Ensure the metadata.discover table exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS metadata.discover (
            original_table_name String,
            new_table_name String,
            original_column_name String,
            new_column_name String,
            bronze_type String,
            inferred_type String,
            type_confidence Float64,
            pattern_matched String,
            reasoning String,
            cardinality UInt64,
            null_count UInt64,
            null_percentage Float64,
            classification String,
            classification_confidence Float64,
            classification_reasoning String,
            is_primary_key_candidate UInt8,
            data_quality_score Float64,
            cardinality_ratio Float64,
            sample_values String,
            analysis_timestamp DateTime,
            created_at DateTime DEFAULT now(),
            version UInt64 DEFAULT 1
        ) ENGINE = ReplacingMergeTree(version)
        ORDER BY (original_table_name, original_column_name)
        """
        
        logger.info("Creating metadata.discover table if it doesn't exist...")
        db_manager.execute_query(create_table_sql)
        
        # Insert records into metadata.discover table
        if metadata_records:
            insert_count = 0
            for record in metadata_records:
                # Build INSERT query
                columns = list(record.keys())
                values = list(record.values())
                
                # Convert values to proper format for ClickHouse
                formatted_values = []
                for value in values:
                    if isinstance(value, str):
                        # Escape single quotes by replacing them
                        escaped_value = value.replace("'", "''")
                        formatted_values.append(f"'{escaped_value}'")
                    elif isinstance(value, datetime):
                        formatted_values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                    else:
                        formatted_values.append(str(value))
                
                insert_query = f"""
                INSERT INTO metadata.discover ({', '.join(columns)})
                VALUES ({', '.join(formatted_values)})
                """
                
                try:
                    db_manager.execute_query(insert_query)
                    insert_count += 1
                except Exception as e:
                    logger.error(f"Failed to insert metadata record for {record['table_name']}.{record['column_name']}: {e}")
                    continue
        
        return {
            "status": "success",
            "message": f"Stored {insert_count} metadata records in metadata.discover",
            "total_records": len(metadata_records),
            "inserted_records": insert_count,
            "analysis_timestamp": analysis_timestamp.isoformat(),
            "tables_analyzed": len(schema_analysis["tables"])
        }
        
    except Exception as e:
        logger.error(f"Error storing discover metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.get("/query/discover-metadata")
async def query_discover_metadata(
    table_name: Optional[str] = None,
    column_name: Optional[str] = None,
    inferred_type: Optional[str] = None,
    limit: int = 100
):
    """Query the discover metadata table with optional filters."""
    try:
        db_manager = DatabaseManager()
        
        # Build query with optional filters
        where_conditions = []
        if table_name:
            where_conditions.append(f"table_name = '{table_name}'")
        if column_name:
            where_conditions.append(f"original_column_name = '{column_name}'")
        if inferred_type:
            where_conditions.append(f"inferred_type = '{inferred_type}'")
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query = f"""
        SELECT 
            table_name,
            original_column_name,
            new_column_name,
            bronze_type,
            inferred_type,
            type_confidence,
            pattern_matched,
            reasoning,
            cardinality,
            null_count,
            null_percentage,
            classification,
            classification_confidence,
            data_quality_score,
            cardinality_ratio,
            sample_values,
            analysis_timestamp,
            created_at,
            version
        FROM metadata.discover
        {where_clause}
        ORDER BY analysis_timestamp DESC, table_name, original_column_name
        LIMIT {limit}
        """
        
        results = db_manager.execute_query(query)
        
        return {
            "status": "success",
            "query": query,
            "results": results,
            "count": len(results)
        }
        
    except Exception as e:
        logger.error(f"Error querying discover metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.get("/metadata")
async def get_discover_metadata(
    table_name: Optional[str] = None,
    limit: int = 100
):
    """Get discovery metadata for all tables or a specific table."""
    try:
        db_manager = DatabaseManager()
        
        # Build query with optional table filter
        where_clause = ""
        if table_name:
            where_clause = f"WHERE original_table_name = '{table_name}'"
        
        query = f"""
        SELECT 
            original_table_name,
            new_table_name,
            original_column_name,
            new_column_name,
            bronze_type,
            inferred_type,
            type_confidence,
            pattern_matched,
            reasoning,
            cardinality,
            null_count,
            null_percentage,
            classification,
            classification_confidence,
            data_quality_score,
            cardinality_ratio,
            sample_values,
            analysis_timestamp,
            created_at,
            version
        FROM metadata.discover
        {where_clause}
        ORDER BY original_table_name, original_column_name
        LIMIT {limit}
        """
        
        results = db_manager.execute_query(query)
        
        # Group results by table for better organization
        tables_metadata = {}
        if results:
            for row in results:
                table_name_key = row["original_table_name"]
                if table_name_key not in tables_metadata:
                    tables_metadata[table_name_key] = []
                tables_metadata[table_name_key].append(row)
        
        return {
            "status": "success",
            "query": query,
            "tables": tables_metadata,
            "total_records": len(results) if results else 0,
            "table_count": len(tables_metadata)
        }
        
    except Exception as e:
        logger.error(f"Error getting discover metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@discover_router.put("/metadata/edit")
async def edit_discover_metadata(request: MetadataEditRequest):
    """
    Edit discovery metadata for a specific column.
    
    IMPORTANT: If new_table_name is provided, it updates the table name for ALL columns
    in that table, not just the specified column. This ensures consistency across the table.
    
    Args:
        request: MetadataEditRequest containing:
            - original_table_name: The original table name to identify the table
            - original_column_name: The original column name to identify the column
            - new_table_name: Optional new table name (updates ALL columns in table)
            - new_column_name: Optional new column name (updates only this column)
            - inferred_type: Optional new inferred data type
            - classification: Optional new fact/dimension classification
    
    Returns:
        Success message with updated fields and new version number
    """
    try:
        db_manager = DatabaseManager()
        
        # Validate that at least one field is being updated
        if not any([request.new_table_name, request.new_column_name, request.inferred_type, request.classification]):
            raise HTTPException(
                status_code=400, 
                detail="At least one field must be provided for update (new_table_name, new_column_name, inferred_type, or classification)"
            )
        
        # Generate new version for upsert functionality (ClickHouse ReplacingMergeTree)
        new_version = int(datetime.now().timestamp() * 1000000)
        
        # Build update fields tracking for response
        update_fields = []
        update_values = []
        
        if request.new_table_name is not None:
            update_fields.append("new_table_name")
            update_values.append(f"'{request.new_table_name}'")
        
        if request.new_column_name is not None:
            update_fields.append("new_column_name")
            update_values.append(f"'{request.new_column_name}'")
        
        if request.inferred_type is not None:
            update_fields.append("inferred_type")
            update_values.append(f"'{request.inferred_type}'")
        
        if request.classification is not None:
            update_fields.append("classification")
            update_values.append(f"'{request.classification}'")
        
        # Always update version and analysis_timestamp for upsert functionality
        update_fields.extend(["version", "analysis_timestamp"])
        update_values.extend([str(new_version), f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'"])
        
        # Build INSERT query for upsert (ReplacingMergeTree will handle the replacement)
        # CRITICAL: If updating table name, update ALL columns for that table to maintain consistency
        if request.new_table_name is not None:
            # Update all columns for the table
            # This ensures that when a table name changes, ALL columns in that table get the new name
            insert_query = f"""
            INSERT INTO metadata.discover (
                original_table_name,
                new_table_name,
                original_column_name,
                new_column_name,
                bronze_type,
                inferred_type,
                type_confidence,
                pattern_matched,
                reasoning,
                cardinality,
                null_count,
                null_percentage,
                classification,
                classification_confidence,
                classification_reasoning,
                is_primary_key_candidate,
                data_quality_score,
                cardinality_ratio,
                sample_values,
                analysis_timestamp,
                version
            )
            SELECT 
                original_table_name,
                '{request.new_table_name}',  -- Update table name for ALL columns
                original_column_name,
                CASE 
                    WHEN original_column_name = '{request.original_column_name}' 
                    THEN {f"'{request.new_column_name}'" if request.new_column_name is not None else f"'{request.original_column_name}'"}
                    ELSE new_column_name 
                END,
                bronze_type,
                CASE 
                    WHEN original_column_name = '{request.original_column_name}' 
                    THEN {f"'{request.inferred_type}'" if request.inferred_type is not None else 'inferred_type'}
                    ELSE inferred_type 
                END,
                type_confidence,
                pattern_matched,
                reasoning,
                cardinality,
                null_count,
                null_percentage,
                CASE 
                    WHEN original_column_name = '{request.original_column_name}' 
                    THEN {f"'{request.classification}'" if request.classification is not None else 'classification'}
                    ELSE classification 
                END,
                classification_confidence,
                classification_reasoning,
                is_primary_key_candidate,
                data_quality_score,
                cardinality_ratio,
                sample_values,
                '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
                {new_version}
            FROM metadata.discover
            WHERE original_table_name = '{request.original_table_name}'  -- Update ALL columns for this table
            """
        else:
            # Update only the specific column (no table name change)
            insert_query = f"""
            INSERT INTO metadata.discover (
                original_table_name,
                new_table_name,
                original_column_name,
                new_column_name,
                bronze_type,
                inferred_type,
                type_confidence,
                pattern_matched,
                reasoning,
                cardinality,
                null_count,
                null_percentage,
                classification,
                classification_confidence,
                classification_reasoning,
                is_primary_key_candidate,
                data_quality_score,
                cardinality_ratio,
                sample_values,
                analysis_timestamp,
                version
            )
            SELECT 
                original_table_name,
                new_table_name,
                original_column_name,
                {update_values[0] if request.new_column_name is not None else 'new_column_name'},
                bronze_type,
                {update_values[1] if request.inferred_type is not None else 'inferred_type'},
                type_confidence,
                pattern_matched,
                reasoning,
                cardinality,
                null_count,
                null_percentage,
                {update_values[2] if request.classification is not None else 'classification'},
                classification_confidence,
                classification_reasoning,
                is_primary_key_candidate,
                data_quality_score,
                cardinality_ratio,
                sample_values,
                '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}',
                {new_version}
            FROM metadata.discover
            WHERE original_table_name = '{request.original_table_name}' 
            AND original_column_name = '{request.original_column_name}'
            """
        
        # Execute the upsert
        result = db_manager.execute_query(insert_query)
        
        # Determine the scope of the update
        if request.new_table_name is not None:
            message = f"Updated table name for all columns in {request.original_table_name} to {request.new_table_name}"
            if any([request.new_column_name, request.inferred_type, request.classification]):
                message += f" and updated specific fields for {request.original_column_name}"
        else:
            message = f"Updated metadata for {request.original_table_name}.{request.original_column_name}"
        
        return {
            "status": "success",
            "message": message,
            "updated_fields": update_fields[:-2],  # Exclude version and timestamp
            "new_version": new_version,
            "query": insert_query
        }
        
    except Exception as e:
        logger.error(f"Error editing discover metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@discover_router.post("/export/metadata")
async def export_metadata(request: DiscoveryRequest):
    """Export discovery metadata to JSON file."""
    try:
        # Perform schema analysis
        schema_analysis = await analyze_bronze_schema(request)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"discovery_metadata_{timestamp}.json"
        
        # Save to file
        with open(filename, 'w') as f:
            json.dump(schema_analysis, f, indent=2, default=str)
        
        logger.info(f"Discovery metadata exported to {filename}")
        
        return {
            "status": "success",
            "filename": filename,
            "export_timestamp": datetime.now().isoformat(),
            "tables_analyzed": schema_analysis["total_tables"],
            "total_columns": schema_analysis["schema_summary"]["total_columns"],
            "fact_columns": schema_analysis["schema_summary"]["total_fact_columns"],
            "dimension_columns": schema_analysis["schema_summary"]["total_dimension_columns"],
            "primary_key_candidates": schema_analysis["schema_summary"]["total_primary_key_candidates"]
        }
        
    except Exception as e:
        logger.error(f"Error exporting metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))