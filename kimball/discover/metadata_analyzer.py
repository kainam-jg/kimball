"""
Enhanced Metadata Analyzer for KIMBALL Discover Phase

This module extends the existing metadata analysis functionality
with improved error handling, logging, and API integration.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import json
import logging

from ..core.database import DatabaseManager
from ..core.logger import Logger

# Import existing functionality
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from connection import get_clickhouse_connection

class MetadataAnalyzer:
    """
    Enhanced metadata analyzer for KIMBALL Discover phase.
    
    Extends the existing metadata analysis with improved error handling,
    logging, and API integration capabilities.
    """
    
    def __init__(self, config_file: str = "config.json"):
        """Initialize the metadata analyzer."""
        self.config_file = config_file
        self.conn = get_clickhouse_connection(config_file)
        self.catalog = {}
        self.fact_columns = []
        self.dimension_columns = []
        self.logger = Logger("metadata_analyzer")
        
    def connect(self) -> bool:
        """
        Connect to the ClickHouse database.
        
        Returns:
            bool: True if connection successful
        """
        try:
            success = self.conn.connect()
            if success:
                self.logger.info("Successfully connected to ClickHouse")
            else:
                self.logger.error("Failed to connect to ClickHouse")
            return success
        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            return False
    
    def disconnect(self):
        """Disconnect from the database."""
        try:
            self.conn.disconnect()
            self.logger.info("Disconnected from ClickHouse")
        except Exception as e:
            self.logger.error(f"Disconnect error: {str(e)}")
    
    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of a single table.
        
        Args:
            table_name (str): Name of the table to analyze
            
        Returns:
            Dict[str, Any]: Complete metadata for the table
        """
        self.logger.info(f"Analyzing table: {table_name}")
        
        try:
            # Get basic table info
            table_info = self.conn.get_table_info(table_name)
            if not table_info:
                self.logger.error(f"Failed to get basic info for table {table_name}")
                return {}
            
            # Get detailed column analysis
            column_analysis = self._analyze_columns(table_name)
            
            # Combine all metadata
            metadata = {
                "table_name": table_name,
                "row_count": table_info["row_count"],
                "column_count": len(table_info["columns"]),
                "columns": column_analysis,
                "analysis_timestamp": datetime.now().isoformat(),
                "fact_columns": [col["name"] for col in column_analysis if col["classification"] == "fact"],
                "dimension_columns": [col["name"] for col in column_analysis if col["classification"] == "dimension"],
                "summary": self._generate_table_summary(column_analysis, table_info["row_count"])
            }
            
            self.logger.info(f"Successfully analyzed table {table_name}")
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error analyzing table {table_name}: {str(e)}")
            return {"error": str(e)}
    
    def _analyze_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Analyze all columns in a table for metadata.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            List[Dict[str, Any]]: Detailed column metadata
        """
        columns_metadata = []
        
        try:
            # Get column schema
            schema = self.conn.get_table_schema(table_name)
            if not schema:
                self.logger.warning(f"No schema found for table {table_name}")
                return columns_metadata
            
            for col in schema:
                col_name = col["name"]
                col_type = col["type"]
                
                self.logger.debug(f"Analyzing column: {col_name}")
                
                try:
                    # Get cardinality
                    cardinality = self._get_cardinality(table_name, col_name)
                    
                    # Get null count
                    null_count = self._get_null_count(table_name, col_name)
                    
                    # Get sample values for type analysis
                    sample_values = self._get_sample_values(table_name, col_name)
                    
                    # Classify column as fact or dimension
                    classification = self._classify_column(col_name, col_type, cardinality, sample_values)
                    
                    # Get row count for cardinality ratio calculation
                    row_count = self.conn.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = row_count[0][0] if row_count and row_count[0][0] > 0 else 0
                    
                    column_metadata = {
                        "name": col_name,
                        "type": col_type,
                        "cardinality": cardinality,
                        "null_count": null_count,
                        "null_percentage": (null_count / row_count) * 100 if row_count > 0 else 0,
                        "classification": classification,
                        "sample_values": sample_values[:5],  # First 5 sample values
                        "is_primary_key_candidate": self._is_primary_key_candidate(col_name, cardinality, null_count, col_type, row_count, classification),
                        "data_quality_score": self._calculate_data_quality_score(null_count, cardinality),
                        "cardinality_ratio": cardinality / row_count if row_count > 0 else 0
                    }
                    
                    columns_metadata.append(column_metadata)
                    
                except Exception as e:
                    self.logger.warning(f"Error analyzing column {col_name}: {str(e)}")
                    # Add error column metadata
                    columns_metadata.append({
                        "name": col_name,
                        "type": col_type,
                        "error": str(e),
                        "classification": "unknown"
                    })
            
            return columns_metadata
            
        except Exception as e:
            self.logger.error(f"Error analyzing columns for table {table_name}: {str(e)}")
            return []
    
    def _get_cardinality(self, table_name: str, column_name: str) -> int:
        """Get the cardinality (number of unique values) for a column."""
        try:
            escaped_column = f"`{column_name}`"
            query = f"SELECT COUNT(DISTINCT {escaped_column}) FROM {table_name}"
            result = self.conn.execute_query(query)
            return result[0][0] if result else 0
        except Exception as e:
            self.logger.warning(f"Failed to get cardinality for {column_name}: {str(e)}")
            return 0
    
    def _get_null_count(self, table_name: str, column_name: str) -> int:
        """Get the number of null values in a column."""
        try:
            escaped_column = f"`{column_name}`"
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {escaped_column} IS NULL"
            result = self.conn.execute_query(query)
            return result[0][0] if result else 0
        except Exception as e:
            self.logger.warning(f"Failed to get null count for {column_name}: {str(e)}")
            return 0
    
    def _get_sample_values(self, table_name: str, column_name: str) -> List[Any]:
        """Get sample values from a column for analysis."""
        try:
            escaped_column = f"`{column_name}`"
            query = f"SELECT DISTINCT {escaped_column} FROM {table_name} WHERE {escaped_column} IS NOT NULL LIMIT 10"
            result = self.conn.execute_query(query)
            return [row[0] for row in result] if result else []
        except Exception as e:
            self.logger.warning(f"Failed to get sample values for {column_name}: {str(e)}")
            return []
    
    def _classify_column(self, col_name: str, col_type: str, cardinality: int, sample_values: List[Any]) -> str:
        """Classify a column as either 'fact' or 'dimension'."""
        # Fact columns: numeric types that represent measures
        numeric_types = ['Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64',
                         'Float32', 'Float64', 'Decimal', 'Decimal32', 'Decimal64', 'Decimal128']
        
        # Check if it's a numeric type
        is_numeric = any(num_type in col_type for num_type in numeric_types)
        
        # High cardinality numeric columns are likely facts
        if is_numeric and cardinality > 100:
            return "fact"
        
        # Date/time columns are typically dimensions
        if any(date_type in col_type.lower() for date_type in ['date', 'datetime', 'timestamp']):
            return "dimension"
        
        # String columns are typically dimensions
        if 'String' in col_type or 'FixedString' in col_type:
            return "dimension"
        
        # Low cardinality numeric columns might be dimensions (like status codes)
        if is_numeric and cardinality <= 10:
            return "dimension"
        
        # Default classification based on type
        return "fact" if is_numeric else "dimension"
    
    def _is_primary_key_candidate(self, col_name: str, cardinality: int, null_count: int, col_type: str, row_count: int, classification: str) -> bool:
        """Determine if a column could be a primary key candidate."""
        # Primary keys should be DIMENSION columns, not fact columns
        if classification != 'dimension':
            return False
        
        # Basic requirements: no nulls
        if null_count > 0:
            return False
        
        # Calculate cardinality ratio (uniqueness)
        cardinality_ratio = cardinality / row_count if row_count > 0 else 0
        
        # Primary key criteria:
        is_high_cardinality = cardinality_ratio >= 0.8  # 80% uniqueness
        is_reasonable_cardinality = cardinality >= 100  # Minimum threshold
        is_suitable_type = self._is_suitable_key_type(col_type)
        
        return is_high_cardinality and is_reasonable_cardinality and is_suitable_type
    
    def _is_suitable_key_type(self, col_type: str) -> bool:
        """Determine if a column type is suitable for a primary key."""
        suitable_types = [
            'Int8', 'Int16', 'Int32', 'Int64',
            'UInt8', 'UInt16', 'UInt32', 'UInt64',
            'String', 'FixedString'
        ]
        
        return any(suitable_type in col_type for suitable_type in suitable_types)
    
    def _calculate_data_quality_score(self, null_count: int, cardinality: int) -> float:
        """Calculate a data quality score for a column."""
        if cardinality == 0:
            return 0.0
        
        # Higher cardinality is generally better (more diverse data)
        cardinality_score = min(cardinality / 1000, 1.0)
        
        # Lower null percentage is better
        null_score = 1.0 - (null_count / max(cardinality, 1))
        
        return (cardinality_score + null_score) / 2
    
    def _generate_table_summary(self, columns: List[Dict[str, Any]], row_count: int) -> Dict[str, Any]:
        """Generate a summary of table characteristics."""
        fact_cols = [col for col in columns if col.get("classification") == "fact"]
        dim_cols = [col for col in columns if col.get("classification") == "dimension"]
        
        return {
            "total_columns": len(columns),
            "fact_columns": len(fact_cols),
            "dimension_columns": len(dim_cols),
            "avg_data_quality": np.mean([col.get("data_quality_score", 0) for col in columns]) if columns else 0,
            "high_quality_columns": len([col for col in columns if col.get("data_quality_score", 0) > 0.8]),
            "primary_key_candidates": len([col for col in columns if col.get("is_primary_key_candidate", False)]),
            "columns_with_nulls": len([col for col in columns if col.get("null_count", 0) > 0])
        }
    
    def build_catalog(self, schema_name: str = "bronze") -> Dict[str, Any]:
        """
        Build a complete metadata catalog for all tables in a schema.
        
        Args:
            schema_name (str): Name of the schema to analyze
            
        Returns:
            Dict[str, Any]: Complete metadata catalog
        """
        self.logger.info(f"Building metadata catalog for schema: {schema_name}")
        
        try:
            # Get all tables in the schema
            tables = self.conn.get_tables()
            if not tables:
                self.logger.error(f"No tables found in schema {schema_name}")
                return {}
            
            catalog = {
                "schema_name": schema_name,
                "analysis_timestamp": datetime.now().isoformat(),
                "total_tables": len(tables),
                "tables": {}
            }
            
            # Analyze each table
            for table in tables:
                self.logger.info(f"Processing table: {table}")
                try:
                    table_metadata = self.analyze_table(table)
                    catalog["tables"][table] = table_metadata
                except Exception as e:
                    self.logger.error(f"Failed to analyze table {table}: {str(e)}")
                    catalog["tables"][table] = {"error": str(e)}
            
            # Generate schema-level summary
            catalog["schema_summary"] = self._generate_schema_summary(catalog["tables"])
            
            self.logger.info(f"Successfully built catalog for {len(tables)} tables")
            return catalog
            
        except Exception as e:
            self.logger.error(f"Error building catalog: {str(e)}")
            return {}
    
    def _generate_schema_summary(self, tables: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a summary of the entire schema."""
        total_tables = len(tables)
        total_columns = sum(len(table.get("columns", [])) for table in tables.values())
        total_fact_cols = sum(len(table.get("fact_columns", [])) for table in tables.values())
        total_dim_cols = sum(len(table.get("dimension_columns", [])) for table in tables.values())
        
        return {
            "total_tables": total_tables,
            "total_columns": total_columns,
            "total_fact_columns": total_fact_cols,
            "total_dimension_columns": total_dim_cols,
            "avg_columns_per_table": total_columns / total_tables if total_tables > 0 else 0,
            "fact_dimension_ratio": total_fact_cols / total_dim_cols if total_dim_cols > 0 else 0
        }
    
    def save_catalog(self, catalog: Dict[str, Any], filename: str = "metadata_catalog.json"):
        """
        Save the metadata catalog to a JSON file.
        
        Args:
            catalog (Dict[str, Any]): The catalog to save
            filename (str): Output filename
        """
        try:
            with open(filename, 'w') as f:
                json.dump(catalog, f, indent=2, default=str)
            self.logger.info(f"Catalog saved to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save catalog: {str(e)}")
            raise
