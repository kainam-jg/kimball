"""
Metadata Catalog for ClickHouse Data Discovery

This module provides comprehensive analysis of ClickHouse tables including:
- Column metadata (names, types, cardinality)
- Null value analysis
- Fact vs Dimension column classification
- Data quality metrics
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from connection import get_clickhouse_connection
import logging
from datetime import datetime
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetadataCatalog:
    """
    A comprehensive metadata catalog for ClickHouse data discovery and analysis.
    """
    
    def __init__(self, config_file: str = "config.json"):
        """
        Initialize the metadata catalog.
        
        Args:
            config_file (str): Path to the configuration file
        """
        self.conn = get_clickhouse_connection(config_file)
        self.catalog = {}
        self.fact_columns = []
        self.dimension_columns = []
        
    def connect(self) -> bool:
        """
        Connect to the ClickHouse database.
        
        Returns:
            bool: True if connection successful
        """
        return self.conn.connect()
    
    def disconnect(self):
        """Disconnect from the database."""
        self.conn.disconnect()
    
    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of a single table.
        
        Args:
            table_name (str): Name of the table to analyze
            
        Returns:
            Dict[str, Any]: Complete metadata for the table
        """
        logger.info(f"Analyzing table: {table_name}")
        
        # Get basic table info
        table_info = self.conn.get_table_info(table_name)
        if not table_info:
            logger.error(f"Failed to get basic info for table {table_name}")
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
        
        return metadata
    
    def _analyze_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Analyze all columns in a table for metadata.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            List[Dict[str, Any]]: Detailed column metadata
        """
        columns_metadata = []
        
        # Get column schema
        schema = self.conn.get_table_schema(table_name)
        if not schema:
            return columns_metadata
        
        for col in schema:
            col_name = col["name"]
            col_type = col["type"]
            
            logger.info(f"Analyzing column: {col_name}")
            
            # Get cardinality
            cardinality = self._get_cardinality(table_name, col_name)
            
            # Get null count
            null_count = self._get_null_count(table_name, col_name)
            
            # Get sample values for type analysis
            sample_values = self._get_sample_values(table_name, col_name)
            
            # Classify column as fact or dimension
            classification = self._classify_column(col_name, col_type, cardinality, sample_values)
            
            # Get row count for cardinality ratio calculation
            row_count = self.conn.execute_query(f"SELECT COUNT(*) FROM {table_name}")[0][0] if self.conn.execute_query(f"SELECT COUNT(*) FROM {table_name}")[0][0] > 0 else 0
            
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
        
        return columns_metadata
    
    def _get_cardinality(self, table_name: str, column_name: str) -> int:
        """
        Get the cardinality (number of unique values) for a column.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column
            
        Returns:
            int: Number of unique values
        """
        try:
            # Escape column name with backticks for special characters
            escaped_column = f"`{column_name}`"
            query = f"SELECT COUNT(DISTINCT {escaped_column}) FROM {table_name}"
            result = self.conn.execute_query(query)
            return result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get cardinality for {column_name}: {str(e)}")
            return 0
    
    def _get_null_count(self, table_name: str, column_name: str) -> int:
        """
        Get the number of null values in a column.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column
            
        Returns:
            int: Number of null values
        """
        try:
            # Escape column name with backticks for special characters
            escaped_column = f"`{column_name}`"
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {escaped_column} IS NULL"
            result = self.conn.execute_query(query)
            return result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to get null count for {column_name}: {str(e)}")
            return 0
    
    def _get_sample_values(self, table_name: str, column_name: str) -> List[Any]:
        """
        Get sample values from a column for analysis.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column
            
        Returns:
            List[Any]: Sample values
        """
        try:
            # Escape column name with backticks for special characters
            escaped_column = f"`{column_name}`"
            query = f"SELECT DISTINCT {escaped_column} FROM {table_name} WHERE {escaped_column} IS NOT NULL LIMIT 10"
            result = self.conn.execute_query(query)
            return [row[0] for row in result] if result else []
        except Exception as e:
            logger.warning(f"Failed to get sample values for {column_name}: {str(e)}")
            return []
    
    def _classify_column(self, col_name: str, col_type: str, cardinality: int, sample_values: List[Any]) -> str:
        """
        Classify a column as either 'fact' or 'dimension'.
        
        Args:
            col_name (str): Column name
            col_type (str): Column data type
            cardinality (int): Number of unique values
            sample_values (List[Any]): Sample values for analysis
            
        Returns:
            str: 'fact' or 'dimension'
        """
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
        """
        Determine if a column could be a primary key candidate.
        
        Args:
            col_name (str): Column name
            cardinality (int): Number of unique values
            null_count (int): Number of null values
            col_type (str): Column data type
            row_count (int): Total number of rows in table
            classification (str): Column classification ('fact' or 'dimension')
            
        Returns:
            bool: True if could be primary key
        """
        # Primary keys should be DIMENSION columns, not fact columns
        if classification != 'dimension':
            return False
        
        # Basic requirements: no nulls
        if null_count > 0:
            return False
        
        # Calculate cardinality ratio (uniqueness)
        cardinality_ratio = cardinality / row_count if row_count > 0 else 0
        
        # Primary key criteria:
        # 1. Must be a dimension column (not a fact/measure)
        # 2. No null values
        # 3. High cardinality (at least 80% of rows have unique values)
        # 4. Reasonable data type for keys
        # 5. Minimum cardinality threshold
        
        is_high_cardinality = cardinality_ratio >= 0.8  # 80% uniqueness
        is_reasonable_cardinality = cardinality >= 100  # Minimum threshold
        is_suitable_type = self._is_suitable_key_type(col_type)
        
        return is_high_cardinality and is_reasonable_cardinality and is_suitable_type
    
    def _is_suitable_key_type(self, col_type: str) -> bool:
        """
        Determine if a column type is suitable for a primary key.
        
        Args:
            col_type (str): Column data type
            
        Returns:
            bool: True if suitable for primary key
        """
        # Suitable types for primary keys
        suitable_types = [
            'Int8', 'Int16', 'Int32', 'Int64',
            'UInt8', 'UInt16', 'UInt32', 'UInt64',
            'String', 'FixedString'
        ]
        
        # Check if any suitable type is in the column type
        return any(suitable_type in col_type for suitable_type in suitable_types)
    
    def _calculate_data_quality_score(self, null_count: int, cardinality: int) -> float:
        """
        Calculate a data quality score for a column.
        
        Args:
            null_count (int): Number of null values
            cardinality (int): Number of unique values
            
        Returns:
            float: Quality score between 0 and 1
        """
        # Simple quality score based on null percentage and cardinality
        if cardinality == 0:
            return 0.0
        
        # Higher cardinality is generally better (more diverse data)
        cardinality_score = min(cardinality / 1000, 1.0)
        
        # Lower null percentage is better
        null_score = 1.0 - (null_count / max(cardinality, 1))
        
        return (cardinality_score + null_score) / 2
    
    def _generate_table_summary(self, columns: List[Dict[str, Any]], row_count: int) -> Dict[str, Any]:
        """
        Generate a summary of table characteristics.
        
        Args:
            columns (List[Dict[str, Any]]): Column metadata
            row_count (int): Total number of rows
            
        Returns:
            Dict[str, Any]: Table summary
        """
        fact_cols = [col for col in columns if col["classification"] == "fact"]
        dim_cols = [col for col in columns if col["classification"] == "dimension"]
        
        return {
            "total_columns": len(columns),
            "fact_columns": len(fact_cols),
            "dimension_columns": len(dim_cols),
            "avg_data_quality": np.mean([col["data_quality_score"] for col in columns]) if columns else 0,
            "high_quality_columns": len([col for col in columns if col["data_quality_score"] > 0.8]),
            "primary_key_candidates": len([col for col in columns if col["is_primary_key_candidate"]]),
            "columns_with_nulls": len([col for col in columns if col["null_count"] > 0])
        }
    
    def build_catalog(self, schema_name: str = "bronze") -> Dict[str, Any]:
        """
        Build a complete metadata catalog for all tables in a schema.
        
        Args:
            schema_name (str): Name of the schema to analyze
            
        Returns:
            Dict[str, Any]: Complete metadata catalog
        """
        logger.info(f"Building metadata catalog for schema: {schema_name}")
        
        # Get all tables in the schema
        tables = self.conn.get_tables()
        if not tables:
            logger.error(f"No tables found in schema {schema_name}")
            return {}
        
        catalog = {
            "schema_name": schema_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "total_tables": len(tables),
            "tables": {}
        }
        
        # Analyze each table
        for table in tables:
            logger.info(f"Processing table: {table}")
            try:
                table_metadata = self.analyze_table(table)
                catalog["tables"][table] = table_metadata
            except Exception as e:
                logger.error(f"Failed to analyze table {table}: {str(e)}")
                catalog["tables"][table] = {"error": str(e)}
        
        # Generate schema-level summary
        catalog["schema_summary"] = self._generate_schema_summary(catalog["tables"])
        
        return catalog
    
    def _generate_schema_summary(self, tables: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a summary of the entire schema.
        
        Args:
            tables (Dict[str, Any]): All table metadata
            
        Returns:
            Dict[str, Any]: Schema summary
        """
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
            logger.info(f"Catalog saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save catalog: {str(e)}")
    
    def print_catalog_summary(self, catalog: Dict[str, Any]):
        """
        Print a human-readable summary of the catalog.
        
        Args:
            catalog (Dict[str, Any]): The catalog to summarize
        """
        print(f"\n=== Metadata Catalog for Schema: {catalog['schema_name']} ===")
        print(f"Analysis Time: {catalog['analysis_timestamp']}")
        print(f"Total Tables: {catalog['total_tables']}")
        
        schema_summary = catalog.get('schema_summary', {})
        print(f"Total Columns: {schema_summary.get('total_columns', 0)}")
        print(f"Fact Columns: {schema_summary.get('total_fact_columns', 0)}")
        print(f"Dimension Columns: {schema_summary.get('total_dimension_columns', 0)}")
        print(f"Fact/Dimension Ratio: {schema_summary.get('fact_dimension_ratio', 0):.2f}")
        
        print(f"\n=== Table Details ===")
        for table_name, table_data in catalog['tables'].items():
            if 'error' in table_data:
                print(f"ERROR {table_name}: Error - {table_data['error']}")
                continue
                
        print(f"\nTable: {table_name}")
        print(f"   Rows: {table_data.get('row_count', 0):,}")
        print(f"   Columns: {table_data.get('column_count', 0)}")
        print(f"   Fact Columns: {len(table_data.get('fact_columns', []))}")
        print(f"   Dimension Columns: {len(table_data.get('dimension_columns', []))}")
        
        summary = table_data.get('summary', {})
        print(f"   Data Quality Score: {summary.get('avg_data_quality', 0):.2f}")
        print(f"   Primary Key Candidates: {summary.get('primary_key_candidates', 0)}")


def main():
    """Main function to demonstrate the metadata catalog."""
    catalog_builder = MetadataCatalog()
    
    if not catalog_builder.connect():
        print("Failed to connect to ClickHouse")
        return
    
    print("Building metadata catalog for bronze schema...")
    catalog = catalog_builder.build_catalog("bronze")
    
    # Print summary
    catalog_builder.print_catalog_summary(catalog)
    
    # Save catalog
    catalog_builder.save_catalog(catalog)
    
    catalog_builder.disconnect()
    print("\nMetadata catalog analysis complete!")


if __name__ == "__main__":
    main()
