"""
KIMBALL Data Transformers

This module provides data transformation functionality:
- Data type conversion and validation
- Schema mapping and flattening
- Data cleansing and normalization
- Format conversion (JSON, CSV, Parquet, etc.)
"""

from typing import Dict, List, Any, Optional, Union
import pandas as pd
import json
from datetime import datetime
import uuid

from ..core.logger import Logger

class DataTransformer:
    """
    Data transformer for various data formats and structures.
    
    Provides unified transformation interface for data cleaning,
    type conversion, and schema mapping.
    """
    
    def __init__(self):
        """Initialize the data transformer."""
        self.logger = Logger("data_transformer")
        self.transformations = {}
    
    def transform_data(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform data according to configuration.
        
        Args:
            data (List[Dict[str, Any]]): Input data
            config (Dict[str, Any]): Transformation configuration
            
        Returns:
            Dict[str, Any]: Transformed data
        """
        try:
            self.logger.info("Starting data transformation")
            
            # Generate transformation ID
            transformation_id = f"trans_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
            
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(data)
            
            # Apply transformations
            transformed_df = self._apply_transformations(df, config)
            
            # Convert back to list of dictionaries
            transformed_data = transformed_df.to_dict('records')
            
            # Store transformation result
            self.transformations[transformation_id] = {
                "input_count": len(data),
                "output_count": len(transformed_data),
                "config": config,
                "timestamp": datetime.now().isoformat(),
                "status": "completed"
            }
            
            self.logger.info(f"Data transformation completed: {transformation_id}")
            
            return {
                "transformation_id": transformation_id,
                "data": transformed_data,
                "record_count": len(transformed_data),
                "columns": list(transformed_data[0].keys()) if transformed_data else [],
                "status": "completed"
            }
            
        except Exception as e:
            self.logger.error(f"Error transforming data: {str(e)}")
            raise
    
    def _apply_transformations(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Apply transformations to DataFrame."""
        try:
            # Start with original DataFrame
            transformed_df = df.copy()
            
            # Apply column mappings
            if "column_mappings" in config:
                transformed_df = self._apply_column_mappings(transformed_df, config["column_mappings"])
            
            # Apply data type conversions
            if "type_conversions" in config:
                transformed_df = self._apply_type_conversions(transformed_df, config["type_conversions"])
            
            # Apply data cleansing
            if "cleansing" in config:
                transformed_df = self._apply_data_cleansing(transformed_df, config["cleansing"])
            
            # Apply filtering
            if "filters" in config:
                transformed_df = self._apply_filters(transformed_df, config["filters"])
            
            # Apply aggregations
            if "aggregations" in config:
                transformed_df = self._apply_aggregations(transformed_df, config["aggregations"])
            
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Error applying transformations: {str(e)}")
            raise
    
    def _apply_column_mappings(self, df: pd.DataFrame, mappings: Dict[str, str]) -> pd.DataFrame:
        """Apply column name mappings."""
        try:
            # Rename columns according to mappings
            df_renamed = df.rename(columns=mappings)
            
            # Select only mapped columns if specified
            if "select_columns" in mappings:
                df_renamed = df_renamed[mappings["select_columns"]]
            
            return df_renamed
            
        except Exception as e:
            self.logger.error(f"Error applying column mappings: {str(e)}")
            return df
    
    def _apply_type_conversions(self, df: pd.DataFrame, conversions: Dict[str, str]) -> pd.DataFrame:
        """Apply data type conversions."""
        try:
            for column, target_type in conversions.items():
                if column in df.columns:
                    if target_type == "string":
                        df[column] = df[column].astype(str)
                    elif target_type == "int":
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    elif target_type == "float":
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    elif target_type == "datetime":
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    elif target_type == "boolean":
                        df[column] = df[column].astype(bool)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error applying type conversions: {str(e)}")
            return df
    
    def _apply_data_cleansing(self, df: pd.DataFrame, cleansing_config: Dict[str, Any]) -> pd.DataFrame:
        """Apply data cleansing operations."""
        try:
            # Handle null values
            if "null_handling" in cleansing_config:
                null_config = cleansing_config["null_handling"]
                
                if null_config.get("strategy") == "drop":
                    df = df.dropna()
                elif null_config.get("strategy") == "fill":
                    fill_value = null_config.get("value", "")
                    df = df.fillna(fill_value)
                elif null_config.get("strategy") == "forward_fill":
                    df = df.fillna(method='ffill')
                elif null_config.get("strategy") == "backward_fill":
                    df = df.fillna(method='bfill')
            
            # Remove duplicates
            if cleansing_config.get("remove_duplicates", False):
                df = df.drop_duplicates()
            
            # Trim whitespace from string columns
            if cleansing_config.get("trim_whitespace", False):
                string_columns = df.select_dtypes(include=['object']).columns
                df[string_columns] = df[string_columns].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            
            # Remove special characters
            if "remove_special_chars" in cleansing_config:
                chars_to_remove = cleansing_config["remove_special_chars"]
                string_columns = df.select_dtypes(include=['object']).columns
                for col in string_columns:
                    df[col] = df[col].str.replace(f"[{chars_to_remove}]", "", regex=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error applying data cleansing: {str(e)}")
            return df
    
    def _apply_filters(self, df: pd.DataFrame, filters: List[Dict[str, Any]]) -> pd.DataFrame:
        """Apply data filters."""
        try:
            for filter_config in filters:
                column = filter_config.get("column")
                operator = filter_config.get("operator")
                value = filter_config.get("value")
                
                if column in df.columns:
                    if operator == "equals":
                        df = df[df[column] == value]
                    elif operator == "not_equals":
                        df = df[df[column] != value]
                    elif operator == "greater_than":
                        df = df[df[column] > value]
                    elif operator == "less_than":
                        df = df[df[column] < value]
                    elif operator == "contains":
                        df = df[df[column].str.contains(value, na=False)]
                    elif operator == "not_contains":
                        df = df[~df[column].str.contains(value, na=False)]
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error applying filters: {str(e)}")
            return df
    
    def _apply_aggregations(self, df: pd.DataFrame, aggregations: Dict[str, Any]) -> pd.DataFrame:
        """Apply data aggregations."""
        try:
            group_by = aggregations.get("group_by", [])
            agg_functions = aggregations.get("functions", {})
            
            if group_by and agg_functions:
                df = df.groupby(group_by).agg(agg_functions).reset_index()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error applying aggregations: {str(e)}")
            return df
    
    def flatten_nested_data(self, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Flatten nested JSON data structures."""
        try:
            flattened_data = []
            
            for record in data:
                flattened_record = self._flatten_record(record, config)
                flattened_data.append(flattened_record)
            
            return flattened_data
            
        except Exception as e:
            self.logger.error(f"Error flattening data: {str(e)}")
            raise
    
    def _flatten_record(self, record: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten a single record."""
        try:
            flattened = {}
            separator = config.get("separator", "_")
            max_depth = config.get("max_depth", 10)
            
            def flatten_dict(d, parent_key="", depth=0):
                if depth > max_depth:
                    return
                
                items = []
                for k, v in d.items():
                    new_key = f"{parent_key}{separator}{k}" if parent_key else k
                    
                    if isinstance(v, dict):
                        items.extend(flatten_dict(v, new_key, depth + 1).items())
                    elif isinstance(v, list):
                        for i, item in enumerate(v):
                            if isinstance(item, dict):
                                items.extend(flatten_dict(item, f"{new_key}{separator}{i}", depth + 1).items())
                            else:
                                items.append((f"{new_key}{separator}{i}", item))
                    else:
                        items.append((new_key, v))
                
                return dict(items)
            
            return flatten_dict(record)
            
        except Exception as e:
            self.logger.error(f"Error flattening record: {str(e)}")
            return record
    
    def get_transformation_status(self, transformation_id: str) -> Dict[str, Any]:
        """Get status of a transformation."""
        if transformation_id in self.transformations:
            return self.transformations[transformation_id]
        else:
            return {"error": "Transformation not found"}
    
    def list_transformations(self) -> List[Dict[str, Any]]:
        """List all transformations."""
        return list(self.transformations.values())
