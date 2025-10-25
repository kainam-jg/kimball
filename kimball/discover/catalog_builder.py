"""
KIMBALL Catalog Builder

This module provides catalog building and summary generation functionality
for the Discover phase.
"""

import json
import pandas as pd
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..core.logger import Logger

class CatalogBuilder:
    """
    Catalog builder for generating summaries and reports from metadata catalogs.
    """
    
    def __init__(self):
        """Initialize the catalog builder."""
        self.logger = Logger("catalog_builder")
    
    def generate_summary(self, catalog: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a comprehensive summary from a metadata catalog.
        
        Args:
            catalog (Dict[str, Any]): The metadata catalog
            
        Returns:
            Dict[str, Any]: Summary report
        """
        try:
            self.logger.info("Generating catalog summary")
            
            summary = {
                "schema_name": catalog.get("schema_name", "unknown"),
                "analysis_timestamp": catalog.get("analysis_timestamp", datetime.now().isoformat()),
                "total_tables": catalog.get("total_tables", 0),
                "schema_summary": catalog.get("schema_summary", {}),
                "table_summaries": [],
                "fact_columns": [],
                "dimension_columns": [],
                "primary_key_candidates": [],
                "data_quality_issues": []
            }
            
            # Process each table
            for table_name, table_data in catalog.get("tables", {}).items():
                if "error" in table_data:
                    continue
                
                # Table summary
                table_summary = {
                    "table_name": table_name,
                    "row_count": table_data.get("row_count", 0),
                    "column_count": table_data.get("column_count", 0),
                    "fact_columns": len(table_data.get("fact_columns", [])),
                    "dimension_columns": len(table_data.get("dimension_columns", [])),
                    "data_quality": table_data.get("summary", {}).get("avg_data_quality", 0),
                    "primary_key_candidates": table_data.get("summary", {}).get("primary_key_candidates", 0)
                }
                summary["table_summaries"].append(table_summary)
                
                # Process columns
                for column in table_data.get("columns", []):
                    col_info = {
                        "table": table_name,
                        "column": column.get("name", "unknown"),
                        "type": column.get("type", "unknown"),
                        "cardinality": column.get("cardinality", 0),
                        "null_percentage": column.get("null_percentage", 0),
                        "quality_score": column.get("data_quality_score", 0),
                        "classification": column.get("classification", "unknown")
                    }
                    
                    if column.get("classification") == "fact":
                        summary["fact_columns"].append(col_info)
                    else:
                        summary["dimension_columns"].append(col_info)
                    
                    # Primary key candidates
                    if column.get("is_primary_key_candidate", False):
                        summary["primary_key_candidates"].append({
                            "table": table_name,
                            "column": column.get("name", "unknown"),
                            "type": column.get("type", "unknown"),
                            "cardinality": column.get("cardinality", 0),
                            "quality_score": column.get("data_quality_score", 0)
                        })
                    
                    # Data quality issues
                    if column.get("data_quality_score", 0) < 0.5:
                        summary["data_quality_issues"].append({
                            "table": table_name,
                            "column": column.get("name", "unknown"),
                            "quality_score": column.get("data_quality_score", 0),
                            "null_percentage": column.get("null_percentage", 0)
                        })
            
            # Sort summaries
            summary["table_summaries"].sort(key=lambda x: x["row_count"], reverse=True)
            summary["fact_columns"].sort(key=lambda x: x["quality_score"], reverse=True)
            summary["dimension_columns"].sort(key=lambda x: x["quality_score"], reverse=True)
            summary["primary_key_candidates"].sort(key=lambda x: x["quality_score"], reverse=True)
            
            self.logger.info("Successfully generated catalog summary")
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating summary: {str(e)}")
            return {"error": str(e)}
    
    def generate_erd_data(self, catalog: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate ERD data from catalog for visualization.
        
        Args:
            catalog (Dict[str, Any]): The metadata catalog
            
        Returns:
            Dict[str, Any]: ERD data structure
        """
        try:
            self.logger.info("Generating ERD data")
            
            erd_data = {
                "tables": [],
                "relationships": [],
                "metadata": {
                    "total_tables": catalog.get("total_tables", 0),
                    "generated_at": datetime.now().isoformat()
                }
            }
            
            # Process tables
            for table_name, table_data in catalog.get("tables", {}).items():
                if "error" in table_data:
                    continue
                
                table_info = {
                    "name": table_name,
                    "columns": [],
                    "primary_keys": [],
                    "row_count": table_data.get("row_count", 0)
                }
                
                # Process columns
                for column in table_data.get("columns", []):
                    col_info = {
                        "name": column.get("name", "unknown"),
                        "type": column.get("type", "unknown"),
                        "is_primary_key": column.get("is_primary_key_candidate", False),
                        "classification": column.get("classification", "unknown")
                    }
                    table_info["columns"].append(col_info)
                    
                    if column.get("is_primary_key_candidate", False):
                        table_info["primary_keys"].append(column.get("name", "unknown"))
                
                erd_data["tables"].append(table_info)
            
            self.logger.info("Successfully generated ERD data")
            return erd_data
            
        except Exception as e:
            self.logger.error(f"Error generating ERD data: {str(e)}")
            return {"error": str(e)}
    
    def export_catalog(self, catalog: Dict[str, Any], format: str = "json") -> str:
        """
        Export catalog in specified format.
        
        Args:
            catalog (Dict[str, Any]): The metadata catalog
            format (str): Export format (json, csv, excel)
            
        Returns:
            str: Path to exported file
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if format.lower() == "json":
                filename = f"catalog_export_{timestamp}.json"
                with open(filename, 'w') as f:
                    json.dump(catalog, f, indent=2, default=str)
            
            elif format.lower() == "csv":
                filename = f"catalog_export_{timestamp}.csv"
                # Convert to DataFrame and export
                df_data = []
                for table_name, table_data in catalog.get("tables", {}).items():
                    if "error" in table_data:
                        continue
                    for column in table_data.get("columns", []):
                        df_data.append({
                            "table_name": table_name,
                            "column_name": column.get("name", "unknown"),
                            "type": column.get("type", "unknown"),
                            "cardinality": column.get("cardinality", 0),
                            "classification": column.get("classification", "unknown"),
                            "quality_score": column.get("data_quality_score", 0)
                        })
                
                df = pd.DataFrame(df_data)
                df.to_csv(filename, index=False)
            
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.logger.info(f"Catalog exported to {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Error exporting catalog: {str(e)}")
            raise
