"""
KIMBALL Quality Assessor

This module provides data quality assessment functionality
for the Discover phase.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import numpy as np

from ..core.logger import Logger

class QualityAssessor:
    """
    Data quality assessor for analyzing data quality issues in catalogs.
    """
    
    def __init__(self):
        """Initialize the quality assessor."""
        self.logger = Logger("quality_assessor")
    
    def assess_catalog_quality(self, catalog: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assess data quality for an entire catalog.
        
        Args:
            catalog (Dict[str, Any]): The metadata catalog
            
        Returns:
            Dict[str, Any]: Quality assessment report
        """
        try:
            self.logger.info("Assessing catalog quality")
            
            quality_report = {
                "overall_score": 0.0,
                "high_quality_tables": 0,
                "medium_quality_tables": 0,
                "low_quality_tables": 0,
                "issues": [],
                "recommendations": [],
                "table_scores": {},
                "column_scores": {},
                "assessment_timestamp": datetime.now().isoformat()
            }
            
            table_scores = []
            all_issues = []
            
            # Assess each table
            for table_name, table_data in catalog.get("tables", {}).items():
                if "error" in table_data:
                    continue
                
                table_quality = self._assess_table_quality(table_name, table_data)
                table_scores.append(table_quality["score"])
                quality_report["table_scores"][table_name] = table_quality
                
                # Categorize table quality
                if table_quality["score"] > 0.8:
                    quality_report["high_quality_tables"] += 1
                elif table_quality["score"] > 0.5:
                    quality_report["medium_quality_tables"] += 1
                else:
                    quality_report["low_quality_tables"] += 1
                
                # Collect issues
                all_issues.extend(table_quality["issues"])
            
            # Calculate overall score
            if table_scores:
                quality_report["overall_score"] = np.mean(table_scores)
            
            # Sort and categorize issues
            quality_report["issues"] = sorted(all_issues, key=lambda x: x.get("severity", 0), reverse=True)
            
            # Generate recommendations
            quality_report["recommendations"] = self._generate_quality_recommendations(quality_report)
            
            self.logger.info(f"Quality assessment completed. Overall score: {quality_report['overall_score']:.2f}")
            return quality_report
            
        except Exception as e:
            self.logger.error(f"Error assessing catalog quality: {str(e)}")
            return {"error": str(e)}
    
    def _assess_table_quality(self, table_name: str, table_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assess quality for a single table.
        
        Args:
            table_name (str): Name of the table
            table_data (Dict[str, Any]): Table metadata
            
        Returns:
            Dict[str, Any]: Table quality assessment
        """
        issues = []
        column_scores = []
        
        # Analyze each column
        for column in table_data.get("columns", []):
            col_quality = self._assess_column_quality(table_name, column)
            column_scores.append(col_quality["score"])
            issues.extend(col_quality["issues"])
        
        # Calculate table score
        table_score = np.mean(column_scores) if column_scores else 0.0
        
        return {
            "table_name": table_name,
            "score": table_score,
            "issues": issues,
            "column_count": len(table_data.get("columns", [])),
            "row_count": table_data.get("row_count", 0)
        }
    
    def _assess_column_quality(self, table_name: str, column: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assess quality for a single column.
        
        Args:
            table_name (str): Name of the table
            column (Dict[str, Any]): Column metadata
            
        Returns:
            Dict[str, Any]: Column quality assessment
        """
        issues = []
        score = column.get("data_quality_score", 0.0)
        
        # Check for specific quality issues
        null_percentage = column.get("null_percentage", 0.0)
        cardinality = column.get("cardinality", 0)
        classification = column.get("classification", "unknown")
        
        # High null percentage
        if null_percentage > 50:
            issues.append({
                "type": "high_nulls",
                "severity": "high",
                "message": f"Column {column.get('name', 'unknown')} has {null_percentage:.1f}% null values",
                "table": table_name,
                "column": column.get("name", "unknown")
            })
        
        # Low cardinality for dimension columns
        if classification == "dimension" and cardinality < 10:
            issues.append({
                "type": "low_cardinality",
                "severity": "medium",
                "message": f"Dimension column {column.get('name', 'unknown')} has very low cardinality ({cardinality})",
                "table": table_name,
                "column": column.get("name", "unknown")
            })
        
        # Very low quality score
        if score < 0.3:
            issues.append({
                "type": "poor_quality",
                "severity": "high",
                "message": f"Column {column.get('name', 'unknown')} has very low quality score ({score:.2f})",
                "table": table_name,
                "column": column.get("name", "unknown")
            })
        
        # Missing primary key candidates
        if classification == "dimension" and not column.get("is_primary_key_candidate", False):
            if cardinality > 100 and null_percentage < 5:
                issues.append({
                    "type": "potential_primary_key",
                    "severity": "low",
                    "message": f"Column {column.get('name', 'unknown')} might be a good primary key candidate",
                    "table": table_name,
                    "column": column.get("name", "unknown")
                })
        
        return {
            "score": score,
            "issues": issues
        }
    
    def _generate_quality_recommendations(self, quality_report: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate quality improvement recommendations.
        
        Args:
            quality_report (Dict[str, Any]): Quality assessment report
            
        Returns:
            List[Dict[str, Any]]: List of recommendations
        """
        recommendations = []
        
        # Overall score recommendations
        overall_score = quality_report.get("overall_score", 0.0)
        if overall_score < 0.5:
            recommendations.append({
                "type": "overall_quality",
                "priority": "high",
                "title": "Improve Overall Data Quality",
                "description": "The overall data quality is low. Focus on addressing null values and data completeness.",
                "actions": [
                    "Review and fix null value patterns",
                    "Implement data validation rules",
                    "Consider data cleansing processes"
                ]
            })
        
        # High null percentage recommendations
        high_null_issues = [issue for issue in quality_report.get("issues", []) if issue.get("type") == "high_nulls"]
        if high_null_issues:
            recommendations.append({
                "type": "null_values",
                "priority": "high",
                "title": "Address High Null Percentages",
                "description": f"Found {len(high_null_issues)} columns with high null percentages.",
                "actions": [
                    "Investigate null value patterns",
                    "Consider default values or data imputation",
                    "Review data collection processes"
                ]
            })
        
        # Primary key recommendations
        pk_issues = [issue for issue in quality_report.get("issues", []) if issue.get("type") == "potential_primary_key"]
        if pk_issues:
            recommendations.append({
                "type": "primary_keys",
                "priority": "medium",
                "title": "Review Primary Key Candidates",
                "description": f"Found {len(pk_issues)} potential primary key candidates.",
                "actions": [
                    "Validate primary key candidates",
                    "Implement proper primary keys",
                    "Review table relationships"
                ]
            })
        
        # Low cardinality recommendations
        low_cardinality_issues = [issue for issue in quality_report.get("issues", []) if issue.get("type") == "low_cardinality"]
        if low_cardinality_issues:
            recommendations.append({
                "type": "cardinality",
                "priority": "medium",
                "title": "Review Low Cardinality Columns",
                "description": f"Found {len(low_cardinality_issues)} columns with very low cardinality.",
                "actions": [
                    "Review if low cardinality is expected",
                    "Consider data aggregation strategies",
                    "Validate data collection completeness"
                ]
            })
        
        return recommendations
