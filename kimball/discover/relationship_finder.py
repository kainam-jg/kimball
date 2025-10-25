"""
KIMBALL Relationship Finder

This module provides relationship discovery functionality
for the Discover phase.
"""

from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import json

from ..core.logger import Logger

class RelationshipFinder:
    """
    Relationship finder for discovering table relationships and join candidates.
    """
    
    def __init__(self):
        """Initialize the relationship finder."""
        self.logger = Logger("relationship_finder")
    
    def find_relationships(self, catalog: Dict[str, Any]) -> Dict[str, Any]:
        """
        Find all relationships in a catalog.
        
        Args:
            catalog (Dict[str, Any]): The metadata catalog
            
        Returns:
            Dict[str, Any]: Relationship discovery report
        """
        try:
            self.logger.info("Finding relationships in catalog")
            
            relationship_report = {
                "total_relationships": 0,
                "high_confidence_joins": 0,
                "primary_key_candidates": 0,
                "relationships": [],
                "join_candidates": [],
                "foreign_key_candidates": [],
                "assessment_timestamp": None
            }
            
            # Get primary key candidates
            pk_candidates = self._get_primary_key_candidates(catalog)
            relationship_report["primary_key_candidates"] = len(pk_candidates)
            
            # Find potential joins
            join_candidates = self._find_join_candidates(catalog)
            relationship_report["join_candidates"] = join_candidates
            relationship_report["total_relationships"] = len(join_candidates)
            
            # Find high confidence joins
            high_conf_joins = [join for join in join_candidates if join.get("confidence", 0) > 0.7]
            relationship_report["high_confidence_joins"] = len(high_conf_joins)
            
            # Find foreign key candidates
            fk_candidates = self._find_foreign_key_candidates(catalog, join_candidates)
            relationship_report["foreign_key_candidates"] = fk_candidates
            
            # Combine all relationships
            all_relationships = join_candidates + fk_candidates
            relationship_report["relationships"] = all_relationships
            
            self.logger.info(f"Found {len(all_relationships)} total relationships")
            return relationship_report
            
        except Exception as e:
            self.logger.error(f"Error finding relationships: {str(e)}")
            return {"error": str(e)}
    
    def _get_primary_key_candidates(self, catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get all primary key candidates from catalog.
        
        Args:
            catalog (Dict[str, Any]): The metadata catalog
            
        Returns:
            List[Dict[str, Any]]: Primary key candidates
        """
        pk_candidates = []
        
        for table_name, table_data in catalog.get("tables", {}).items():
            if "error" in table_data:
                continue
            
            for column in table_data.get("columns", []):
                if column.get("is_primary_key_candidate", False):
                    pk_candidates.append({
                        "table": table_name,
                        "column": column.get("name", "unknown"),
                        "type": column.get("type", "unknown"),
                        "cardinality": column.get("cardinality", 0),
                        "quality_score": column.get("data_quality_score", 0)
                    })
        
        return pk_candidates
    
    def _find_join_candidates(self, catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Find potential join relationships based on column names and types.
        
        Args:
            catalog (Dict[str, Any]): The metadata catalog
            
        Returns:
            List[Dict[str, Any]]: Join candidates
        """
        join_candidates = []
        
        # Build column index
        column_index = defaultdict(list)
        
        for table_name, table_data in catalog.get("tables", {}).items():
            if "error" in table_data:
                continue
            
            for column in table_data.get("columns", []):
                col_name = column.get("name", "").lower()
                if col_name:
                    column_index[col_name].append({
                        "table": table_name,
                        "column": column.get("name", "unknown"),
                        "type": column.get("type", "unknown"),
                        "cardinality": column.get("cardinality", 0),
                        "is_pk_candidate": column.get("is_primary_key_candidate", False),
                        "classification": column.get("classification", "unknown")
                    })
        
        # Find columns that appear in multiple tables
        for col_name, occurrences in column_index.items():
            if len(occurrences) > 1:
                # Check for compatible joins
                for i, occ1 in enumerate(occurrences):
                    for j, occ2 in enumerate(occurrences[i+1:], i+1):
                        if self._are_types_compatible(occ1["type"], occ2["type"]):
                            confidence = self._calculate_join_confidence(occ1, occ2)
                            
                            join_candidates.append({
                                "table1": occ1["table"],
                                "column1": occ1["column"],
                                "table2": occ2["table"],
                                "column2": occ2["column"],
                                "type1": occ1["type"],
                                "type2": occ2["type"],
                                "cardinality1": occ1["cardinality"],
                                "cardinality2": occ2["cardinality"],
                                "is_pk1": occ1["is_pk_candidate"],
                                "is_pk2": occ2["is_pk_candidate"],
                                "confidence": confidence,
                                "relationship_type": self._determine_relationship_type(occ1, occ2)
                            })
        
        # Sort by confidence
        join_candidates.sort(key=lambda x: x["confidence"], reverse=True)
        return join_candidates
    
    def _find_foreign_key_candidates(self, catalog: Dict[str, Any], join_candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Find foreign key candidates based on join relationships.
        
        Args:
            catalog (Dict[str, Any]): The metadata catalog
            join_candidates (List[Dict[str, Any]]): Join candidates
            
        Returns:
            List[Dict[str, Any]]: Foreign key candidates
        """
        fk_candidates = []
        
        for join in join_candidates:
            if join.get("confidence", 0) > 0.7:
                # Determine which table has the foreign key
                if join.get("is_pk1", False) and not join.get("is_pk2", False):
                    fk_candidates.append({
                        "foreign_key_table": join["table2"],
                        "foreign_key_column": join["column2"],
                        "referenced_table": join["table1"],
                        "referenced_column": join["column1"],
                        "confidence": join["confidence"],
                        "relationship_type": "one_to_many"
                    })
                elif join.get("is_pk2", False) and not join.get("is_pk1", False):
                    fk_candidates.append({
                        "foreign_key_table": join["table1"],
                        "foreign_key_column": join["column1"],
                        "referenced_table": join["table2"],
                        "referenced_column": join["column2"],
                        "confidence": join["confidence"],
                        "relationship_type": "one_to_many"
                    })
                elif join.get("is_pk1", False) and join.get("is_pk2", False):
                    fk_candidates.append({
                        "foreign_key_table": join["table1"],
                        "foreign_key_column": join["column1"],
                        "referenced_table": join["table2"],
                        "referenced_column": join["column2"],
                        "confidence": join["confidence"],
                        "relationship_type": "many_to_many"
                    })
        
        return fk_candidates
    
    def _are_types_compatible(self, type1: str, type2: str) -> bool:
        """
        Check if two column types are compatible for joining.
        
        Args:
            type1 (str): First column type
            type2 (str): Second column type
            
        Returns:
            bool: True if types are compatible
        """
        # Normalize types for comparison
        type1_norm = type1.lower().replace('nullable(', '').replace(')', '')
        type2_norm = type2.lower().replace('nullable(', '').replace(')', '')
        
        # Exact match
        if type1_norm == type2_norm:
            return True
        
        # Numeric type compatibility
        numeric_types = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float32', 'float64']
        if type1_norm in numeric_types and type2_norm in numeric_types:
            return True
        
        # String type compatibility
        if 'string' in type1_norm and 'string' in type2_norm:
            return True
        
        return False
    
    def _calculate_join_confidence(self, col1: Dict[str, Any], col2: Dict[str, Any]) -> float:
        """
        Calculate confidence score for a potential join.
        
        Args:
            col1 (Dict[str, Any]): First column info
            col2 (Dict[str, Any]): Second column info
            
        Returns:
            float: Join confidence score (0-1)
        """
        confidence = 0.0
        
        # Base confidence for same column name
        confidence += 0.3
        
        # Type compatibility bonus
        if self._are_types_compatible(col1["type"], col2["type"]):
            confidence += 0.2
        
        # Primary key candidate bonus
        if col1["is_pk_candidate"] and col2["is_pk_candidate"]:
            confidence += 0.3
        elif col1["is_pk_candidate"] or col2["is_pk_candidate"]:
            confidence += 0.15
        
        # Cardinality similarity bonus
        if col1["cardinality"] > 0 and col2["cardinality"] > 0:
            cardinality_ratio = min(col1["cardinality"], col2["cardinality"]) / max(col1["cardinality"], col2["cardinality"])
            confidence += cardinality_ratio * 0.2
        
        return min(confidence, 1.0)
    
    def _determine_relationship_type(self, col1: Dict[str, Any], col2: Dict[str, Any]) -> str:
        """
        Determine the type of relationship between two columns.
        
        Args:
            col1 (Dict[str, Any]): First column info
            col2 (Dict[str, Any]): Second column info
            
        Returns:
            str: Relationship type
        """
        if col1["is_pk_candidate"] and col2["is_pk_candidate"]:
            return "many_to_many"
        elif col1["is_pk_candidate"] or col2["is_pk_candidate"]:
            return "one_to_many"
        else:
            return "many_to_many"
