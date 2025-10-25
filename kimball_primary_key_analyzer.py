"""
Primary Key Analysis and Join Relationship Discovery

This module provides detailed analysis of primary key candidates and potential
join relationships between tables in the bronze schema.
"""

import json
from typing import Dict, List, Any, Tuple
from collections import defaultdict


class PrimaryKeyAnalyzer:
    """
    Analyzes primary key candidates and potential join relationships.
    """
    
    def __init__(self, catalog_file: str = "metadata_catalog.json"):
        """
        Initialize with metadata catalog.
        
        Args:
            catalog_file (str): Path to metadata catalog JSON file
        """
        with open(catalog_file, 'r') as f:
            self.catalog = json.load(f)
    
    def get_primary_key_candidates(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all primary key candidates across all tables.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Primary key candidates by table
        """
        pk_candidates = {}
        
        for table_name, table_data in self.catalog['tables'].items():
            if 'error' in table_data:
                continue
                
            candidates = []
            for column in table_data.get('columns', []):
                if column.get('is_primary_key_candidate', False):
                    candidates.append({
                        'column_name': column['name'],
                        'type': column['type'],
                        'cardinality': column['cardinality'],
                        'cardinality_ratio': column.get('cardinality_ratio', 0),
                        'null_count': column['null_count'],
                        'data_quality_score': column['data_quality_score']
                    })
            
            if candidates:
                pk_candidates[table_name] = candidates
        
        return pk_candidates
    
    def find_potential_joins(self) -> List[Dict[str, Any]]:
        """
        Find potential join relationships based on column names and types.
        
        Returns:
            List[Dict[str, Any]]: Potential join relationships
        """
        potential_joins = []
        
        # Get all primary key candidates
        pk_candidates = self.get_primary_key_candidates()
        
        # Look for common column names and types
        column_info = {}
        for table_name, table_data in self.catalog['tables'].items():
            if 'error' in table_data:
                continue
                
            for column in table_data.get('columns', []):
                col_name = column['name'].lower()
                if col_name not in column_info:
                    column_info[col_name] = []
                
                column_info[col_name].append({
                    'table': table_name,
                    'column': column['name'],
                    'type': column['type'],
                    'cardinality': column['cardinality'],
                    'is_pk_candidate': column.get('is_primary_key_candidate', False)
                })
        
        # Find columns that appear in multiple tables
        for col_name, occurrences in column_info.items():
            if len(occurrences) > 1:
                # Check if types are compatible
                compatible_joins = []
                for i, occ1 in enumerate(occurrences):
                    for j, occ2 in enumerate(occurrences[i+1:], i+1):
                        if self._are_types_compatible(occ1['type'], occ2['type']):
                            compatible_joins.append({
                                'table1': occ1['table'],
                                'column1': occ1['column'],
                                'table2': occ2['table'],
                                'column2': occ2['column'],
                                'type1': occ1['type'],
                                'type2': occ2['type'],
                                'cardinality1': occ1['cardinality'],
                                'cardinality2': occ2['cardinality'],
                                'is_pk1': occ1['is_pk_candidate'],
                                'is_pk2': occ2['is_pk_candidate'],
                                'join_confidence': self._calculate_join_confidence(occ1, occ2)
                            })
                
                potential_joins.extend(compatible_joins)
        
        # Sort by join confidence
        potential_joins.sort(key=lambda x: x['join_confidence'], reverse=True)
        return potential_joins
    
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
        if self._are_types_compatible(col1['type'], col2['type']):
            confidence += 0.2
        
        # Primary key candidate bonus
        if col1['is_pk_candidate'] and col2['is_pk_candidate']:
            confidence += 0.3
        elif col1['is_pk_candidate'] or col2['is_pk_candidate']:
            confidence += 0.15
        
        # Cardinality similarity bonus
        if col1['cardinality'] > 0 and col2['cardinality'] > 0:
            cardinality_ratio = min(col1['cardinality'], col2['cardinality']) / max(col1['cardinality'], col2['cardinality'])
            confidence += cardinality_ratio * 0.2
        
        return min(confidence, 1.0)
    
    def generate_join_analysis_report(self) -> None:
        """
        Generate a comprehensive report of primary key candidates and join relationships.
        """
        print("=" * 80)
        print("PRIMARY KEY CANDIDATES AND JOIN RELATIONSHIP ANALYSIS")
        print("=" * 80)
        
        # Primary Key Candidates
        print("\nPRIMARY KEY CANDIDATES:")
        print("-" * 80)
        
        pk_candidates = self.get_primary_key_candidates()
        
        if not pk_candidates:
            print("No primary key candidates found.")
            return
        
        for table_name, candidates in pk_candidates.items():
            print(f"\nTable: {table_name}")
            for candidate in candidates:
                print(f"  Column: {candidate['column_name']}")
                print(f"    Type: {candidate['type']}")
                print(f"    Cardinality: {candidate['cardinality']:,}")
                print(f"    Cardinality Ratio: {candidate['cardinality_ratio']:.2%}")
                print(f"    Null Count: {candidate['null_count']}")
                print(f"    Quality Score: {candidate['data_quality_score']:.2f}")
                print()
        
        # Join Relationships
        print("\nPOTENTIAL JOIN RELATIONSHIPS:")
        print("-" * 80)
        
        potential_joins = self.find_potential_joins()
        
        if not potential_joins:
            print("No potential join relationships found.")
            return
        
        print(f"Found {len(potential_joins)} potential join relationships:")
        print()
        
        for i, join in enumerate(potential_joins[:20], 1):  # Show top 20
            print(f"{i:2d}. {join['table1']}.{join['column1']} = {join['table2']}.{join['column2']}")
            print(f"     Types: {join['type1']} = {join['type2']}")
            print(f"     Cardinalities: {join['cardinality1']:,} = {join['cardinality2']:,}")
            print(f"     PK Candidates: {join['is_pk1']} = {join['is_pk2']}")
            print(f"     Join Confidence: {join['join_confidence']:.2f}")
            print()
        
        # Summary Statistics
        print("\nSUMMARY STATISTICS:")
        print("-" * 80)
        
        total_pk_candidates = sum(len(candidates) for candidates in pk_candidates.values())
        high_confidence_joins = len([j for j in potential_joins if j['join_confidence'] > 0.7])
        
        print(f"Total Primary Key Candidates: {total_pk_candidates}")
        print(f"Tables with PK Candidates: {len(pk_candidates)}")
        print(f"Total Potential Joins: {len(potential_joins)}")
        print(f"High Confidence Joins (>0.7): {high_confidence_joins}")
        
        # Recommendations
        print("\nRECOMMENDATIONS:")
        print("-" * 80)
        
        print("1. PRIMARY KEY RECOMMENDATIONS:")
        for table_name, candidates in pk_candidates.items():
            if candidates:
                best_candidate = max(candidates, key=lambda x: x['data_quality_score'])
                print(f"   {table_name}: {best_candidate['column_name']} (Quality: {best_candidate['data_quality_score']:.2f})")
        
        print("\n2. HIGH-CONFIDENCE JOIN RECOMMENDATIONS:")
        high_conf_joins = [j for j in potential_joins if j['join_confidence'] > 0.7]
        for join in high_conf_joins[:5]:
            print(f"   {join['table1']}.{join['column1']} = {join['table2']}.{join['column2']} (Confidence: {join['join_confidence']:.2f})")


def main():
    """Main function to run primary key analysis."""
    try:
        analyzer = PrimaryKeyAnalyzer()
        analyzer.generate_join_analysis_report()
    except FileNotFoundError:
        print("Error: metadata_catalog.json not found. Please run metadata_catalog.py first.")
    except Exception as e:
        print(f"Error running analysis: {str(e)}")


if __name__ == "__main__":
    main()
