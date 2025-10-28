#!/usr/bin/env python3
"""
KIMBALL Model Phase - Hierarchy Analysis

This module provides hierarchy analysis for the Model Phase.
It analyzes dimensional hierarchies in Stage 1 silver tables and generates
hierarchy metadata for OLAP modeling.

Based on the archive kimball_hierarchy_analyzer.py with enhancements for Stage 1 data.
"""

from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict
import logging
import numpy as np
from datetime import datetime

from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)


class HierarchyAnalyzer:
    """
    Analyzes dimensional hierarchies for Stage 1 silver tables.
    
    This class discovers:
    - ROOT nodes (lowest cardinality, highest level)
    - LEAF nodes (highest cardinality, lowest level)
    - Parent-child relationships
    - Sibling relationships
    - Level-based hierarchy structures
    - OLAP-compliant hierarchy metadata
    """
    
    def __init__(self):
        """Initialize hierarchy analyzer with database connection."""
        self.db_manager = DatabaseManager()
        self.stage1_tables = []
        self.dimension_columns = {}
        self.hierarchies = {}
        
    def discover_stage1_tables(self) -> List[str]:
        """
        Discover all Stage 1 tables in the silver schema.
        
        Returns:
            List[str]: List of Stage 1 table names
        """
        try:
            query = """
            SELECT name 
            FROM system.tables 
            WHERE database = 'silver' 
            AND name LIKE '%_stage1'
            ORDER BY name
            """
            
            results = self.db_manager.execute_query_dict(query)
            self.stage1_tables = [row['name'] for row in results] if results else []
            
            logger.info(f"Discovered {len(self.stage1_tables)} Stage 1 tables")
            return self.stage1_tables
            
        except Exception as e:
            logger.error(f"Error discovering Stage 1 tables: {e}")
            return []
    
    def load_dimension_columns(self) -> None:
        """
        Load dimension columns from Stage 1 tables for hierarchy analysis.
        """
        logger.info("Loading dimension columns from Stage 1 tables...")
        
        for table_name in self.stage1_tables:
            try:
                # Get dimension columns from metadata.discover
                query = f"""
                SELECT 
                    original_table_name,
                    original_column_name,
                    new_column_name,
                    inferred_type,
                    classification,
                    cardinality,
                    null_count,
                    sample_values
                FROM metadata.discover
                WHERE original_table_name = '{table_name.replace('_stage1', '')}'
                AND classification = 'dimension'
                ORDER BY cardinality ASC
                """
                
                results = self.db_manager.execute_query_dict(query)
                
                for row in results:
                    col_key = f"{table_name}.{row['new_column_name']}"
                    self.dimension_columns[col_key] = {
                        'table_name': table_name,
                        'original_table_name': row['original_table_name'],
                        'original_column_name': row['original_column_name'],
                        'new_column_name': row['new_column_name'],
                        'inferred_type': row['inferred_type'],
                        'classification': row['classification'],
                        'cardinality': row['cardinality'],
                        'null_count': row['null_count'],
                        'sample_values': row['sample_values'] if row['sample_values'] else [],
                        'data_type': row['inferred_type']
                    }
                
            except Exception as e:
                logger.error(f"Error loading dimension columns for {table_name}: {e}")
        
        logger.info(f"Loaded {len(self.dimension_columns)} dimension columns")
    
    def build_table_hierarchies(self) -> Dict[str, Any]:
        """
        Build hierarchies based on table structure and cardinality (ROOT to LEAF).
        
        Returns:
            Dict[str, Any]: Dictionary of hierarchies by table name
        """
        logger.info("Building table-based hierarchies (ROOT to LEAF)...")
        
        # Group columns by table
        table_columns = defaultdict(list)
        for col_key, col_info in self.dimension_columns.items():
            table_name = col_info['table_name']
            table_columns[table_name].append((col_key, col_info))
        
        hierarchies = {}
        
        for table_name, columns in table_columns.items():
            if len(columns) < 2:
                continue  # Need at least 2 columns for a hierarchy
            
            # Sort columns by cardinality (ASCENDING - lowest cardinality = ROOT)
            sorted_columns = sorted(columns, key=lambda x: x[1]['cardinality'], reverse=False)
            
            # Create hierarchy for this table
            # ROOT = lowest cardinality (Level 0)
            # LEAF = highest cardinality (Level N)
            root_col = sorted_columns[0]  # Lowest cardinality = ROOT
            leaf_col = sorted_columns[-1]  # Highest cardinality = LEAF
            intermediate_cols = sorted_columns[1:-1]  # Middle levels
            
            hierarchy = {
                'table_name': table_name,
                'original_table_name': root_col[1]['original_table_name'],
                'root': {
                    'key': root_col[0],
                    'column': root_col[1]['new_column_name'],
                    'original_column': root_col[1]['original_column_name'],
                    'cardinality': root_col[1]['cardinality'],
                    'level': 0,
                    'parent': None,
                    'children': [],
                    'siblings': [],
                    'role': 'ROOT',
                    'data_type': root_col[1]['data_type']
                },
                'leaf': {
                    'key': leaf_col[0],
                    'column': leaf_col[1]['new_column_name'],
                    'original_column': leaf_col[1]['original_column_name'],
                    'cardinality': leaf_col[1]['cardinality'],
                    'level': len(intermediate_cols) + 1,
                    'parent': intermediate_cols[-1][0] if intermediate_cols else root_col[0],
                    'children': [],
                    'siblings': [],
                    'role': 'LEAF',
                    'data_type': leaf_col[1]['data_type']
                },
                'intermediate_levels': [],
                'total_levels': len(sorted_columns)
            }
            
            # Add intermediate levels
            for i, (col_key, col_info) in enumerate(intermediate_cols):
                level_node = {
                    'key': col_key,
                    'column': col_info['new_column_name'],
                    'original_column': col_info['original_column_name'],
                    'cardinality': col_info['cardinality'],
                    'level': i + 1,
                    'parent': root_col[0] if i == 0 else intermediate_cols[i-1][0],
                    'children': [],
                    'siblings': [],
                    'role': 'INTERMEDIATE',
                    'data_type': col_info['data_type']
                }
                hierarchy['intermediate_levels'].append(level_node)
            
            # Build parent-child relationships
            self._build_parent_child_relationships(hierarchy, sorted_columns)
            
            # Find siblings at each level
            self._find_siblings_by_level(hierarchy, sorted_columns)
            
            hierarchies[table_name] = hierarchy
        
        self.hierarchies = hierarchies
        return hierarchies
    
    def _build_parent_child_relationships(self, hierarchy: Dict[str, Any], sorted_columns: List) -> None:
        """
        Build parent-child relationships from ROOT to LEAF.
        
        Args:
            hierarchy (Dict[str, Any]): Hierarchy structure to update
            sorted_columns (List): Columns sorted by cardinality
        """
        # ROOT has no parent, but has children
        if hierarchy['intermediate_levels']:
            hierarchy['root']['children'].append(hierarchy['intermediate_levels'][0])
        else:
            # Direct ROOT to LEAF relationship
            hierarchy['root']['children'].append(hierarchy['leaf'])
            hierarchy['leaf']['parent'] = hierarchy['root']['key']
        
        # Intermediate levels
        for i, level in enumerate(hierarchy['intermediate_levels']):
            # Parent is previous level
            if i == 0:
                level['parent'] = hierarchy['root']['key']
            else:
                level['parent'] = hierarchy['intermediate_levels'][i-1]['key']
            
            # Children is next level
            if i == len(hierarchy['intermediate_levels']) - 1:
                level['children'].append(hierarchy['leaf'])
                hierarchy['leaf']['parent'] = level['key']
            else:
                level['children'].append(hierarchy['intermediate_levels'][i+1])
    
    def _find_siblings_by_level(self, hierarchy: Dict[str, Any], sorted_columns: List) -> None:
        """
        Find sibling relationships at each level.
        
        Args:
            hierarchy (Dict[str, Any]): Hierarchy structure to update
            sorted_columns (List): Columns sorted by cardinality
        """
        # Group columns by similar cardinality ranges
        cardinality_groups = defaultdict(list)
        
        for col_key, col_info in sorted_columns:
            # Group by cardinality ranges (logarithmic grouping)
            if col_info['cardinality'] == 0:
                group = 0
            else:
                group = int(np.log10(col_info['cardinality']))
            cardinality_groups[group].append((col_key, col_info))
        
        # Find siblings within each cardinality group
        for group, columns in cardinality_groups.items():
            if len(columns) > 1:
                for i, (col1_key, col1_info) in enumerate(columns):
                    siblings = []
                    for j, (col2_key, col2_info) in enumerate(columns):
                        if i != j:
                            # Check name similarity
                            name_similarity = self._calculate_name_similarity(
                                col1_info['new_column_name'], 
                                col2_info['new_column_name']
                            )
                            if name_similarity > 0.3:
                                siblings.append({
                                    'key': col2_key,
                                    'column': col2_info['new_column_name'],
                                    'original_column': col2_info['original_column_name'],
                                    'cardinality': col2_info['cardinality'],
                                    'name_similarity': name_similarity,
                                    'data_type': col2_info['data_type']
                                })
                    
                    # Add siblings to appropriate node
                    if col1_key == hierarchy['root']['key']:
                        hierarchy['root']['siblings'] = siblings
                    elif col1_key == hierarchy['leaf']['key']:
                        hierarchy['leaf']['siblings'] = siblings
                    else:
                        for level in hierarchy['intermediate_levels']:
                            if level['key'] == col1_key:
                                level['siblings'] = siblings
                                break
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate similarity between two column names.
        
        Args:
            name1 (str): First column name
            name2 (str): Second column name
            
        Returns:
            float: Similarity score between 0 and 1
        """
        words1 = set(name1.lower().replace('_', ' ').replace('-', ' ').split())
        words2 = set(name2.lower().replace('_', ' ').replace('-', ' ').split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0
    
    def find_cross_hierarchy_relationships(self) -> List[Dict[str, Any]]:
        """
        Find relationships between hierarchies across different tables.
        
        Returns:
            List[Dict[str, Any]]: Cross-hierarchy relationships
        """
        cross_relationships = []
        
        # Look for columns with similar names across tables
        column_groups = defaultdict(list)
        
        for hierarchy in self.hierarchies.values():
            # Add all columns from this hierarchy
            all_columns = [hierarchy['root']]
            all_columns.extend(hierarchy['intermediate_levels'])
            all_columns.append(hierarchy['leaf'])
            
            for col in all_columns:
                col_name = col['column'].lower()
                column_groups[col_name].append({
                    'table': hierarchy['table_name'],
                    'original_table': hierarchy['original_table_name'],
                    'column': col['column'],
                    'original_column': col['original_column'],
                    'cardinality': col['cardinality'],
                    'level': col['level'],
                    'role': col['role'],
                    'data_type': col['data_type']
                })
        
        # Find cross-table relationships
        for col_name, occurrences in column_groups.items():
            if len(occurrences) > 1:
                for i, occ1 in enumerate(occurrences):
                    for j, occ2 in enumerate(occurrences[i+1:], i+1):
                        if occ1['table'] != occ2['table']:  # Different tables
                            relationship = {
                                'table1': occ1['table'],
                                'original_table1': occ1['original_table'],
                                'column1': occ1['column'],
                                'original_column1': occ1['original_column'],
                                'table2': occ2['table'],
                                'original_table2': occ2['original_table'],
                                'column2': occ2['column'],
                                'original_column2': occ2['original_column'],
                                'cardinality1': occ1['cardinality'],
                                'cardinality2': occ2['cardinality'],
                                'level1': occ1['level'],
                                'level2': occ2['level'],
                                'role1': occ1['role'],
                                'role2': occ2['role'],
                                'data_type1': occ1['data_type'],
                                'data_type2': occ2['data_type'],
                                'name_similarity': self._calculate_name_similarity(
                                    occ1['column'], occ2['column']
                                ),
                                'cardinality_similarity': min(occ1['cardinality'], occ2['cardinality']) / 
                                                        max(occ1['cardinality'], occ2['cardinality']) 
                                                        if max(occ1['cardinality'], occ2['cardinality']) > 0 else 0,
                                'relationship_confidence': self._calculate_cross_hierarchy_confidence(occ1, occ2)
                            }
                            cross_relationships.append(relationship)
        
        # Sort by confidence
        cross_relationships.sort(key=lambda x: x['relationship_confidence'], reverse=True)
        
        return cross_relationships
    
    def _calculate_cross_hierarchy_confidence(self, occ1: Dict[str, Any], occ2: Dict[str, Any]) -> float:
        """
        Calculate confidence score for cross-hierarchy relationships.
        
        Args:
            occ1 (Dict[str, Any]): First occurrence
            occ2 (Dict[str, Any]): Second occurrence
            
        Returns:
            float: Confidence score between 0 and 1
        """
        confidence = 0.0
        
        # Base confidence for same column name
        confidence += 0.4
        
        # Data type compatibility
        if occ1['data_type'] == occ2['data_type']:
            confidence += 0.2
        
        # Cardinality similarity
        if occ1['cardinality'] > 0 and occ2['cardinality'] > 0:
            cardinality_ratio = min(occ1['cardinality'], occ2['cardinality']) / max(occ1['cardinality'], occ2['cardinality'])
            confidence += cardinality_ratio * 0.2
        
        # Level similarity (prefer same levels)
        level_diff = abs(occ1['level'] - occ2['level'])
        if level_diff == 0:
            confidence += 0.1
        elif level_diff == 1:
            confidence += 0.05
        
        # Role similarity (prefer same roles)
        if occ1['role'] == occ2['role']:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def generate_hierarchy_metadata(self) -> Dict[str, Any]:
        """
        Generate comprehensive hierarchy metadata for all Stage 1 tables.
        
        Returns:
            Dict[str, Any]: Complete hierarchy metadata
        """
        # Discover and load dimension columns
        self.discover_stage1_tables()
        self.load_dimension_columns()
        
        # Build hierarchies
        hierarchies = self.build_table_hierarchies()
        
        # Find cross-hierarchy relationships
        cross_relationships = self.find_cross_hierarchy_relationships()
        
        # Generate hierarchy metadata
        hierarchy_metadata = {
            'schema_name': 'silver',
            'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_tables': len(self.stage1_tables),
            'total_dimension_columns': len(self.dimension_columns),
            'total_hierarchies': len(hierarchies),
            'total_cross_relationships': len(cross_relationships),
            'hierarchies': hierarchies,
            'cross_hierarchy_relationships': cross_relationships,
            'summary': {
                'tables_with_hierarchies': len(hierarchies),
                'max_hierarchy_depth': max(h['total_levels'] for h in hierarchies.values()) if hierarchies else 0,
                'high_confidence_cross_relationships': len([r for r in cross_relationships if r['relationship_confidence'] > 0.7]),
                'root_nodes': len([h for h in hierarchies.values() if h['root']]),
                'leaf_nodes': len([h for h in hierarchies.values() if h['leaf']])
            }
        }
        
        return hierarchy_metadata
    
    def store_hierarchy_metadata(self, hierarchy_metadata: Dict[str, Any]) -> bool:
        """
        Store hierarchy metadata in the metadata.hierarchies table.
        
        Args:
            hierarchy_metadata (Dict[str, Any]): Hierarchy metadata to store
            
        Returns:
            bool: True if successful
        """
        try:
            # Create metadata.hierarchies table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS metadata.hierarchies (
                id UInt64,
                schema_name String,
                analysis_timestamp DateTime,
                table_name String,
                original_table_name String,
                hierarchy_name String,
                total_levels UInt32,
                root_column String,
                root_cardinality UInt64,
                leaf_column String,
                leaf_cardinality UInt64,
                intermediate_levels Array(String),
                parent_child_relationships Array(String),
                sibling_relationships Array(String),
                cross_hierarchy_relationships Array(String),
                metadata_json String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (schema_name, table_name, analysis_timestamp)
            """
            
            self.db_manager.execute_query(create_table_sql)
            
            # Insert hierarchy metadata
            for table_name, hierarchy in hierarchy_metadata['hierarchies'].items():
                intermediate_levels = [level['column'] for level in hierarchy['intermediate_levels']]
                
                # Parent-child relationships
                parent_child_rels = []
                if hierarchy['root']['children']:
                    for child in hierarchy['root']['children']:
                        parent_child_rels.append(f"{hierarchy['root']['column']} -> {child['column']}")
                
                for level in hierarchy['intermediate_levels']:
                    if level['children']:
                        for child in level['children']:
                            parent_child_rels.append(f"{level['column']} -> {child['column']}")
                
                # Sibling relationships
                sibling_rels = []
                if hierarchy['root']['siblings']:
                    for sibling in hierarchy['root']['siblings']:
                        sibling_rels.append(f"{hierarchy['root']['column']} <-> {sibling['column']}")
                
                for level in hierarchy['intermediate_levels']:
                    if level['siblings']:
                        for sibling in level['siblings']:
                            sibling_rels.append(f"{level['column']} <-> {sibling['column']}")
                
                if hierarchy['leaf']['siblings']:
                    for sibling in hierarchy['leaf']['siblings']:
                        sibling_rels.append(f"{hierarchy['leaf']['column']} <-> {sibling['column']}")
                
                # Cross-hierarchy relationships for this table
                cross_rels = [
                    f"{rel['table1']}.{rel['column1']} <-> {rel['table2']}.{rel['column2']}"
                    for rel in hierarchy_metadata['cross_hierarchy_relationships']
                    if rel['table1'] == table_name or rel['table2'] == table_name
                ]
                
                insert_sql = f"""
                INSERT INTO metadata.hierarchies (
                    id, schema_name, analysis_timestamp, table_name, original_table_name,
                    hierarchy_name, total_levels, root_column, root_cardinality,
                    leaf_column, leaf_cardinality, intermediate_levels,
                    parent_child_relationships, sibling_relationships,
                    cross_hierarchy_relationships, metadata_json
                ) VALUES (
                    {hash(table_name) % 2**63}, '{hierarchy_metadata['schema_name']}',
                    '{hierarchy_metadata['analysis_timestamp']}', '{table_name}',
                    '{hierarchy['original_table_name']}', '{table_name}_hierarchy',
                    {hierarchy['total_levels']}, '{hierarchy['root']['column']}',
                    {hierarchy['root']['cardinality']}, '{hierarchy['leaf']['column']}',
                    {hierarchy['leaf']['cardinality']}, {repr(intermediate_levels)},
                    {repr(parent_child_rels)}, {repr(sibling_rels)}, {repr(cross_rels)},
                    '{str(hierarchy).replace("'", "''")}'
                )
                """
                
                self.db_manager.execute_query(insert_sql)
            
            logger.info(f"Stored hierarchy metadata for {len(hierarchy_metadata['hierarchies'])} tables")
            return True
            
        except Exception as e:
            logger.error(f"Error storing hierarchy metadata: {e}")
            return False
