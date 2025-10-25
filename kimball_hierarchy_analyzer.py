"""
Corrected Hierarchy Level Analysis

This module creates proper level-based hierarchies with ROOT (lowest cardinality)
at the top and LEAF (highest cardinality) at the bottom, following OLAP standards.
"""

import json
import numpy as np
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CorrectedHierarchyLevelAnalyzer:
    """
    Creates proper OLAP-style hierarchies with ROOT at top and LEAF at bottom.
    """
    
    def __init__(self, catalog_file: str = "metadata_catalog.json"):
        """Initialize with metadata catalog."""
        with open(catalog_file, 'r') as f:
            self.catalog = json.load(f)
        
        self.column_info = {}
        self.hierarchies = {}
        
    def load_dimension_columns(self) -> None:
        """Load dimension columns for analysis."""
        logger.info("Loading dimension columns...")
        
        for table_name, table_data in self.catalog['tables'].items():
            if 'error' in table_data:
                continue
                
            for column in table_data.get('columns', []):
                if column.get('classification') == 'dimension':
                    col_key = f"{table_name}.{column['name']}"
                    self.column_info[col_key] = {
                        'table_name': table_name,
                        'column_name': column['name'],
                        'cardinality': column['cardinality'],
                        'null_count': column['null_count'],
                        'sample_values': column.get('sample_values', []),
                        'data_type': column['type']
                    }
        
        logger.info(f"Loaded {len(self.column_info)} dimension columns")
    
    def build_table_hierarchies(self) -> Dict:
        """Build hierarchies based on table structure and cardinality (ROOT to LEAF)."""
        logger.info("Building table-based hierarchies (ROOT to LEAF)...")
        
        # Group columns by table
        table_columns = defaultdict(list)
        for col_key, col_info in self.column_info.items():
            table_name = col_info['table_name']
            table_columns[table_name].append((col_key, col_info))
        
        hierarchies = {}
        
        for table_name, columns in table_columns.items():
            # Sort columns by cardinality (ASCENDING - lowest cardinality = ROOT)
            sorted_columns = sorted(columns, key=lambda x: x[1]['cardinality'], reverse=False)
            
            if len(sorted_columns) > 1:
                # Create hierarchy for this table
                # ROOT = lowest cardinality (Level 0)
                # LEAF = highest cardinality (Level N)
                root_col = sorted_columns[0]  # Lowest cardinality = ROOT
                leaf_col = sorted_columns[-1]  # Highest cardinality = LEAF
                intermediate_cols = sorted_columns[1:-1]  # Middle levels
                
                hierarchy = {
                    'table_name': table_name,
                    'root': {
                        'key': root_col[0],
                        'column': root_col[1]['column_name'],
                        'cardinality': root_col[1]['cardinality'],
                        'level': 0,
                        'parent': None,
                        'children': [],
                        'siblings': [],
                        'role': 'ROOT'
                    },
                    'leaf': {
                        'key': leaf_col[0],
                        'column': leaf_col[1]['column_name'],
                        'cardinality': leaf_col[1]['cardinality'],
                        'level': len(intermediate_cols) + 1,
                        'parent': intermediate_cols[-1][0] if intermediate_cols else root_col[0],
                        'children': [],
                        'siblings': [],
                        'role': 'LEAF'
                    },
                    'intermediate_levels': [],
                    'total_levels': len(sorted_columns)
                }
                
                # Add intermediate levels
                for i, (col_key, col_info) in enumerate(intermediate_cols):
                    level_node = {
                        'key': col_key,
                        'column': col_info['column_name'],
                        'cardinality': col_info['cardinality'],
                        'level': i + 1,
                        'parent': root_col[0] if i == 0 else intermediate_cols[i-1][0],
                        'children': [],
                        'siblings': [],
                        'role': 'INTERMEDIATE'
                    }
                    hierarchy['intermediate_levels'].append(level_node)
                
                # Build parent-child relationships
                self._build_parent_child_relationships(hierarchy, sorted_columns)
                
                # Find siblings at each level
                self._find_siblings_by_level(hierarchy, sorted_columns)
                
                hierarchies[table_name] = hierarchy
        
        self.hierarchies = hierarchies
        return hierarchies
    
    def _build_parent_child_relationships(self, hierarchy: Dict, sorted_columns: List) -> None:
        """Build parent-child relationships from ROOT to LEAF."""
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
    
    def _find_siblings_by_level(self, hierarchy: Dict, sorted_columns: List) -> None:
        """Find sibling relationships at each level."""
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
                            name_similarity = self._calculate_name_similarity(col1_info['column_name'], col2_info['column_name'])
                            if name_similarity > 0.3:
                                siblings.append({
                                    'key': col2_key,
                                    'column': col2_info['column_name'],
                                    'cardinality': col2_info['cardinality'],
                                    'name_similarity': name_similarity
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
        """Calculate similarity between two column names."""
        words1 = set(name1.lower().replace('_', ' ').replace('-', ' ').split())
        words2 = set(name2.lower().replace('_', ' ').replace('-', ' ').split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0
    
    def generate_level_report(self) -> None:
        """Generate a comprehensive level-based hierarchy report."""
        print("=" * 80)
        print("CORRECTED HIERARCHY LEVEL ANALYSIS REPORT")
        print("=" * 80)
        
        # Load and analyze
        self.load_dimension_columns()
        hierarchies = self.build_table_hierarchies()
        
        print(f"\nANALYSIS SUMMARY:")
        print("-" * 80)
        print(f"Total Dimension Columns: {len(self.column_info)}")
        print(f"Tables with Hierarchies: {len(hierarchies)}")
        
        # Show level distribution
        level_distribution = defaultdict(int)
        for hierarchy in hierarchies.values():
            level_distribution[0] += 1  # ROOT
            level_distribution[len(hierarchy['intermediate_levels'])] += 1  # LEAF
            for level in hierarchy['intermediate_levels']:
                level_distribution[level['level']] += 1
        
        print(f"\nLEVEL DISTRIBUTION:")
        print("-" * 80)
        for level in sorted(level_distribution.keys()):
            print(f"Level {level}: {level_distribution[level]} columns")
        
        # Show hierarchies by level (ROOT to LEAF)
        print(f"\nHIERARCHY STRUCTURES (ROOT to LEAF):")
        print("-" * 80)
        
        for table_name, hierarchy in hierarchies.items():
            print(f"\nTABLE: {table_name}")
            print(f"Total Levels: {hierarchy['total_levels']}")
            print(f"ROOT: {hierarchy['root']['column']} (Cardinality: {hierarchy['root']['cardinality']:,}, Level: {hierarchy['root']['level']})")
            print(f"LEAF: {hierarchy['leaf']['column']} (Cardinality: {hierarchy['leaf']['cardinality']:,}, Level: {hierarchy['leaf']['level']})")
            
            self._print_hierarchy_by_level(hierarchy, indent="")
    
    def _print_hierarchy_by_level(self, hierarchy: Dict, indent: str = "") -> None:
        """Print hierarchy structure from ROOT to LEAF."""
        # Print ROOT
        root = hierarchy['root']
        print(f"{indent}├─ {root['column']} (ROOT, Level {root['level']}, Cardinality: {root['cardinality']:,})")
        
        # Print intermediate levels
        for level in hierarchy['intermediate_levels']:
            print(f"{indent}   ├─ {level['column']} (Level {level['level']}, Cardinality: {level['cardinality']:,})")
        
        # Print LEAF
        leaf = hierarchy['leaf']
        print(f"{indent}   └─ {leaf['column']} (LEAF, Level {leaf['level']}, Cardinality: {leaf['cardinality']:,})")
        
        # Print siblings if any
        if root['siblings']:
            print(f"{indent}   ROOT Siblings:")
            for sibling in root['siblings']:
                print(f"{indent}   ├─ {sibling['column']} (Cardinality: {sibling['cardinality']:,})")
        
        if leaf['siblings']:
            print(f"{indent}   LEAF Siblings:")
            for sibling in leaf['siblings']:
                print(f"{indent}   ├─ {sibling['column']} (Cardinality: {sibling['cardinality']:,})")
    
    def save_level_hierarchies(self, filename: str = "hierarchy_corrected_levels.json") -> None:
        """Save corrected level-based hierarchies to JSON file."""
        results = {
            'metadata': {
                'total_columns': len(self.column_info),
                'total_tables': len(self.hierarchies),
                'analysis_type': 'corrected_olap_style',
                'hierarchy_direction': 'ROOT_to_LEAF'
            },
            'hierarchies': self.hierarchies,
            'level_distribution': {
                'total_levels': max(h['total_levels'] for h in self.hierarchies.values()) if self.hierarchies else 0,
                'tables_with_hierarchies': len(self.hierarchies)
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Corrected level-based hierarchies saved to {filename}")


def main():
    """Main function to run corrected hierarchy level analysis."""
    analyzer = CorrectedHierarchyLevelAnalyzer()
    
    try:
        # Generate level-based report
        analyzer.generate_level_report()
        
        # Save results
        analyzer.save_level_hierarchies()
        
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")


if __name__ == "__main__":
    main()
