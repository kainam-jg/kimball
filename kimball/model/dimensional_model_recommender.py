#!/usr/bin/env python3
"""
KIMBALL Model Phase - Dimensional Model Recommendation Engine

This module provides dimensional modeling recommendations based on:
- ERD metadata (relationships, table types, fact/dimension columns)
- Hierarchy metadata (parent-child and sibling relationships)
- Discover metadata (column classifications)

Recommends fact tables (_fact suffix) and dimension tables (_dim suffix) for the gold schema.
"""

from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
import logging
from datetime import datetime

from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)


class DimensionalModelRecommender:
    """
    Recommends dimensional models for the gold schema based on metadata analysis.
    
    This class:
    1. Analyzes ERD relationships to identify fact tables
    2. Analyzes hierarchies to identify dimension tables
    3. Uses discover metadata for column type validation
    4. Generates recommendations for _fact and _dim tables
    5. Stores recommendations in metadata.dimensional_model
    """
    
    def __init__(self):
        """Initialize the recommender with database connection."""
        self.db_manager = DatabaseManager()
        self.recommendations = {
            'dimension_tables': [],
            'fact_tables': [],
            'relationships': []
        }
        
    def analyze_metadata(self) -> Dict[str, Any]:
        """
        Analyze all relevant metadata to understand the data model.
        
        Returns:
            Dict containing ERD, hierarchy, and discover metadata
        """
        logger.info("Analyzing metadata for dimensional model recommendations...")
        
        metadata = {
            'erd': {},
            'hierarchies': {},
            'discover': {},
            'stage1_tables': []
        }
        
        # Get ERD metadata
        try:
            erd_query = """
            SELECT 
                table_name,
                table_type,
                fact_columns,
                dimension_columns,
                relationships,
                primary_key_candidates
            FROM metadata.erd
            ORDER BY analysis_timestamp DESC
            """
            
            erd_results = self.db_manager.execute_query_dict(erd_query)
            
            # Get most recent entry per table
            for row in erd_results:
                table_name = row['table_name']
                if table_name not in metadata['erd']:
                    metadata['erd'][table_name] = row
            
            logger.info(f"Loaded ERD metadata for {len(metadata['erd'])} tables")
            
        except Exception as e:
            logger.warning(f"Error loading ERD metadata: {e}")
        
        # Get hierarchy metadata
        try:
            hierarchy_query = """
            SELECT 
                table_name,
                hierarchy_name,
                root_column,
                leaf_column,
                intermediate_levels,
                parent_child_relationships,
                sibling_relationships
            FROM metadata.hierarchies
            ORDER BY analysis_timestamp DESC
            """
            
            hierarchy_results = self.db_manager.execute_query_dict(hierarchy_query)
            
            # Get most recent entry per table
            for row in hierarchy_results:
                table_name = row['table_name']
                if table_name not in metadata['hierarchies']:
                    metadata['hierarchies'][table_name] = row
            
            logger.info(f"Loaded hierarchy metadata for {len(metadata['hierarchies'])} tables")
            
        except Exception as e:
            logger.warning(f"Error loading hierarchy metadata: {e}")
        
        # Get discover metadata (for column type validation)
        try:
            discover_query = """
            SELECT 
                original_table_name,
                new_column_name,
                inferred_type,
                classification,
                cardinality
            FROM metadata.discover
            WHERE original_table_name LIKE '%_stage1'
            OR original_table_name IN (
                SELECT DISTINCT REPLACE(table_name, '_stage1', '') 
                FROM metadata.hierarchies
            )
            """
            
            discover_results = self.db_manager.execute_query_dict(discover_query)
            
            # Group by table
            for row in discover_results:
                table_name = row['original_table_name']
                if table_name not in metadata['discover']:
                    metadata['discover'][table_name] = []
                metadata['discover'][table_name].append(row)
            
            logger.info(f"Loaded discover metadata for {len(metadata['discover'])} tables")
            
        except Exception as e:
            logger.warning(f"Error loading discover metadata: {e}")
        
        # Get list of stage1 tables
        try:
            tables_query = """
            SELECT name 
            FROM system.tables 
            WHERE database = 'silver' 
            AND name LIKE '%_stage1'
            ORDER BY name
            """
            
            table_results = self.db_manager.execute_query_dict(tables_query)
            metadata['stage1_tables'] = [row['name'] for row in table_results]
            
            logger.info(f"Found {len(metadata['stage1_tables'])} Stage 1 tables")
            
        except Exception as e:
            logger.warning(f"Error discovering Stage 1 tables: {e}")
        
        return metadata
    
    def identify_dimension_tables(self, metadata: Dict[str, Any], fact_table_names: Set[str]) -> List[Dict[str, Any]]:
        """
        Identify dimension tables based on hierarchies.
        
        Each hierarchy becomes a dimension table, including:
        - Root, leaf, and intermediate columns
        - All sibling relationship columns
        - Related columns from discover metadata
        
        Args:
            metadata: Analyzed metadata dictionary
            fact_table_names: Set of table names that are identified as fact tables (to exclude)
            
        Returns:
            List of dimension table recommendations
        """
        dimension_tables = []
        dim_counter = 1
        
        for table_name, hierarchy_info in metadata['hierarchies'].items():
            # Skip tables that are identified as fact tables
            if table_name in fact_table_names:
                continue
            # Collect all columns that should be in this dimension
            dimension_columns = set()
            
            # Add root column
            if hierarchy_info.get('root_column'):
                dimension_columns.add(hierarchy_info['root_column'])
            
            # Add leaf column
            if hierarchy_info.get('leaf_column'):
                dimension_columns.add(hierarchy_info['leaf_column'])
            
            # Add intermediate levels
            intermediate = hierarchy_info.get('intermediate_levels', [])
            if intermediate:
                for col in intermediate:
                    dimension_columns.add(col)
            
            # Add columns from parent-child relationships
            parent_child = hierarchy_info.get('parent_child_relationships', [])
            for rel in parent_child:
                # Parse "parent -> child" format
                if '->' in rel:
                    parts = [p.strip() for p in rel.split('->')]
                    dimension_columns.update(parts)
            
            # Add columns from sibling relationships
            siblings = hierarchy_info.get('sibling_relationships', [])
            for sib in siblings:
                # Parse "col1 <-> col2" format
                if '<->' in sib:
                    parts = [p.strip() for p in sib.split('<->')]
                    dimension_columns.update(parts)
            
            # Get full column information from discover metadata
            original_table_name = table_name.replace('_stage1', '')
            column_details = []
            
            if original_table_name in metadata['discover']:
                for col_info in metadata['discover'][original_table_name]:
                    col_name = col_info.get('new_column_name') or col_info.get('original_column_name')
                    if col_name and col_name in dimension_columns:
                        column_details.append({
                            'column_name': col_name,
                            'data_type': col_info.get('inferred_type', 'String'),
                            'classification': col_info.get('classification', 'dimension'),
                            'cardinality': col_info.get('cardinality', 0)
                        })
            
            # Also get columns directly from the stage1 table structure
            # to ensure we have all dimension columns
            try:
                structure_query = f"""
                SELECT 
                    name as column_name,
                    type as data_type
                FROM system.columns 
                WHERE database = 'silver' 
                AND table = '{table_name}'
                ORDER BY position
                """
                
                structure_cols = self.db_manager.execute_query_dict(structure_query)
                
                for col in structure_cols:
                    col_name = col['column_name']
                    # Skip fact columns (numeric measures)
                    if self._is_fact_column(col['data_type'], col_name, table_name):
                        continue
                    
                    # Add to dimension columns if not already there
                    if col_name not in [c['column_name'] for c in column_details]:
                        column_details.append({
                            'column_name': col_name,
                            'data_type': col['data_type'],
                            'classification': 'dimension',
                            'cardinality': 0  # Will be calculated if needed
                        })
            except Exception as e:
                logger.warning(f"Error getting column structure for {table_name}: {e}")
            
            # Create dimension table recommendation
            dim_table = {
                'recommended_name': f'dimension{dim_counter}_dim',
                'source_table': table_name,
                'original_table_name': original_table_name,
                'hierarchy_name': hierarchy_info.get('hierarchy_name', ''),
                'root_column': hierarchy_info.get('root_column'),
                'leaf_column': hierarchy_info.get('leaf_column'),
                'columns': sorted(column_details, key=lambda x: x['column_name']),
                'total_columns': len(column_details),
                'hierarchy_levels': len(hierarchy_info.get('intermediate_levels', [])) + 2  # root + leaf
            }
            
            dimension_tables.append(dim_table)
            dim_counter += 1
        
        return dimension_tables
    
    def identify_fact_tables(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Identify fact tables based on ERD metadata and relationships.
        
        Fact tables are tables that:
        - Are identified as 'fact' type in ERD metadata
        - Have fact columns (measures)
        - Have relationships to dimension tables
        
        Args:
            metadata: Analyzed metadata dictionary
            
        Returns:
            List of fact table recommendations
        """
        fact_tables = []
        fact_counter = 1
        
        # Find fact tables from ERD metadata
        for table_name, erd_info in metadata['erd'].items():
            if erd_info.get('table_type') == 'fact':
                fact_columns = erd_info.get('fact_columns', [])
                dimension_columns = erd_info.get('dimension_columns', [])
                relationships = erd_info.get('relationships', [])
                
                # Get column details from discover metadata
                original_table_name = table_name.replace('_stage1', '')
                column_details = []
                
                # Get all columns from table structure first
                try:
                    structure_query = f"""
                    SELECT 
                        name as column_name,
                        type as data_type
                    FROM system.columns 
                    WHERE database = 'silver' 
                    AND table = '{table_name}'
                    ORDER BY position
                    """
                    
                    structure_cols = self.db_manager.execute_query_dict(structure_query)
                except Exception as e:
                    logger.warning(f"Error getting column structure for {table_name}: {e}")
                    structure_cols = []
                
                # Find dimension foreign keys from relationships and ERD metadata
                dimension_keys = set()
                
                # Get dimension columns from ERD metadata
                if dimension_columns:
                    dimension_keys.update(dimension_columns)
                
                # Parse relationships to find dimension keys
                if relationships:
                    for rel in relationships:
                        # Relationships are stored as strings, need to parse
                        # Format: "table1.column1 -> table2.column2"
                        if isinstance(rel, str) and '->' in rel:
                            parts = rel.split('->')
                            if len(parts) == 2:
                                fact_part = parts[0].strip()
                                if '.' in fact_part:
                                    fact_col = fact_part.split('.')[1].strip()
                                    # If this is a column from our fact table, it's a dimension key
                                    if fact_col:
                                        dimension_keys.add(fact_col)
                        elif isinstance(rel, dict):
                            # Relationship dict format
                            if 'table1' in rel and rel.get('table1') == table_name:
                                fact_col = rel.get('column1', '')
                                if fact_col:
                                    dimension_keys.add(fact_col)
                
                # For fact tables, all non-fact columns should be dimension keys (foreign keys)
                # Check table structure - any column that's not a fact is a dimension key
                for col in structure_cols:
                    col_name = col['column_name']
                    # Skip fact columns (measures) - they're not dimension keys
                    if not self._is_fact_column(col['data_type'], col_name, table_name):
                        # For fact tables, any non-fact column is a dimension key
                        dimension_keys.add(col_name)
                
                # Now build column_details: include both fact columns (measures) and dimension keys
                
                # Get data types from discover metadata if available
                discover_col_types = {}
                if original_table_name in metadata['discover']:
                    for col_info in metadata['discover'][original_table_name]:
                        col_name = col_info.get('new_column_name') or col_info.get('original_column_name')
                        discover_col_types[col_name] = col_info.get('inferred_type', 'String')
                
                # Add fact columns (measures)
                for col in structure_cols:
                    col_name = col['column_name']
                    if self._is_fact_column(col['data_type'], col_name, table_name):
                        data_type = discover_col_types.get(col_name, col['data_type'])
                        column_details.append({
                            'column_name': col_name,
                            'data_type': data_type,
                            'classification': 'fact',
                            'cardinality': 0
                        })
                
                # Add dimension keys (foreign keys)
                for col in structure_cols:
                    col_name = col['column_name']
                    if col_name in dimension_keys:
                        # Make sure we haven't already added it as a fact column
                        if col_name not in [c['column_name'] for c in column_details]:
                            data_type = discover_col_types.get(col_name, col['data_type'])
                            column_details.append({
                                'column_name': col_name,
                                'data_type': data_type,
                                'classification': 'dimension_key',
                                'cardinality': 0
                            })
                
                # Create fact table recommendation
                # Separate fact columns and dimension keys for clarity
                fact_cols_list = [c['column_name'] for c in column_details if c['classification'] == 'fact']
                dim_keys_list = [c['column_name'] for c in column_details if c['classification'] == 'dimension_key']
                
                fact_table = {
                    'recommended_name': f'fact{fact_counter}_fact',
                    'source_table': table_name,
                    'original_table_name': original_table_name,
                    'fact_columns': fact_cols_list,
                    'dimension_keys': dim_keys_list,
                    'columns': sorted(column_details, key=lambda x: x['column_name']),
                    'total_columns': len(column_details),
                    'relationships': relationships
                }
                
                fact_tables.append(fact_table)
                fact_counter += 1
        
        return fact_tables
    
    def _is_fact_column(self, data_type: str, column_name: str, table_name: str) -> bool:
        """
        Determine if a column is a fact column (measure).
        
        Args:
            data_type: Column data type
            column_name: Column name
            table_name: Table name
            
        Returns:
            bool: True if column is a fact/measure
        """
        data_type_lower = data_type.lower()
        column_name_lower = column_name.lower()
        table_name_lower = table_name.lower()
        
        # Dimension tables - all columns are dimensions
        dimension_table_patterns = ['calendar', 'time', 'date', 'dim']
        if any(pattern in table_name_lower for pattern in dimension_table_patterns):
            return False
        
        # ID columns are dimensions
        id_patterns = ['_id', '_key', 'id_', 'key_']
        if any(pattern in column_name_lower for pattern in id_patterns):
            return False
        
        # Numeric dimension attributes
        dimension_numeric_patterns = ['_num', '_code', '_flag', 'is_', 'has_', 'working', '_year', '_day', '_month', '_qtr', '_week']
        if any(pattern in column_name_lower for pattern in dimension_numeric_patterns):
            return False
        
        # Fact measure patterns
        fact_patterns = ['amount', 'total', 'sum', 'quantity', 'count', 'value', 'price', 'cost', 'revenue', 'sales_amount']
        if any(pattern in column_name_lower for pattern in fact_patterns):
            return True
        
        # Numeric types are typically facts
        numeric_types = ['float', 'int', 'uint', 'decimal', 'numeric']
        if any(num_type in data_type_lower for num_type in numeric_types):
            return True
        
        return False
    
    def generate_recommendations(self) -> Dict[str, Any]:
        """
        Generate dimensional model recommendations.
        
        Returns:
            Dict containing dimension and fact table recommendations
        """
        logger.info("Generating dimensional model recommendations...")
        
        # Analyze metadata
        metadata = self.analyze_metadata()
        
        # Identify fact tables from ERD first
        fact_tables = self.identify_fact_tables(metadata)
        
        # Get set of fact table names to exclude from dimension recommendations
        fact_table_names = {ft['source_table'] for ft in fact_tables}
        
        # Identify dimension tables from hierarchies (excluding fact tables)
        dimension_tables = self.identify_dimension_tables(metadata, fact_table_names)
        
        recommendations = {
            'recommendation_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_dimension_tables': len(dimension_tables),
            'total_fact_tables': len(fact_tables),
            'dimension_tables': dimension_tables,
            'fact_tables': fact_tables,
            'metadata_summary': {
                'erd_tables': len(metadata['erd']),
                'hierarchy_tables': len(metadata['hierarchies']),
                'discover_tables': len(metadata['discover']),
                'stage1_tables': len(metadata['stage1_tables'])
            }
        }
        
        self.recommendations = recommendations
        
        logger.info(f"Generated {len(dimension_tables)} dimension and {len(fact_tables)} fact table recommendations")
        
        return recommendations
    
    def store_recommendations(self, recommendations: Dict[str, Any]) -> bool:
        """
        Store recommendations in metadata.dimensional_model table.
        
        Args:
            recommendations: Generated recommendations dictionary
            
        Returns:
            bool: True if successful
        """
        try:
            # Create metadata.dimensional_model table if it doesn't exist
            # Table is unique by (table_type, recommended_name, source_table)
            # final_name can be updated by users to rename recommendations
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS metadata.dimensional_model (
                id UInt64,
                recommendation_timestamp DateTime,
                table_type String,
                recommended_name String,
                final_name String,
                source_table String,
                original_table_name String,
                hierarchy_name String,
                root_column String,
                leaf_column String,
                fact_columns Array(String),
                dimension_keys Array(String),
                columns Array(String),
                column_details Array(String),
                total_columns UInt32,
                hierarchy_levels UInt32,
                relationships Array(String),
                metadata_json String,
                created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (table_type, recommended_name, source_table)
            """
            
            self.db_manager.execute_command(create_table_sql)
            
            # Add final_name column if table exists but column doesn't (for existing tables)
            # Try to add the column - it will fail if it already exists, which is fine
            try:
                alter_sql = "ALTER TABLE metadata.dimensional_model ADD COLUMN final_name String"
                self.db_manager.execute_command(alter_sql)
                logger.info("Added final_name column to existing table")
            except Exception as e:
                # Column likely already exists or table is new - that's fine
                logger.debug(f"final_name column may already exist: {e}")
            
            # Truncate existing recommendations
            self.db_manager.execute_command("TRUNCATE TABLE metadata.dimensional_model")
            
            # Store dimension tables
            for dim_table in recommendations['dimension_tables']:
                columns_list = [col['column_name'] for col in dim_table['columns']]
                column_details_json = [f"{col['column_name']}:{col['data_type']}:{col['classification']}" 
                                      for col in dim_table['columns']]
                
                # Initialize final_name with recommended_name
                recommended_name = dim_table['recommended_name']
                final_name = recommended_name  # Initially same as recommended_name
                
                insert_sql = f"""
                INSERT INTO metadata.dimensional_model (
                    id, recommendation_timestamp, table_type, recommended_name, final_name,
                    source_table, original_table_name, hierarchy_name,
                    root_column, leaf_column, columns, column_details,
                    total_columns, hierarchy_levels, metadata_json
                ) VALUES (
                    {hash(f"{recommended_name}_{dim_table['source_table']}") % 2**63},
                    '{recommendations['recommendation_timestamp']}',
                    'dimension',
                    '{recommended_name}',
                    '{final_name}',
                    '{dim_table['source_table']}',
                    '{dim_table['original_table_name']}',
                    '{dim_table.get('hierarchy_name', '').replace("'", "''")}',
                    '{dim_table.get('root_column', '')}',
                    '{dim_table.get('leaf_column', '')}',
                    {repr(columns_list)},
                    {repr(column_details_json)},
                    {dim_table['total_columns']},
                    {dim_table.get('hierarchy_levels', 0)},
                    '{str(dim_table).replace("'", "''")[:1000]}'
                )
                """
                
                self.db_manager.execute_command(insert_sql)
            
            # Store fact tables
            for fact_table in recommendations['fact_tables']:
                columns_list = [col['column_name'] for col in fact_table['columns']]
                column_details_json = [f"{col['column_name']}:{col['data_type']}:{col['classification']}" 
                                      for col in fact_table['columns']]
                relationships_list = fact_table.get('relationships', [])
                if isinstance(relationships_list, str):
                    relationships_list = [relationships_list]
                
                # Initialize final_name with recommended_name
                recommended_name = fact_table['recommended_name']
                final_name = recommended_name  # Initially same as recommended_name
                
                insert_sql = f"""
                INSERT INTO metadata.dimensional_model (
                    id, recommendation_timestamp, table_type, recommended_name, final_name,
                    source_table, original_table_name, fact_columns,
                    dimension_keys, columns, column_details, total_columns,
                    relationships, metadata_json
                ) VALUES (
                    {hash(f"{recommended_name}_{fact_table['source_table']}") % 2**63},
                    '{recommendations['recommendation_timestamp']}',
                    'fact',
                    '{recommended_name}',
                    '{final_name}',
                    '{fact_table['source_table']}',
                    '{fact_table['original_table_name']}',
                    {repr(fact_table.get('fact_columns', []))},
                    {repr(fact_table.get('dimension_keys', []))},
                    {repr(columns_list)},
                    {repr(column_details_json)},
                    {fact_table['total_columns']},
                    {repr(relationships_list)},
                    '{str(fact_table).replace("'", "''")[:1000]}'
                )
                """
                
                self.db_manager.execute_command(insert_sql)
            
            logger.info(f"Stored {recommendations['total_dimension_tables']} dimension and "
                       f"{recommendations['total_fact_tables']} fact table recommendations")
            return True
            
        except Exception as e:
            logger.error(f"Error storing recommendations: {e}")
            return False
    
    def generate_stage3_transformations(self) -> Dict[str, Any]:
        """
        Generate stage3 transformation SQL for gold schema tables.
        
        For each recommendation, creates transformations with:
        1. DROP TABLE IF EXISTS gold.final_name
        2. CREATE TABLE gold.final_name with proper schema
        3. INSERT INTO gold.final_name SELECT ... FROM silver.source_table
        4. OPTIMIZE TABLE gold.final_name FINAL
        
        Returns:
            Dict containing transformation generation results
        """
        try:
            logger.info("Generating stage3 transformations for gold schema...")
            
            # Get all recommendations
            query = """
            SELECT 
                recommended_name,
                final_name,
                table_type,
                source_table,
                columns,
                column_details
            FROM metadata.dimensional_model
            ORDER BY table_type, recommended_name
            """
            
            recommendations = self.db_manager.execute_query_dict(query)
            
            if not recommendations:
                return {
                    "status": "error",
                    "message": "No recommendations found. Generate recommendations first.",
                    "transformations_created": 0
                }
            
            transformations_created = []
            transformation_counter = 1
            
            # Get next transformation_id
            max_id_query = """
            SELECT max(transformation_id) as max_id
            FROM metadata.transformation1
            WHERE transformation_stage = 'stage3'
            """
            max_result = self.db_manager.execute_query_dict(max_id_query)
            next_transformation_id = (max_result[0]['max_id'] + 1) if max_result and max_result[0].get('max_id') else 1
            
            for rec in recommendations:
                recommended_name = rec['recommended_name']
                final_name = rec.get('final_name') or recommended_name
                table_type = rec['table_type']
                source_table = rec['source_table']
                columns = rec.get('columns', [])
                column_details = rec.get('column_details', [])
                
                # Skip if no final_name set
                if not final_name or final_name == recommended_name:
                    logger.warning(f"Skipping {recommended_name} - final_name not set or same as recommended_name")
                    continue
                
                # Parse column details to get data types
                # Format: "column_name:data_type:classification"
                column_types = {}
                for detail in column_details:
                    if isinstance(detail, str) and ':' in detail:
                        parts = detail.split(':')
                        if len(parts) >= 2:
                            col_name = parts[0]
                            data_type = parts[1]
                            column_types[col_name] = data_type
                
                # Also get actual column types from source table to ensure accuracy
                try:
                    source_cols_query = f"""
                    SELECT 
                        name as column_name,
                        type as data_type
                    FROM system.columns
                    WHERE database = 'silver'
                        AND table = '{source_table}'
                    ORDER BY position
                    """
                    source_cols = self.db_manager.execute_query_dict(source_cols_query)
                    # Override with actual types from source table
                    for col in source_cols:
                        col_name = col['column_name']
                        if col_name in columns:  # Only include columns in our recommendation
                            column_types[col_name] = col['data_type']
                except Exception as e:
                    logger.warning(f"Could not get column types from source table {source_table}: {e}")
                    # Continue with types from column_details
                
                # Generate transformation name
                transformation_name = f"{final_name}_transformation"
                
                statements = []
                new_version = int(datetime.now().timestamp() * 1000000)
                
                # Statement 1: DROP TABLE IF EXISTS
                drop_sql = f"DROP TABLE IF EXISTS gold.{final_name};"
                statements.append({
                    'execution_sequence': 1,
                    'sql_statement': drop_sql,
                    'statement_type': 'DROP',
                    'description': f'Drop existing {final_name} table'
                })
                
                # Statement 2: CREATE TABLE
                # Build column definitions
                column_defs = []
                for col in columns:
                    col_name = col if isinstance(col, str) else col.get('column_name', str(col))
                    # Get data type from column_types or default to String
                    data_type = column_types.get(col_name, 'String')
                    column_defs.append(f"{col_name} {data_type}")
                
                # Determine ORDER BY clause based on table type
                if table_type == 'fact':
                    # For fact tables, order by dimension keys (foreign keys)
                    order_by_cols = [col for col in columns if col in rec.get('dimension_keys', [])]
                    if not order_by_cols:
                        # Fallback to first few columns
                        order_by_cols = columns[:3] if len(columns) >= 3 else columns
                    order_by = f"ORDER BY ({', '.join(order_by_cols)})"
                else:
                    # For dimension tables, order by root/leaf columns or primary key
                    root_col = rec.get('root_column')
                    leaf_col = rec.get('leaf_column')
                    if root_col and root_col in columns:
                        order_by = f"ORDER BY ({root_col})"
                    elif leaf_col and leaf_col in columns:
                        order_by = f"ORDER BY ({leaf_col})"
                    else:
                        # Use first column as fallback
                        order_by = f"ORDER BY ({columns[0]})" if columns else "ORDER BY tuple()"
                
                create_sql = f"""
CREATE TABLE gold.{final_name} (
    {', '.join(column_defs)}
) ENGINE = MergeTree()
{order_by};
""".strip()
                
                statements.append({
                    'execution_sequence': 2,
                    'sql_statement': create_sql,
                    'statement_type': 'CREATE',
                    'description': f'Create {final_name} table in gold schema'
                })
                
                # Statement 3: INSERT INTO ... SELECT
                # Map columns from silver to gold (assuming same names for now)
                select_cols = [col if isinstance(col, str) else col.get('column_name', str(col)) for col in columns]
                insert_sql = f"""
INSERT INTO gold.{final_name} ({', '.join(select_cols)})
SELECT {', '.join(select_cols)}
FROM silver.{source_table};
""".strip()
                
                statements.append({
                    'execution_sequence': 3,
                    'sql_statement': insert_sql,
                    'statement_type': 'INSERT',
                    'description': f'Populate {final_name} from {source_table}'
                })
                
                # Statement 4: OPTIMIZE TABLE
                optimize_sql = f"OPTIMIZE TABLE gold.{final_name} FINAL;"
                statements.append({
                    'execution_sequence': 4,
                    'sql_statement': optimize_sql,
                    'statement_type': 'OPTIMIZE',
                    'description': f'Optimize {final_name} table'
                })
                
                # Insert statements into transformation1
                for statement in statements:
                    escaped_sql = statement['sql_statement'].replace("'", "''")
                    
                    insert_transformation_sql = f"""
                    INSERT INTO metadata.transformation1 (
                        transformation_stage,
                        transformation_id,
                        transformation_name,
                        transformation_schema_name,
                        dependencies,
                        execution_frequency,
                        source_schema,
                        source_table,
                        target_schema,
                        target_table,
                        execution_sequence,
                        sql_statement,
                        statement_type,
                        version
                    ) VALUES (
                        'stage3',
                        {next_transformation_id},
                        '{transformation_name}',
                        'metadata',
                        '[]',
                        'daily',
                        'silver',
                        '{source_table}',
                        'gold',
                        '{final_name}',
                        {statement['execution_sequence']},
                        '{escaped_sql}',
                        '{statement['statement_type']}',
                        {new_version}
                    )
                    """
                    
                    self.db_manager.execute_command(insert_transformation_sql)
                    new_version += 1  # Increment version for each statement
                
                transformations_created.append({
                    'transformation_name': transformation_name,
                    'transformation_id': next_transformation_id,
                    'final_name': final_name,
                    'source_table': source_table,
                    'table_type': table_type,
                    'statements': len(statements)
                })
                
                next_transformation_id += 1
            
            logger.info(f"Generated {len(transformations_created)} stage3 transformations")
            
            return {
                "status": "success",
                "message": f"Generated {len(transformations_created)} stage3 transformations",
                "transformations_created": len(transformations_created),
                "transformations": transformations_created
            }
            
        except Exception as e:
            logger.error(f"Error generating stage3 transformations: {e}")
            return {
                "status": "error",
                "message": f"Failed to generate stage3 transformations: {str(e)}",
                "transformations_created": 0
            }

