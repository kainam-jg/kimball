#!/usr/bin/env python3
"""
KIMBALL Model Phase - ERD Analysis

This module provides ERD (Entity Relationship Diagram) analysis for the Model Phase.
It analyzes relationships between tables in the Stage 1 silver schema and generates
ERD metadata for dimensional modeling.

Based on the archive kimball_primary_key_analyzer.py with enhancements for Stage 1 data.
"""

from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict
import logging
from datetime import datetime

from ..core.database import DatabaseManager
from ..core.config import Config

logger = logging.getLogger(__name__)


class ERDAnalyzer:
    """
    Analyzes Entity Relationship Diagrams for Stage 1 silver tables.
    
    This class discovers:
    - Primary key candidates
    - Foreign key relationships
    - Join relationships between tables
    - Fact vs dimension table identification
    - ERD metadata for dimensional modeling
    """
    
    def __init__(self):
        """Initialize ERD analyzer with database connection."""
        self.db_manager = DatabaseManager()
        self.config = Config()
        self.stage1_tables = []
        self.table_metadata = {}
        self.erd_relationships = []
        
        # Load ignore fields configuration
        config_data = self.config.get_config()
        self.ignore_join_fields = config_data.get('model_settings', {}).get('ignore_join_fields', [])
        logger.info(f"Loaded ignore join fields: {self.ignore_join_fields}")
        
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
    
    def analyze_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Analyze metadata for a single Stage 1 table.
        
        Args:
            table_name (str): Name of the Stage 1 table
            
        Returns:
            Dict[str, Any]: Table metadata including columns and relationships
        """
        try:
            # Get table structure
            structure_query = f"""
            SELECT 
                name as column_name,
                type as data_type,
                default_kind,
                is_in_partition_key,
                is_in_sorting_key,
                is_in_primary_key
            FROM system.columns 
            WHERE database = 'silver' 
            AND table = '{table_name}'
            ORDER BY position
            """
            
            columns = self.db_manager.execute_query_dict(structure_query)
            
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM silver.{table_name}"
            count_result = self.db_manager.execute_query_dict(count_query)
            row_count = count_result[0]['row_count'] if count_result else 0
            
            # Analyze each column
            column_analysis = []
            for col in columns:
                col_analysis = self._analyze_column(table_name, col)
                column_analysis.append(col_analysis)
            
            # Determine table type (fact vs dimension)
            table_type = self._determine_table_type(column_analysis, row_count)
            
            metadata = {
                'table_name': table_name,
                'row_count': row_count,
                'column_count': len(columns),
                'table_type': table_type,
                'columns': column_analysis,
                'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.table_metadata[table_name] = metadata
            return metadata
            
        except Exception as e:
            logger.error(f"Error analyzing table {table_name}: {e}")
            return {}
    
    def _analyze_column(self, table_name: str, column_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a single column for ERD purposes.
        
        Args:
            table_name (str): Name of the table
            column_info (Dict[str, Any]): Column information from system.columns
            
        Returns:
            Dict[str, Any]: Column analysis results
        """
        column_name = column_info['column_name']
        data_type = column_info['data_type']
        
        try:
            # Get cardinality and null count
            cardinality_query = f"""
            SELECT 
                COUNT(DISTINCT {column_name}) as cardinality,
                COUNT(*) - COUNT({column_name}) as null_count,
                COUNT(*) as total_count
            FROM silver.{table_name}
            """
            
            stats = self.db_manager.execute_query_dict(cardinality_query)
            if stats:
                cardinality = stats[0]['cardinality']
                null_count = stats[0]['null_count']
                total_count = stats[0]['total_count']
                null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
            else:
                cardinality = 0
                null_count = 0
                null_percentage = 0
            
            # Get sample values
            sample_query = f"""
            SELECT DISTINCT {column_name} as sample_value
            FROM silver.{table_name}
            WHERE {column_name} IS NOT NULL
            ORDER BY {column_name}
            LIMIT 5
            """
            
            samples = self.db_manager.execute_query_dict(sample_query)
            sample_values = [row['sample_value'] for row in samples] if samples else []
            
            # Determine if it's a primary key candidate
            is_pk_candidate = self._is_primary_key_candidate(
                column_name, data_type, cardinality, null_count, total_count
            )
            
            # Determine column classification
            classification = self._classify_column(
                column_name, data_type, cardinality, null_percentage
            )
            
            # Calculate data quality score
            quality_score = self._calculate_quality_score(
                cardinality, null_percentage, total_count
            )
            
            return {
                'column_name': column_name,
                'data_type': data_type,
                'cardinality': cardinality,
                'null_count': null_count,
                'null_percentage': null_percentage,
                'sample_values': sample_values,
                'is_primary_key_candidate': is_pk_candidate,
                'classification': classification,
                'data_quality_score': quality_score,
                'cardinality_ratio': cardinality / total_count if total_count > 0 else 0,
                'is_in_primary_key': column_info.get('is_in_primary_key', False),
                'is_in_sorting_key': column_info.get('is_in_sorting_key', False)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing column {table_name}.{column_name}: {e}")
            return {
                'column_name': column_name,
                'data_type': data_type,
                'cardinality': 0,
                'null_count': 0,
                'null_percentage': 0,
                'sample_values': [],
                'is_primary_key_candidate': False,
                'classification': 'unknown',
                'data_quality_score': 0.0,
                'cardinality_ratio': 0.0,
                'is_in_primary_key': False,
                'is_in_sorting_key': False
            }
    
    def _is_primary_key_candidate(self, column_name: str, data_type: str, 
                                 cardinality: int, null_count: int, total_count: int) -> bool:
        """
        Determine if a column is a primary key candidate.
        
        Args:
            column_name (str): Column name
            data_type (str): Column data type
            cardinality (int): Number of distinct values
            null_count (int): Number of null values
            total_count (int): Total number of rows
            
        Returns:
            bool: True if column is a primary key candidate
        """
        # Must have no nulls
        if null_count > 0:
            return False
        
        # Must have unique values (cardinality = total_count)
        if cardinality != total_count:
            return False
        
        # Prefer certain data types
        preferred_types = ['string', 'int', 'uint']
        data_type_lower = data_type.lower()
        
        # Check if data type is suitable for primary key
        type_suitable = any(pref in data_type_lower for pref in preferred_types)
        
        # Check if column name suggests it's an ID
        name_suggests_id = any(indicator in column_name.lower() 
                              for indicator in ['id', 'key', 'code', 'num', 'no'])
        
        return type_suitable and (name_suggests_id or cardinality > 1000)
    
    def _classify_column(self, column_name: str, data_type: str, 
                        cardinality: int, null_percentage: float) -> str:
        """
        Classify a column as fact or dimension.
        
        Args:
            column_name (str): Column name
            data_type (str): Column data type
            cardinality (int): Number of distinct values
            null_percentage (float): Percentage of null values
            
        Returns:
            str: Column classification ('fact' or 'dimension')
        """
        # Numeric columns are typically facts
        numeric_types = ['float', 'int', 'uint', 'decimal']
        if any(num_type in data_type.lower() for num_type in numeric_types):
            return 'fact'
        
        # High cardinality string columns might be facts (IDs)
        if cardinality > 10000 and 'string' in data_type.lower():
            return 'fact'
        
        # Everything else is a dimension
        return 'dimension'
    
    def _determine_table_type(self, columns: List[Dict[str, Any]], row_count: int) -> str:
        """
        Determine if a table is a fact table or dimension table.
        
        Args:
            columns (List[Dict[str, Any]]): Column analysis results
            row_count (int): Number of rows in the table
            
        Returns:
            str: Table type ('fact' or 'dimension')
        """
        fact_columns = [col for col in columns if col['classification'] == 'fact']
        dimension_columns = [col for col in columns if col['classification'] == 'dimension']
        
        # If more fact columns than dimension columns, it's likely a fact table
        if len(fact_columns) > len(dimension_columns):
            return 'fact'
        
        # If high row count and many dimension columns, it's likely a fact table
        if row_count > 100000 and len(dimension_columns) > 3:
            return 'fact'
        
        # Otherwise, it's a dimension table
        return 'dimension'
    
    def _calculate_quality_score(self, cardinality: int, null_percentage: float, 
                                total_count: int) -> float:
        """
        Calculate data quality score for a column.
        
        Args:
            cardinality (int): Number of distinct values
            null_percentage (float): Percentage of null values
            total_count (int): Total number of rows
            
        Returns:
            float: Quality score between 0 and 1
        """
        score = 1.0
        
        # Penalize high null percentage
        if null_percentage > 10:
            score -= 0.3
        elif null_percentage > 5:
            score -= 0.1
        
        # Reward high cardinality for dimension columns
        if cardinality > 1000:
            score += 0.1
        elif cardinality > 100:
            score += 0.05
        
        # Penalize very low cardinality (unless it's a flag)
        if cardinality < 10 and total_count > 1000:
            score -= 0.2
        
        return max(0.0, min(1.0, score))
    
    def find_join_relationships(self) -> List[Dict[str, Any]]:
        """
        Find potential join relationships between Stage 1 tables.
        
        Returns:
            List[Dict[str, Any]]: List of potential join relationships
        """
        relationships = []
        
        # Get all primary key candidates
        pk_candidates = {}
        for table_name, metadata in self.table_metadata.items():
            pk_candidates[table_name] = [
                col for col in metadata['columns'] 
                if col['is_primary_key_candidate']
            ]
        
        # Look for common column names and types
        column_info = defaultdict(list)
        for table_name, metadata in self.table_metadata.items():
            for col in metadata['columns']:
                col_name = col['column_name'].lower()
                column_info[col_name].append({
                    'table': table_name,
                    'column': col['column_name'],
                    'type': col['data_type'],
                    'cardinality': col['cardinality'],
                    'is_pk_candidate': col['is_primary_key_candidate'],
                    'classification': col['classification']
                })
        
        # Find columns that appear in multiple tables
        for col_name, occurrences in column_info.items():
            if len(occurrences) > 1:
                # Skip ignored fields
                if col_name in self.ignore_join_fields:
                    logger.debug(f"Skipping ignored field '{col_name}' for join relationships")
                    continue
                
                # Check if types are compatible
                for i, occ1 in enumerate(occurrences):
                    for j, occ2 in enumerate(occurrences[i+1:], i+1):
                        if self._are_types_compatible(occ1['type'], occ2['type']):
                            relationship = {
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
                                'classification1': occ1['classification'],
                                'classification2': occ2['classification'],
                                'join_confidence': self._calculate_join_confidence(occ1, occ2),
                                'relationship_type': self._determine_relationship_type(occ1, occ2)
                            }
                            relationships.append(relationship)
        
        # Sort by join confidence
        relationships.sort(key=lambda x: x['join_confidence'], reverse=True)
        self.erd_relationships = relationships
        
        return relationships
    
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
    
    def _determine_relationship_type(self, col1: Dict[str, Any], col2: Dict[str, Any]) -> str:
        """
        Determine the type of relationship between two columns.
        
        Args:
            col1 (Dict[str, Any]): First column info
            col2 (Dict[str, Any]): Second column info
            
        Returns:
            str: Relationship type ('one_to_one', 'one_to_many', 'many_to_many')
        """
        # If both are primary key candidates, likely one-to-one
        if col1['is_pk_candidate'] and col2['is_pk_candidate']:
            return 'one_to_one'
        
        # If one is primary key candidate, likely one-to-many
        if col1['is_pk_candidate'] or col2['is_pk_candidate']:
            return 'one_to_many'
        
        # Otherwise, many-to-many
        return 'many_to_many'
    
    def generate_erd_metadata(self) -> Dict[str, Any]:
        """
        Generate comprehensive ERD metadata for all Stage 1 tables.
        
        Returns:
            Dict[str, Any]: Complete ERD metadata
        """
        # Discover and analyze all Stage 1 tables
        self.discover_stage1_tables()
        
        for table_name in self.stage1_tables:
            self.analyze_table_metadata(table_name)
        
        # Find join relationships
        relationships = self.find_join_relationships()
        
        # Generate ERD metadata
        erd_metadata = {
            'schema_name': 'silver',
            'analysis_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_tables': len(self.stage1_tables),
            'total_relationships': len(relationships),
            'tables': self.table_metadata,
            'relationships': relationships,
            'summary': {
                'fact_tables': len([t for t in self.table_metadata.values() if t['table_type'] == 'fact']),
                'dimension_tables': len([t for t in self.table_metadata.values() if t['table_type'] == 'dimension']),
                'high_confidence_relationships': len([r for r in relationships if r['join_confidence'] > 0.7]),
                'primary_key_candidates': sum(len([c for c in t['columns'] if c['is_primary_key_candidate']]) 
                                            for t in self.table_metadata.values())
            }
        }
        
        return erd_metadata
    
    def store_erd_metadata(self, erd_metadata: Dict[str, Any]) -> bool:
        """
        Store ERD metadata in the metadata.erd table.
        
        Args:
            erd_metadata (Dict[str, Any]): ERD metadata to store
            
        Returns:
            bool: True if successful
        """
        try:
            # Create metadata.erd table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS metadata.erd (
                id UInt64,
                schema_name String,
                analysis_timestamp DateTime,
                table_name String,
                table_type String,
                row_count UInt64,
                column_count UInt32,
                primary_key_candidates Array(String),
                fact_columns Array(String),
                dimension_columns Array(String),
                relationships Array(String),
                metadata_json String,
                created_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (schema_name, table_name, analysis_timestamp)
            """
            
            self.db_manager.execute_query(create_table_sql)
            
            # Insert ERD metadata
            for table_name, table_metadata in erd_metadata['tables'].items():
                pk_candidates = [col['column_name'] for col in table_metadata['columns'] 
                               if col['is_primary_key_candidate']]
                fact_columns = [col['column_name'] for col in table_metadata['columns'] 
                              if col['classification'] == 'fact']
                dimension_columns = [col['column_name'] for col in table_metadata['columns'] 
                                   if col['classification'] == 'dimension']
                
                # Get relationships for this table
                table_relationships = [
                    f"{rel['table1']}.{rel['column1']} = {rel['table2']}.{rel['column2']}"
                    for rel in erd_metadata['relationships']
                    if rel['table1'] == table_name or rel['table2'] == table_name
                ]
                
                insert_sql = f"""
                INSERT INTO metadata.erd (
                    id, schema_name, analysis_timestamp, table_name, table_type,
                    row_count, column_count, primary_key_candidates, fact_columns,
                    dimension_columns, relationships, metadata_json
                ) VALUES (
                    {hash(table_name) % 2**63}, '{erd_metadata['schema_name']}',
                    '{erd_metadata['analysis_timestamp']}', '{table_name}',
                    '{table_metadata['table_type']}', {table_metadata['row_count']},
                    {table_metadata['column_count']}, {repr(pk_candidates)},
                    {repr(fact_columns)}, {repr(dimension_columns)}, {repr(table_relationships)},
                    '{str(table_metadata).replace("'", "''")}'
                )
                """
                
                self.db_manager.execute_query(insert_sql)
            
            logger.info(f"Stored ERD metadata for {len(erd_metadata['tables'])} tables")
            return True
            
        except Exception as e:
            logger.error(f"Error storing ERD metadata: {e}")
            return False
