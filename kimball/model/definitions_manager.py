#!/usr/bin/env python3
"""
KIMBALL Definitions Manager

This module manages column-level metadata definitions for all tables across schemas.
Provides functionality to:
- Seed definitions from system.columns
- Generate intelligent column descriptions
- Update column descriptions
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..core.database import DatabaseManager

logger = logging.getLogger(__name__)


class DefinitionsManager:
    """
    Manages column-level metadata definitions for all tables.
    """
    
    def __init__(self):
        """Initialize the definitions manager."""
        self.db_manager = DatabaseManager()
    
    def create_definitions_table(self) -> bool:
        """
        Create the metadata.definitions table if it doesn't exist.
        
        Returns:
            bool: True if successful
        """
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS metadata.definitions (
                id UInt64,
                schema_name String,
                table_name String,
                column_name String,
                column_type String,
                column_precision String,
                column_description String,
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (schema_name, table_name, column_name)
            SETTINGS index_granularity = 8192;
            """
            
            self.db_manager.execute_command(create_table_sql)
            logger.info("Created metadata.definitions table")
            return True
            
        except Exception as e:
            logger.error(f"Error creating definitions table: {e}")
            return False
    
    def seed_definitions_from_schemas(self, schema_names: List[str] = None) -> Dict[str, Any]:
        """
        Seed the definitions table from all tables in specified schemas.
        
        Args:
            schema_names (List[str]): List of schema names to process. Defaults to ['bronze', 'silver', 'gold']
        
        Returns:
            Dict[str, Any]: Seeding results
        """
        if schema_names is None:
            schema_names = ['bronze', 'silver', 'gold']
        
        try:
            self.create_definitions_table()
            
            total_tables = 0
            total_columns = 0
            schema_stats = {}
            
            for schema_name in schema_names:
                logger.info(f"Seeding definitions for schema: {schema_name}")
                
                # Get all tables in this schema
                tables_query = f"""
                SELECT name as table_name
                FROM system.tables
                WHERE database = '{schema_name}'
                ORDER BY name
                """
                
                tables = self.db_manager.execute_query_dict(tables_query)
                schema_table_count = 0
                schema_column_count = 0
                
                for table in tables:
                    table_name = table['table_name']
                    schema_table_count += 1
                    total_tables += 1
                    
                    # Get column information from system.columns
                    columns_query = f"""
                    SELECT 
                        name as column_name,
                        type as column_type,
                        position
                    FROM system.columns
                    WHERE database = '{schema_name}'
                    AND table = '{table_name}'
                    ORDER BY position
                    """
                    
                    columns = self.db_manager.execute_query_dict(columns_query)
                    
                    for col in columns:
                        column_name = col['column_name']
                        column_type = col['column_type']
                        
                        # Extract precision from column type (e.g., Decimal(15,2) -> "15,2")
                        column_precision = ""
                        if "(" in column_type and ")" in column_type:
                            precision_part = column_type.split("(")[1].split(")")[0]
                            column_precision = precision_part
                        
                        # Generate ID from schema, table, and column name
                        col_id = hash(f"{schema_name}.{table_name}.{column_name}") % (2**63)
                        
                        # Insert or update definition (ReplacingMergeTree will handle deduplication)
                        insert_sql = f"""
                        INSERT INTO metadata.definitions (
                            id,
                            schema_name,
                            table_name,
                            column_name,
                            column_type,
                            column_precision,
                            column_description,
                            created_at,
                            updated_at
                        ) VALUES (
                            {col_id},
                            '{schema_name}',
                            '{table_name}',
                            '{column_name}',
                            '{column_type}',
                            '{column_precision}',
                            '',
                            '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                            '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                        )
                        """
                        
                        self.db_manager.execute_command(insert_sql)
                        schema_column_count += 1
                        total_columns += 1
                    
                    logger.info(f"Processed {table_name}: {len(columns)} columns")
                
                schema_stats[schema_name] = {
                    'tables': schema_table_count,
                    'columns': schema_column_count
                }
                
                logger.info(f"Schema {schema_name}: {schema_table_count} tables, {schema_column_count} columns")
            
            return {
                'status': 'success',
                'message': f'Seeded definitions for {len(schema_names)} schemas',
                'total_tables': total_tables,
                'total_columns': total_columns,
                'schema_stats': schema_stats
            }
            
        except Exception as e:
            logger.error(f"Error seeding definitions: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'total_tables': 0,
                'total_columns': 0,
                'schema_stats': {}
            }
    
    def generate_gold_descriptions(self) -> Dict[str, Any]:
        """
        Intelligently generate column descriptions for gold schema tables.
        
        Uses knowledge from:
        - Column names and types
        - ERD metadata (table types, relationships)
        - Dimensional model metadata
        - Discovery metadata
        - Sample data analysis
        
        Returns:
            Dict[str, Any]: Generation results
        """
        try:
            # Get all gold schema definitions
            gold_query = """
            SELECT 
                id,
                schema_name,
                table_name,
                column_name,
                column_type,
                column_description
            FROM metadata.definitions
            WHERE schema_name = 'gold'
            AND column_description = ''
            ORDER BY table_name, column_name
            """
            
            gold_definitions = self.db_manager.execute_query_dict(gold_query)
            
            if not gold_definitions:
                return {
                    'status': 'success',
                    'message': 'No gold schema definitions found without descriptions',
                    'descriptions_generated': 0
                }
            
            descriptions_generated = 0
            table_descriptions = {}
            
            # Get ERD metadata for gold schema
            erd_query = """
            SELECT 
                schema_name,
                table_name,
                table_type,
                primary_key_candidates,
                fact_columns,
                dimension_columns
            FROM metadata.erd
            WHERE schema_name = 'gold'
            """
            erd_data = self.db_manager.execute_query_dict(erd_query)
            erd_map = {row['table_name']: row for row in erd_data if erd_data}
            
            # Get dimensional model metadata
            dim_model_query = """
            SELECT 
                final_name,
                table_type,
                columns,
                column_details
            FROM metadata.dimensional_model
            """
            dim_model_data = self.db_manager.execute_query_dict(dim_model_query)
            dim_model_map = {row['final_name']: row for row in dim_model_data if dim_model_data}
            
            # Get discovery metadata for reference
            discovery_query = """
            SELECT DISTINCT
                new_table_name,
                new_column_name,
                inferred_type,
                classification
            FROM metadata.discover
            """
            discovery_data = self.db_manager.execute_query_dict(discovery_query)
            discovery_map = {}
            for row in discovery_data:
                key = f"{row['new_table_name']}.{row['new_column_name']}"
                discovery_map[key] = row
            
            # Process each definition
            for def_row in gold_definitions:
                table_name = def_row['table_name']
                column_name = def_row['column_name']
                column_type = def_row['column_type']
                def_id = def_row['id']
                
                # Generate intelligent description
                description = self._generate_column_description(
                    table_name=table_name,
                    column_name=column_name,
                    column_type=column_type,
                    erd_info=erd_map.get(table_name),
                    dim_model_info=dim_model_map.get(table_name),
                    discovery_info=discovery_map.get(f"{table_name}.{column_name}")
                )
                
                if description:
                    # Update the description
                    update_sql = f"""
                    INSERT INTO metadata.definitions (
                        id,
                        schema_name,
                        table_name,
                        column_name,
                        column_type,
                        column_precision,
                        column_description,
                    created_at,
                    updated_at
                ) VALUES (
                    {def_id},
                    'gold',
                    '{table_name}',
                    '{column_name}',
                    '{column_type}',
                    '',
                    '{description.replace("'", "''")}',
                    '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                    '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
                    )
                    """
                    
                    self.db_manager.execute_command(update_sql)
                    descriptions_generated += 1
                    
                    if table_name not in table_descriptions:
                        table_descriptions[table_name] = 0
                    table_descriptions[table_name] += 1
            
            return {
                'status': 'success',
                'message': f'Generated {descriptions_generated} column descriptions for gold schema',
                'descriptions_generated': descriptions_generated,
                'table_descriptions': table_descriptions
            }
            
        except Exception as e:
            logger.error(f"Error generating gold descriptions: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'descriptions_generated': 0
            }
    
    def _generate_column_description(
        self,
        table_name: str,
        column_name: str,
        column_type: str,
        erd_info: Optional[Dict[str, Any]] = None,
        dim_model_info: Optional[Dict[str, Any]] = None,
        discovery_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate an intelligent column description based on available metadata.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            column_type: Data type of the column
            erd_info: ERD metadata for the table
            dim_model_info: Dimensional model metadata for the table
            discovery_info: Discovery metadata for the column
            
        Returns:
            str: Generated description
        """
        # Determine table type
        table_type = None
        if erd_info:
            table_type = erd_info.get('table_type', '').lower()
        elif dim_model_info:
            table_type = dim_model_info.get('table_type', '').lower()
        
        # Check if this is a fact or dimension table
        is_fact_table = table_type == 'fact' or table_name.endswith('_fact') or table_name.endswith('_k')
        is_dim_table = table_type == 'dimension' or table_name.endswith('_dim')
        
        # Check if this is a K-Table
        is_k_table = table_name.endswith('_k')
        
        # Generate description based on column name patterns and metadata
        description_parts = []
        
        # Primary key detection
        if erd_info and column_name in erd_info.get('primary_key_candidates', []):
            description_parts.append("Primary key candidate")
        
        # Fact vs Dimension column detection
        if erd_info:
            if column_name in erd_info.get('fact_columns', []):
                description_parts.append("Fact measure")
            elif column_name in erd_info.get('dimension_columns', []):
                description_parts.append("Dimension attribute")
        
        # Dimension key detection (for fact tables)
        if is_fact_table and (column_name.endswith('_key') or column_name.endswith('_id')):
            dim_name = column_name.replace('_key', '').replace('_id', '')
            description_parts.append(f"Foreign key to {dim_name} dimension")
        
        # Common column name patterns
        if 'amount' in column_name.lower() or 'sales' in column_name.lower() or 'revenue' in column_name.lower():
            description_parts.append("Monetary measure")
            if is_fact_table:
                description_parts.append("Fact measure representing sales/amount data")
        
        if 'date' in column_name.lower() or 'time' in column_name.lower():
            if is_dim_table or 'calendar' in table_name.lower():
                description_parts.append("Date attribute for time dimension")
            else:
                description_parts.append("Date/timestamp field")
        
        if 'name' in column_name.lower():
            description_parts.append("Descriptive name attribute")
        
        if 'id' in column_name.lower() or 'key' in column_name.lower():
            if not description_parts or "Primary key" not in description_parts[0]:
                description_parts.append("Unique identifier")
        
        # Data type specific descriptions
        if 'Decimal' in column_type or 'Float' in column_type:
            if not any('Monetary' in p or 'measure' in p.lower() for p in description_parts):
                description_parts.append("Numeric measure")
        
        if 'String' in column_type:
            if not description_parts:
                description_parts.append("Text attribute")
        
        if 'Date' in column_type or 'DateTime' in column_type:
            if not any('date' in p.lower() or 'time' in p.lower() for p in description_parts):
                description_parts.append("Date/timestamp value")
        
        # Table-specific descriptions
        if 'calendar' in table_name.lower():
            if 'year' in column_name.lower():
                description_parts.append("Calendar year")
            elif 'quarter' in column_name.lower():
                description_parts.append("Calendar quarter")
            elif 'month' in column_name.lower():
                description_parts.append("Calendar month")
            elif 'week' in column_name.lower():
                description_parts.append("Calendar week")
            elif 'day' in column_name.lower():
                description_parts.append("Calendar day")
        
        if 'geography' in table_name.lower() or 'region' in table_name.lower():
            if 'region' in column_name.lower():
                description_parts.append("Geographic region")
            elif 'state' in column_name.lower():
                description_parts.append("State/province")
            elif 'city' in column_name.lower():
                description_parts.append("City/location")
            elif 'country' in column_name.lower():
                description_parts.append("Country")
        
        if 'product' in table_name.lower():
            if 'product' in column_name.lower():
                description_parts.append("Product identifier or name")
            elif 'category' in column_name.lower():
                description_parts.append("Product category")
        
        # K-Table specific handling
        if is_k_table:
            # K-Table columns are prefixed with dimension name
            if '_' in column_name and not column_name.startswith(table_name.split('_')[0]):
                # Likely a dimension column (prefixed)
                description_parts.append("Denormalized dimension attribute from K-Table")
        
        # Combine description parts
        if description_parts:
            description = ". ".join(description_parts)
            if not description.endswith('.'):
                description += "."
        else:
            # Fallback generic description
            description = f"{column_type} column in {table_name} table."
        
        return description
    
    def update_column_description(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        description: str
    ) -> Dict[str, Any]:
        """
        Update the column description for a specific column.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            column_name: Column name
            description: New description
        
        Returns:
            Dict[str, Any]: Update results
        """
        try:
            # Get the existing definition to get the ID and column_type
            get_query = f"""
            SELECT 
                id,
                column_type,
                column_precision
            FROM metadata.definitions
            WHERE schema_name = '{schema_name}'
            AND table_name = '{table_name}'
            AND column_name = '{column_name}'
            ORDER BY updated_at DESC
            LIMIT 1
            """
            
            existing = self.db_manager.execute_query_dict(get_query)
            
            if not existing:
                return {
                    'status': 'error',
                    'message': f'Definition not found for {schema_name}.{table_name}.{column_name}'
                }
            
            def_id = existing[0]['id']
            column_type = existing[0]['column_type']
            column_precision = existing[0].get('column_precision', '')
            
            # Update using INSERT (ReplacingMergeTree will handle deduplication)
            update_sql = f"""
            INSERT INTO metadata.definitions (
                id,
                schema_name,
                table_name,
                column_name,
                column_type,
                column_precision,
                column_description,
                created_at,
                updated_at
            ) VALUES (
                {def_id},
                '{schema_name}',
                '{table_name}',
                '{column_name}',
                '{column_type}',
                '{column_precision}',
                '{description.replace("'", "''")}',
                '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                '{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            )
            """
            
            self.db_manager.execute_command(update_sql)
            
            return {
                'status': 'success',
                'message': f'Updated description for {schema_name}.{table_name}.{column_name}',
                'schema_name': schema_name,
                'table_name': table_name,
                'column_name': column_name,
                'description': description
            }
            
        except Exception as e:
            logger.error(f"Error updating column description: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def get_definitions(
        self,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """
        Get definitions with optional filters.
        
        Args:
            schema_name: Optional schema filter
            table_name: Optional table filter
            limit: Maximum number of records
        
        Returns:
            Dict[str, Any]: Definitions data
        """
        try:
            query = """
            SELECT 
                schema_name,
                table_name,
                column_name,
                column_type,
                column_precision,
                column_description,
                created_at,
                updated_at
            FROM metadata.definitions
            WHERE 1=1
            """
            
            if schema_name:
                query += f" AND schema_name = '{schema_name}'"
            
            if table_name:
                query += f" AND table_name = '{table_name}'"
            
            query += f" ORDER BY schema_name, table_name, column_name LIMIT {limit}"
            
            results = self.db_manager.execute_query_dict(query)
            
            return {
                'status': 'success',
                'count': len(results),
                'data': results
            }
            
        except Exception as e:
            logger.error(f"Error getting definitions: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'count': 0,
                'data': []
            }

