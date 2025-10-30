import json
from typing import List, Optional
from .sql_transformation import SQLTransformation, TransformationStage
from .database import DatabaseManager

class TransformationStorage:
    """Manages storage and retrieval of SQL transformations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def get_transformation_table(self, stage: TransformationStage) -> str:
        """Get the appropriate transformation table for the stage"""
        stage_to_table = {
            TransformationStage.STAGE1: "metadata.transformation1",
            TransformationStage.STAGE2: "metadata.transformation2", 
            TransformationStage.STAGE3: "metadata.transformation3",
            TransformationStage.STAGE4: "metadata.transformation4"
        }
        return stage_to_table[stage]
    
    def store_transformation(self, transformation: SQLTransformation) -> bool:
        """Store a transformation in the appropriate table"""
        try:
            table_name = self.get_transformation_table(transformation.get_stage())
            json_data = transformation.to_json()
            
            # Escape JSON for SQL insertion into ClickHouse
            # json.dumps() properly escapes newlines as \n, but we need to escape
            # the backslashes for ClickHouse string literals
            json_string = json.dumps(json_data)
            # Escape single quotes and backslashes for ClickHouse
            escaped_json = json_string.replace("\\", "\\\\").replace("'", "''")
            
            # Deprecated: source/target schema & table columns are no longer populated
            
            # Get metadata values
            dependencies = transformation.get_metadata('dependencies', [])
            execution_frequency = transformation.get_metadata('execution_frequency', 'daily')
            transformation_schema_name = transformation.get_metadata('transformation_schema_name', 'metadata')
            
            # Only keep dependencies (Array(String)); do not persist source/target arrays anymore
            def escape_for_array(items):
                if not items:
                    return "[]"
                escaped_items = []
                for item in items:
                    escaped_item = item.replace("'", "''")
                    escaped_items.append(f"'{escaped_item}'")
                return "[" + ",".join(escaped_items) + "]"
            
            dependencies_json = escape_for_array(dependencies)
            
            insert_sql = f"""
            INSERT INTO {table_name} (
                transformation_stage,
                transformation_id,
                transformation_name,
                transformation_schema_name,
                dependencies,
                execution_frequency,
                execution_sequence,
                statement_type,
                sql_data,
                created_at,
                updated_at,
                version
            ) VALUES (
                '{transformation.get_stage().value}',
                {transformation.data.transformation_id},
                '{transformation.data.transformation_name}',
                '{transformation_schema_name}',
                {dependencies_json},
                '{execution_frequency}',
                {transformation.data.execution_sequence},
                '{transformation.data.statement_type}',
                '{escaped_json}',
                now(),
                now(),
                1
            )
            """
            
            self.db_manager.execute_command(insert_sql)
            return True
            
        except Exception as e:
            print(f"Error storing transformation: {e}")
            return False
    
    def get_transformation(self, transformation_id: int, stage: TransformationStage) -> Optional[SQLTransformation]:
        """Retrieve a transformation by ID and stage"""
        try:
            table_name = self.get_transformation_table(stage)
            
            query = f"""
            SELECT sql_data
            FROM {table_name}
            WHERE transformation_id = {transformation_id}
            ORDER BY version DESC
            LIMIT 1
            """
            
            result = self.db_manager.execute_query_dict(query)
            if result:
                json_data = json.loads(result[0]['sql_data'])
                return SQLTransformation.from_json(json_data)
            return None
            
        except Exception as e:
            print(f"Error retrieving transformation: {e}")
            return None
    
    def get_transformations_for_stage(self, stage: TransformationStage) -> List[SQLTransformation]:
        """Get all transformations for a specific stage"""
        try:
            table_name = self.get_transformation_table(stage)
            
            query = f"""
            SELECT sql_data
            FROM {table_name}
            ORDER BY transformation_id, execution_sequence
            """
            
            results = self.db_manager.execute_query_dict(query)
            transformations = []
            
            for row in results:
                json_data = json.loads(row['sql_data'])
                transformations.append(SQLTransformation.from_json(json_data))
            
            return transformations
            
        except Exception as e:
            print(f"Error retrieving transformations for stage {stage}: {e}")
            return []
