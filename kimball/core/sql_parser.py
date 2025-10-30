import re
from typing import List, Dict, Set, Any
from .sql_transformation import SQLTransformationData, TransformationStage

class SQLParser:
    """Utility class for parsing SQL and extracting metadata"""
    
    @staticmethod
    def extract_source_tables(sql: str) -> List[str]:
        """Extract source table names from SQL"""
        # Pattern to match FROM clauses
        from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        matches = re.findall(from_pattern, sql, re.IGNORECASE)
        return list(set(matches))
    
    @staticmethod
    def extract_target_tables(sql: str) -> List[str]:
        """Extract target table names from SQL"""
        target_tables = []
        
        # INSERT INTO pattern
        insert_pattern = r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        insert_matches = re.findall(insert_pattern, sql, re.IGNORECASE)
        target_tables.extend(insert_matches)
        
        # CREATE TABLE pattern
        create_pattern = r'CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        create_matches = re.findall(create_pattern, sql, re.IGNORECASE)
        target_tables.extend(create_matches)
        
        # UPDATE pattern
        update_pattern = r'UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        update_matches = re.findall(update_pattern, sql, re.IGNORECASE)
        target_tables.extend(update_matches)
        
        # DROP TABLE pattern
        drop_pattern = r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        drop_matches = re.findall(drop_pattern, sql, re.IGNORECASE)
        target_tables.extend(drop_matches)
        
        # OPTIMIZE TABLE pattern
        optimize_pattern = r'OPTIMIZE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
        optimize_matches = re.findall(optimize_pattern, sql, re.IGNORECASE)
        target_tables.extend(optimize_matches)
        
        return list(set(target_tables))
    
    @staticmethod
    def detect_statement_type(sql: str) -> str:
        """Detect the type of SQL statement"""
        sql_upper = sql.strip().upper()
        
        if sql_upper.startswith('SELECT'):
            return 'SELECT'
        elif sql_upper.startswith('INSERT'):
            return 'INSERT'
        elif sql_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif sql_upper.startswith('DELETE'):
            return 'DELETE'
        elif sql_upper.startswith('CREATE'):
            return 'CREATE'
        elif sql_upper.startswith('DROP'):
            return 'DROP'
        elif sql_upper.startswith('ALTER'):
            return 'ALTER'
        elif sql_upper.startswith('OPTIMIZE'):
            return 'OPTIMIZE'
        else:
            return 'UNKNOWN'
    
    @staticmethod
    def generate_validation_queries(sql: str, stage: TransformationStage) -> Dict[str, str]:
        """Generate validation queries based on SQL and stage"""
        source_tables = SQLParser.extract_source_tables(sql)
        target_tables = SQLParser.extract_target_tables(sql)
        
        validation = {}
        
        # Generate source count queries
        for table in source_tables:
            validation[f'source_count_{table.replace(".", "_")}'] = f"SELECT COUNT(*) FROM {table}"
        
        # Generate target count queries
        for table in target_tables:
            validation[f'target_count_{table.replace(".", "_")}'] = f"SELECT COUNT(*) FROM {table}"
        
        return validation
    
    @staticmethod
    def create_transformation_data(
        sql: str,
        stage: TransformationStage,
        transformation_id: int,
        transformation_name: str,
        execution_sequence: int,
        custom_metadata: Dict[str, Any] = None
    ) -> SQLTransformationData:
        """Create transformation data from SQL"""
        
        source_tables = SQLParser.extract_source_tables(sql)
        target_tables = SQLParser.extract_target_tables(sql)
        statement_type = SQLParser.detect_statement_type(sql)
        validation = SQLParser.generate_validation_queries(sql, stage)
        
        return SQLTransformationData(
            raw_sql=sql,
            stage=stage,
            transformation_id=transformation_id,
            transformation_name=transformation_name,
            source_tables=source_tables,
            target_tables=target_tables,
            statement_type=statement_type,
            execution_sequence=execution_sequence,
            metadata=custom_metadata or {},
            validation=validation
        )
