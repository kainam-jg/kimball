import json
import re
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

class TransformationStage(Enum):
    STAGE1 = "stage1"
    STAGE2 = "stage2" 
    STAGE3 = "stage3"
    STAGE4 = "stage4"

@dataclass
class SQLTransformationData:
    """Data structure for SQL transformation with metadata"""
    raw_sql: str
    stage: TransformationStage
    transformation_id: int
    transformation_name: str
    source_tables: List[str]
    target_tables: List[str]
    statement_type: str
    execution_sequence: int
    metadata: Dict[str, Any]
    validation: Dict[str, Any]

class SQLTransformation:
    """Reusable class for handling SQL transformations across all stages"""
    
    def __init__(self, data: SQLTransformationData):
        self.data = data
        self._validated = False
    
    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> 'SQLTransformation':
        """Create from JSON data stored in database"""
        return cls(SQLTransformationData(
            raw_sql=json_data['raw_sql'],
            stage=TransformationStage(json_data['stage']),
            transformation_id=json_data['transformation_id'],
            transformation_name=json_data['transformation_name'],
            source_tables=json_data.get('source_tables', []),
            target_tables=json_data.get('target_tables', []),
            statement_type=json_data['statement_type'],
            execution_sequence=json_data['execution_sequence'],
            metadata=json_data.get('metadata', {}),
            validation=json_data.get('validation', {})
        ))
    
    def to_json(self) -> Dict[str, Any]:
        """Convert to JSON for database storage"""
        return {
            'raw_sql': self.data.raw_sql,
            'stage': self.data.stage.value,
            'transformation_id': self.data.transformation_id,
            'transformation_name': self.data.transformation_name,
            'source_tables': self.data.source_tables,
            'target_tables': self.data.target_tables,
            'statement_type': self.data.statement_type,
            'execution_sequence': self.data.execution_sequence,
            'metadata': self.data.metadata,
            'validation': self.data.validation
        }
    
    def to_sql(self) -> str:
        """Get the raw SQL for execution - no conversion needed"""
        return self.data.raw_sql
    
    def validate_sql_syntax(self) -> bool:
        """Basic SQL syntax validation"""
        # Add SQL syntax validation logic here
        # For now, just check if it's not empty
        return bool(self.data.raw_sql.strip())
    
    def get_validation_queries(self) -> Dict[str, str]:
        """Get validation queries for record count checking"""
        return self.data.validation
    
    def get_source_tables(self) -> List[str]:
        """Get source table names"""
        return self.data.source_tables
    
    def get_target_tables(self) -> List[str]:
        """Get target table names"""
        return self.data.target_tables
    
    def get_stage(self) -> TransformationStage:
        """Get transformation stage"""
        return self.data.stage
    
    def get_statement_type(self) -> str:
        """Get SQL statement type (INSERT, CREATE, etc.)"""
        return self.data.statement_type
    
    def add_metadata(self, key: str, value: Any):
        """Add custom metadata"""
        self.data.metadata[key] = value
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get custom metadata"""
        return self.data.metadata.get(key, default)
