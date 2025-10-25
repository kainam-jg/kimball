"""
KIMBALL Data Models

This module provides Pydantic models for the KIMBALL platform:
- Request/response models for API endpoints
- Data validation schemas
- Common data structures
"""

from pydantic import BaseModel, Field, validator
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from enum import Enum

class PhaseType(str, Enum):
    """KIMBALL phase types."""
    ACQUIRE = "acquire"
    DISCOVER = "discover"
    MODEL = "model"
    BUILD = "build"

class SourceType(str, Enum):
    """Data source types."""
    DATABASE = "database"
    API = "api"
    STORAGE = "storage"

class DatabaseType(str, Enum):
    """Database types."""
    CLICKHOUSE = "clickhouse"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"

class APIType(str, Enum):
    """API types."""
    REST = "rest"
    GRAPHQL = "graphql"
    SOAP = "soap"

class StorageType(str, Enum):
    """Storage types."""
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"
    LOCAL = "local"

class QualityLevel(str, Enum):
    """Data quality levels."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class RelationshipType(str, Enum):
    """Relationship types."""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_MANY = "many_to_many"

class BaseResponse(BaseModel):
    """Base response model."""
    status: str = Field(..., description="Response status")
    message: str = Field(..., description="Response message")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")

class ErrorResponse(BaseResponse):
    """Error response model."""
    status: str = "error"
    error_code: Optional[str] = Field(None, description="Error code")
    details: Optional[Dict[str, Any]] = Field(None, description="Error details")

# Acquire Phase Models
class ConnectionConfig(BaseModel):
    """Connection configuration model."""
    source_type: SourceType = Field(..., description="Type of data source")
    connection_config: Dict[str, Any] = Field(..., description="Connection parameters")
    test_connection: bool = Field(True, description="Whether to test connection")

class ExtractionConfig(BaseModel):
    """Data extraction configuration model."""
    source_id: str = Field(..., description="Source ID")
    extraction_config: Dict[str, Any] = Field(..., description="Extraction parameters")
    batch_size: int = Field(1000, description="Batch size for extraction")

class LoadConfig(BaseModel):
    """Data loading configuration model."""
    extraction_id: str = Field(..., description="Extraction ID")
    target_table: str = Field(..., description="Target table name")
    load_config: Dict[str, Any] = Field(default_factory=dict, description="Loading parameters")

# Discover Phase Models
class DiscoverConfig(BaseModel):
    """Discover phase configuration model."""
    schema_name: str = Field("bronze", description="Schema name to analyze")
    include_quality: bool = Field(True, description="Include quality analysis")
    include_relationships: bool = Field(True, description="Include relationship analysis")
    include_hierarchies: bool = Field(True, description="Include hierarchy analysis")

class ColumnMetadata(BaseModel):
    """Column metadata model."""
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column type")
    cardinality: int = Field(..., description="Number of unique values")
    null_count: int = Field(..., description="Number of null values")
    null_percentage: float = Field(..., description="Percentage of null values")
    classification: str = Field(..., description="Fact or dimension classification")
    sample_values: List[Any] = Field(default_factory=list, description="Sample values")
    is_primary_key_candidate: bool = Field(False, description="Is primary key candidate")
    data_quality_score: float = Field(..., description="Data quality score")
    cardinality_ratio: float = Field(..., description="Cardinality ratio")

class TableMetadata(BaseModel):
    """Table metadata model."""
    table_name: str = Field(..., description="Table name")
    row_count: int = Field(..., description="Number of rows")
    column_count: int = Field(..., description="Number of columns")
    columns: List[ColumnMetadata] = Field(..., description="Column metadata")
    fact_columns: List[str] = Field(default_factory=list, description="Fact column names")
    dimension_columns: List[str] = Field(default_factory=list, description="Dimension column names")
    analysis_timestamp: datetime = Field(default_factory=datetime.now, description="Analysis timestamp")

class CatalogMetadata(BaseModel):
    """Catalog metadata model."""
    schema_name: str = Field(..., description="Schema name")
    analysis_timestamp: datetime = Field(..., description="Analysis timestamp")
    total_tables: int = Field(..., description="Total number of tables")
    total_columns: int = Field(..., description="Total number of columns")
    total_fact_columns: int = Field(..., description="Total fact columns")
    total_dimension_columns: int = Field(..., description="Total dimension columns")
    fact_dimension_ratio: float = Field(..., description="Fact to dimension ratio")

class QualityIssue(BaseModel):
    """Data quality issue model."""
    type: str = Field(..., description="Issue type")
    severity: str = Field(..., description="Issue severity")
    message: str = Field(..., description="Issue message")
    table: str = Field(..., description="Table name")
    column: str = Field(..., description="Column name")

class QualityReport(BaseModel):
    """Data quality report model."""
    overall_score: float = Field(..., description="Overall quality score")
    high_quality_tables: int = Field(..., description="Number of high quality tables")
    medium_quality_tables: int = Field(..., description="Number of medium quality tables")
    low_quality_tables: int = Field(..., description="Number of low quality tables")
    issues: List[QualityIssue] = Field(default_factory=list, description="Quality issues")
    recommendations: List[Dict[str, Any]] = Field(default_factory=list, description="Recommendations")

class Relationship(BaseModel):
    """Relationship model."""
    table1: str = Field(..., description="First table name")
    column1: str = Field(..., description="First column name")
    table2: str = Field(..., description="Second table name")
    column2: str = Field(..., description="Second column name")
    type1: str = Field(..., description="First column type")
    type2: str = Field(..., description="Second column type")
    cardinality1: int = Field(..., description="First column cardinality")
    cardinality2: int = Field(..., description="Second column cardinality")
    is_pk1: bool = Field(False, description="First column is primary key candidate")
    is_pk2: bool = Field(False, description="Second column is primary key candidate")
    confidence: float = Field(..., description="Relationship confidence score")
    relationship_type: RelationshipType = Field(..., description="Type of relationship")

class RelationshipReport(BaseModel):
    """Relationship discovery report model."""
    total_relationships: int = Field(..., description="Total number of relationships")
    high_confidence_joins: int = Field(..., description="Number of high confidence joins")
    primary_key_candidates: int = Field(..., description="Number of primary key candidates")
    relationships: List[Relationship] = Field(default_factory=list, description="Discovered relationships")

# Model Phase Models
class ERDConfig(BaseModel):
    """ERD generation configuration model."""
    catalog_id: str = Field(..., description="Catalog ID")
    include_relationships: bool = Field(True, description="Include relationships")
    include_attributes: bool = Field(True, description="Include attributes")

class HierarchyConfig(BaseModel):
    """Hierarchy modeling configuration model."""
    catalog_id: str = Field(..., description="Catalog ID")
    hierarchy_config: Dict[str, Any] = Field(default_factory=dict, description="Hierarchy configuration")

class StarSchemaConfig(BaseModel):
    """Star schema design configuration model."""
    catalog_id: str = Field(..., description="Catalog ID")
    fact_tables: List[str] = Field(..., description="Fact table names")
    dimension_tables: List[str] = Field(..., description="Dimension table names")
    relationships: List[Dict[str, Any]] = Field(default_factory=list, description="Table relationships")

# Build Phase Models
class DAGConfig(BaseModel):
    """DAG generation configuration model."""
    transformation_id: str = Field(..., description="Transformation ID")
    schedule: str = Field("daily", description="DAG schedule")
    dependencies: List[str] = Field(default_factory=list, description="DAG dependencies")
    config: Dict[str, Any] = Field(default_factory=dict, description="DAG configuration")

class SQLConfig(BaseModel):
    """SQL generation configuration model."""
    transformation_id: str = Field(..., description="Transformation ID")
    target_layer: str = Field(..., description="Target layer (silver/gold)")
    optimization_level: str = Field("standard", description="Optimization level")

class PipelineConfig(BaseModel):
    """Pipeline configuration model."""
    dag_id: str = Field(..., description="DAG ID")
    environment: str = Field("development", description="Target environment")
    config: Dict[str, Any] = Field(default_factory=dict, description="Pipeline configuration")

# Common Models
class TaskStatus(BaseModel):
    """Task status model."""
    task_id: str = Field(..., description="Task ID")
    status: str = Field(..., description="Task status")
    progress: float = Field(0.0, description="Task progress (0-100)")
    message: str = Field("", description="Status message")
    started_at: Optional[datetime] = Field(None, description="Start time")
    completed_at: Optional[datetime] = Field(None, description="Completion time")
    error: Optional[str] = Field(None, description="Error message if failed")

class PerformanceMetrics(BaseModel):
    """Performance metrics model."""
    execution_time: float = Field(..., description="Execution time in seconds")
    memory_usage: float = Field(..., description="Memory usage in MB")
    cpu_usage: float = Field(..., description="CPU usage percentage")
    records_processed: int = Field(..., description="Number of records processed")
    throughput: float = Field(..., description="Records per second")

class AuditLog(BaseModel):
    """Audit log model."""
    timestamp: datetime = Field(default_factory=datetime.now, description="Log timestamp")
    user_id: Optional[str] = Field(None, description="User ID")
    action: str = Field(..., description="Action performed")
    resource: str = Field(..., description="Resource affected")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional details")
    ip_address: Optional[str] = Field(None, description="IP address")
    user_agent: Optional[str] = Field(None, description="User agent")

# Validation methods
class BaseModelWithValidation(BaseModel):
    """Base model with common validation methods."""
    
    @validator('*', pre=True)
    def validate_strings(cls, v):
        """Validate and clean string values."""
        if isinstance(v, str):
            return v.strip()
        return v
    
    @validator('*', pre=True)
    def validate_numbers(cls, v):
        """Validate numeric values."""
        if isinstance(v, (int, float)):
            if v < 0:
                raise ValueError("Negative values not allowed")
        return v

class ConfigModel(BaseModelWithValidation):
    """Configuration model with validation."""
    name: str = Field(..., description="Configuration name")
    value: Union[str, int, float, bool, Dict, List] = Field(..., description="Configuration value")
    description: Optional[str] = Field(None, description="Configuration description")
    required: bool = Field(False, description="Whether configuration is required")
    default_value: Optional[Union[str, int, float, bool, Dict, List]] = Field(None, description="Default value")
