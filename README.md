# KIMBALL Platform v2.0

**Kinetic Intelligent Model Builder with Augmented Learning and Loading**

A comprehensive full-stack data warehouse automation platform that transforms raw data into production-ready data pipelines through four intelligent phases.

## 🎯 Overview

KIMBALL is a complete data warehouse automation platform that takes you from raw data sources to production data pipelines. It provides intelligent discovery, modeling, and automation capabilities for modern data architectures.

## 🏗️ Architecture

KIMBALL follows a four-phase approach:

### 1. **Acquire Phase** ✅ **COMPLETE**
- **Multi-source data connectors** (PostgreSQL ✅, S3 ✅, APIs 🔄, cloud storage 🔄)
- **Automated data ingestion** into ClickHouse bronze layer with optimized batch processing
- **Universal chunking framework** for handling datasets of any size (KB to TB)
- **Stream-based processing** for memory-efficient file handling (CSV, Excel, Parquet)
- **Connection testing** and validation for all source types
- **Data source CRUD operations** via API
- **Parallel processing** with intelligent worker scaling
- **Unicode handling** and character encoding cleanup
- **Performance optimization** with 10K record batches (10x improvement)

### 2. **Discover Phase** ✅ **COMPLETE**
- **Intelligent Type Inference**: Advanced pattern recognition for date and numeric detection
- **Multi-Pattern Date Detection**: YYYYMMDD, YYYY-MM-DD, MM/DD/YYYY, DD-MM-YYYY, Unix timestamps, ISO datetime
- **Statistical Numeric Detection**: Decimal, integer, currency, percentage, scientific notation detection
- **Confidence Scoring**: Multi-factor confidence calculation with optimized thresholds (90%+ accuracy)
- **Online Learning System**: Learns from user corrections to improve accuracy over time
- **Performance Optimization**: Intelligent caching and smart sampling for efficiency
- **Metadata Storage**: ClickHouse `metadata.discover` table with upsert functionality
- **Editable Names**: Both table and column names can be customized while preserving originals
- **Bulk Table Updates**: When table name changes, ALL columns in that table are updated automatically
- **Automated metadata discovery** and catalog generation
- **Data quality assessment** with intelligent scoring
- **Relationship discovery** and join candidate identification
- **Primary key detection** and foreign key mapping

### 3. **Transform Phase** ✅ **COMPLETE**
- **Multi-Statement Transformations**: Advanced transformation system with DROP, CREATE, INSERT, OPTIMIZE sequences
- **Upsert Logic**: Update transformations with automatic deduplication using ReplacingMergeTree
- **TransformEngine**: Sequential execution engine for multi-statement transformations
- **Stage1 Transformations**: Complete bronze to silver transformations with data type conversion
- **Advanced SQL Processing**: Currency parsing, date conversion, and table optimization
- **Parallel Execution**: Execute multiple transformations concurrently for better performance
- **Schema Management**: transformation_schema_name for better organization
- **Metadata-Driven**: Uses Discover phase metadata for intelligent transformations
- **Rollback Capabilities**: Transaction-safe transformations with rollback support
- **Monitoring & Logging**: Comprehensive transformation monitoring and audit trails

### 4. **Model Phase** ✅ **COMPLETE**
- **ERD Analysis**: Entity Relationship Diagram generation and analysis
- **Hierarchy Discovery**: Dimensional hierarchy analysis with ROOT-to-LEAF progression
- **Join Relationship Detection**: Automatic discovery of table relationships and foreign keys
- **Fact vs Dimension Classification**: Intelligent classification of tables and columns
- **Primary Key Detection**: Automated identification of primary key candidates
- **Cross-Hierarchy Relationships**: Discovery of relationships between different hierarchies
- **OLAP-Compliant Hierarchies**: Proper level-based hierarchy structures for dimensional modeling
- **Metadata Storage**: ClickHouse `metadata.erd` and `metadata.hierarchies` tables
- **Confidence Scoring**: Relationship confidence calculation for join recommendations
- **Dimensional Modeling Support**: Foundation for star schema and data warehouse design
- **✏️ Editing Capabilities**: Full CRUD operations for ERD relationships and hierarchies
- **🎨 Custom Creation**: User-defined relationship and hierarchy creation
- **🗑️ Deletion Management**: Soft deletion with version control and audit trails

#### **🔄 Model Phase Architecture**

The Model Phase provides comprehensive ERD and hierarchy analysis for dimensional modeling:

##### **ERD Analysis Components**
- **Table Classification**: Automatic fact vs dimension table identification
- **Column Analysis**: Data type inference, cardinality analysis, and quality scoring
- **Primary Key Detection**: Identification of primary key candidates based on uniqueness and data quality
- **Join Relationship Discovery**: Automatic detection of potential joins between tables
- **Confidence Scoring**: Multi-factor confidence calculation for relationship recommendations

##### **Hierarchy Analysis Components**
- **ROOT-to-LEAF Progression**: Proper OLAP-compliant hierarchy structures
- **Level-Based Organization**: Cardinality-based level assignment (lowest = ROOT, highest = LEAF)
- **Parent-Child Relationships**: Automatic discovery of hierarchical relationships
- **Sibling Detection**: Identification of columns at the same hierarchy level
- **Cross-Hierarchy Analysis**: Discovery of relationships between different table hierarchies

##### **Metadata Storage**
- **`metadata.erd`**: Entity relationship metadata with table classifications and join relationships
- **`metadata.hierarchies`**: Dimensional hierarchy metadata with level structures and relationships
- **Upsert Functionality**: Automatic metadata updates with version control
- **Query Optimization**: Indexed metadata tables for fast retrieval

##### **Editing and Customization**
- **ERD Relationship Editing**: Modify table types, primary keys, fact/dimension columns, and relationships
- **Hierarchy Editing**: Customize hierarchy names, root/leaf columns, and level structures
- **Custom Relationship Creation**: Create user-defined ERD relationships between any tables/columns
- **Custom Hierarchy Creation**: Build custom dimensional hierarchies with specific level definitions
- **Soft Deletion**: Remove relationships and hierarchies while preserving audit history
- **Bulk Operations**: Update multiple relationships or hierarchy levels in single operations

#### **🔄 ELT Transform Architecture**

The Transform Phase implements a sophisticated ELT (Extract, Load, Transform) architecture using ClickHouse UDFs for data processing:

##### **Multi-Stage Processing Pipeline**
- **Bronze Layer**: Raw data from various sources (S3, PostgreSQL, APIs)
- **Silver Layer**: Cleaned and typed data with business logic applied
- **Gold Layer**: Aggregated and optimized data for analytics

##### **Gold Layer Dimensional Model**
- **Dimension Tables**: Tables with `_dim` suffix (e.g., `dealers_dim`, `vehicles_dim`)
- **Fact Tables**: Tables with `_fact` suffix (e.g., `sales_fact`)
- **Star Schema**: Traditional dimensional modeling structure
- **Stage 3 UDFs**: Transform Silver Stage 2 data into Gold dimensional model
- **Metadata-Driven**: Uses Model Phase ERD and hierarchy analysis for transformation logic
- **Source/Target Tracking**: New columns in `metadata.transformation1` track source and target schemas/tables
- **Version Control**: Microsecond-precision versioning for upserts
- **Execution Frequency**: Configurable execution schedules

##### **Transformation Stages**

###### **Stage 1: Data Type Conversion (Bronze → Silver)**
- **Purpose**: Convert string data to proper data types based on intelligent inference
- **Process**: 
  1. Truncate existing silver tables
  2. Load data from bronze with type conversions
  3. Apply custom column and table names from metadata
- **Tables**: `*_stage1` suffix (e.g., `sales_transactions_stage1`)

###### **Stage 2: Change Data Capture (CDC) (Silver Stage1 → Silver Stage2)**
- **Purpose**: Implement CDC using delta lake concepts for current data maintenance
- **Process**:
  1. Create Stage 2 tables with `MergeTree` engine
  2. Drop and recreate tables to avoid deduplication issues
  3. Insert all records from Stage 1 to Stage 2
  4. Maintain clean data transfer
- **Tables**: `*_stage2` suffix (e.g., `sales_transactions_stage2`) - these are the "current" tables
- **CDC Logic**: Drop and recreate approach ensures clean data transfer

###### **Stage 3+: Business Logic and Aggregation (Silver → Gold)**
- **Purpose**: Apply business rules and create analytical datasets
- **Process**: TBD (future implementation)

##### **CDC (Change Data Capture) Architecture**

The Stage 2 CDC implementation follows delta lake principles:

```sql
-- Stage 2 Table Creation (MergeTree for clean data transfer)
DROP TABLE IF EXISTS silver.sales_transactions_stage2;

CREATE TABLE silver.sales_transactions_stage2 AS silver.sales_transactions_stage1
ENGINE = MergeTree()
ORDER BY create_date;

-- CDC Logic: Insert all data from Stage 1 to Stage 2
INSERT INTO silver.sales_transactions_stage2
SELECT *
FROM silver.sales_transactions_stage1;
```

**CDC Benefits**:
- **Current Data**: Stage 2 tables always contain the most current data
- **Clean Transfer**: Drop and recreate approach ensures no stale data
- **Performance**: Optimized for both inserts and queries
- **Scalability**: Handles large datasets efficiently
- **Reliability**: No deduplication issues with identical timestamps

##### **Metadata Schema**
```sql
CREATE TABLE metadata.transformation1 (
    transformation_stage String,
    udf_name String,
    udf_number Int32,
    udf_logic String,
    udf_schema_name String,
    dependencies Array(String),
    execution_frequency String,
    created_at DateTime,
    updated_at DateTime,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (transformation_stage, udf_name)
```

### 4. **Model Phase** 🏗️
- **Interactive ERD generation** and editing
- **Hierarchical relationship modeling** (OLAP-style)
- **Star schema design** for data warehouse optimization
- **Silver layer (3NF)** and **Gold layer (star schema)** modeling

## 🧠 Intelligent Type Inference System

KIMBALL features a sophisticated **Intelligent Type Inference System** that automatically detects data types from string values in the bronze layer using advanced pattern recognition and machine learning techniques.

### **Key Features:**

#### **Multi-Pattern Date Detection**
- **YYYYMMDD**: 8-digit dates (20210926) with 90% confidence
- **YYYY-MM-DD**: ISO date format (2025-10-26) with 95% confidence  
- **MM/DD/YYYY**: US date format (10/26/2025) with 90% confidence
- **DD-MM-YYYY**: European date format (26-10-2025) with 90% confidence
- **Unix Timestamp**: 10-digit timestamps (1732627200) with 80% confidence
- **ISO DateTime**: ISO datetime format (2025-10-26T10:30:00) with 95% confidence

#### **Statistical Numeric Detection**
- **Decimal Numbers**: 10336.48 with 84% confidence
- **Integers**: 12345 with pattern matching
- **Currency**: $1,234.56 with format detection
- **Percentages**: 85.5% with percentage pattern
- **Scientific Notation**: 1.23e+04 with scientific format
- **Negative Numbers**: -123.45 with sign detection

#### **Hybrid Detection Strategy**
- **Rule-Based Pattern Matching**: Fast detection for obvious patterns
- **Statistical Analysis**: Multi-factor analysis for numeric measures
- **Confidence Scoring**: Combines multiple signals for final classification
- **Online Learning**: Learns from user corrections to improve accuracy
- **Performance Optimization**: Intelligent caching and smart sampling

#### **Production Features**
- **API Endpoints**: `/test/intelligent-inference` and `/learn/correction`
- **Performance Monitoring**: Cache hit rates and request tracking
- **Error Handling**: Comprehensive error handling and graceful degradation
- **Documentation**: Extensive code documentation and usage examples

### **Usage Example:**
```python
from kimball.discover.intelligent_type_inference import IntelligentTypeInference

# Initialize the inference engine
engine = IntelligentTypeInference()

# Analyze column values
result = engine.infer_column_type(['20210926', '20210927', '20210928'], 'sales_date')

print(f"Type: {result.inferred_type}")        # Output: date
print(f"Confidence: {result.confidence}")     # Output: 0.9
print(f"Pattern: {result.pattern_matched}")   # Output: YYYYMMDD
print(f"Reasoning: {result.reasoning}")       # Output: Detected YYYYMMDD date pattern with 0.90 confidence
```

## 🔄 Metadata Management & Editing System

KIMBALL includes a comprehensive **Metadata Management & Editing System** that allows users to customize table and column names while preserving original values and maintaining data consistency.

### **Key Features:**

#### **Editable Names**
- **Original vs. New Names**: Both table names and column names have `original_*` and `new_*` fields
- **Preserve Originals**: Original names are never lost, allowing traceability back to source systems
- **Custom Display Names**: Users can set friendly display names for better business understanding

#### **Bulk Table Updates**
- **Consistency Enforcement**: When a table name changes, ALL columns in that table are automatically updated
- **Single Request Updates**: Update both table and column names in a single API call
- **Version Control**: Microsecond-precision versioning ensures proper deduplication

#### **Upsert Functionality**
- **ClickHouse ReplacingMergeTree**: Prevents duplicate metadata records
- **Automatic Deduplication**: Latest version is kept based on version numbers
- **Performance Optimized**: Efficient upsert operations for large metadata sets

### **API Endpoints:**
- `GET /api/v1/discover/metadata` - Retrieve metadata with optional table filtering
- `PUT /api/v1/discover/metadata/edit` - Edit metadata fields with bulk table name updates
- `POST /api/v1/discover/store/discover-metadata` - Store discovery analysis results

### **Usage Examples:**

#### **Bulk Table Name Update:**
```bash
PUT /api/v1/discover/metadata/edit
{
  "original_table_name": "daily_sales",
  "original_column_name": "amount_sales",
  "new_table_name": "sales_transactions"
}
# Result: ALL columns in daily_sales get new_table_name = "sales_transactions"
```

#### **Combined Table + Column Updates:**
```bash
PUT /api/v1/discover/metadata/edit
{
  "original_table_name": "vehicles",
  "original_column_name": "vehicle_class",
  "new_table_name": "product_catalog",
  "new_column_name": "product_category",
  "classification": "dimension"
}
# Result: ALL columns get new_table_name = "product_catalog"
#         AND vehicle_class gets new_column_name = "product_category"
```

## 🔄 ELT Transformation Architecture

KIMBALL implements a sophisticated **ELT (Extract, Load, Transform) architecture** using ClickHouse User-Defined Functions (UDFs) for data transformation orchestration.

### **Architecture Overview:**

#### **Multi-Stage Processing Pipeline:**
```
Bronze Layer (Raw Data) → Silver Layer (Cleaned/Transformed) → Gold Layer (Business-Ready)
     ↓                           ↓                              ↓
  Stage 1                    Stage 2                        Stage 3
(Data Type & Name)        (Change Data Capture)         (Business Logic)
```

#### **Key Components:**

1. **Metadata-Driven Transformations**
   - UDF logic stored in `metadata.transformation1` table
   - Transformation dependencies and execution order
   - Rollback capabilities and version control

2. **ClickHouse UDFs**
   - SQL-based transformation functions
   - Parameterized and reusable logic
   - Performance-optimized for large datasets

3. **Orchestration Engine**
   - Automated UDF execution scheduling
   - Dependency management and parallel execution
   - Monitoring and logging infrastructure

4. **Delta Lake Framework**
   - Standardized change data capture
   - Version control and time travel capabilities
   - ACID transactions and rollback support

### **Transformation Stages:**

#### **Stage 1: Data Type & Name Transformation**
- Convert string data types to proper types (date, numeric, etc.)
- Apply custom table and column names from metadata
- Create Silver layer tables with `stage1` suffix
- Data quality validation and cleansing

#### **Stage 2: Change Data Capture (CDC)**
- Implement delta processing for incremental updates
- Track changes and maintain data lineage
- Optimize for performance with incremental loads

#### **Stage 3: Business Logic & Aggregation**
- Apply business rules and calculations
- Create aggregated tables and materialized views
- Generate final Gold layer for analytics

### **Metadata Schema:**

```sql
-- Transformation metadata table
CREATE TABLE metadata.transformation1 (
    transformation_stage String,           -- stage1, stage2, stage3, etc.
    transformation_id UInt32,              -- Unique integer ID for each transformation
    transformation_name String,            -- Human-readable transformation name
    transformation_schema_name String,     -- Schema where transformation is stored
    dependencies Array(String),            -- Dependent transformations
    execution_frequency String,            -- How often to run (daily, hourly, etc.)
    source_schema String,                  -- Source schema name
    source_table String,                   -- Source table name
    target_schema String,                  -- Target schema name
    target_table String,                   -- Target table name
    execution_sequence UInt32,             -- Order of execution within transformation
    sql_statement String,                  -- The actual SQL statement
    statement_type String,                 -- Type of statement (DROP, CREATE, INSERT, OPTIMIZE)
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    version UInt64 DEFAULT 1
) ENGINE = ReplacingMergeTree(version)
ORDER BY (transformation_id, execution_sequence)
```

## 🔄 Stream-Based Data Processing Architecture

KIMBALL uses a **stream-based processing architecture** that ensures consistency, scalability, and extensibility across all data sources and file types.

### **Core Principles:**

1. **Universal Stream Processing**: All data sources (databases, files, APIs) are converted to byte streams
2. **String Standardization**: All data types are converted to strings for consistent handling
3. **Memory Efficiency**: No DataFrames or large objects loaded into memory
4. **Extensible Design**: New file types require only implementing stream-to-string conversion

### **Processing Flow:**

```
Data Source → Byte Stream → File Type Parser → String Records → ClickHouse Bronze Layer
```

### **File Type Support:**

| File Type | Parser | Stream Method | Status |
|-----------|--------|---------------|---------|
| CSV | `csv.DictReader` | `io.StringIO` | ✅ Implemented |
| Excel (.xlsx/.xls) | `openpyxl` | `iter_rows()` | ✅ Implemented |
| Parquet | `pyarrow.parquet` | `to_batches()` | ✅ Implemented |
| JSON | `json.loads()` | `io.StringIO` | 🔄 Planned |
| XML | `xml.etree` | Stream parsing | 🔄 Planned |

### **Benefits:**

- **Consistency**: All file types processed identically after stream conversion
- **Scalability**: Memory-efficient processing of large files
- **Maintainability**: Single code path for data loading and validation
- **Extensibility**: Easy to add new file types by implementing stream parsers
- **Reliability**: Robust error handling and logging throughout the pipeline

### **Implementation Pattern:**

```python
async def _parse_[file_type]_data(file_content: bytes) -> List[Dict[str, Any]]:
    """Parse [file_type] data from bytes using stream-based approach."""
    try:
        # 1. Create stream from bytes
        stream = io.BytesIO(file_content)
        
        # 2. Use appropriate parser for file type
        parser = [file_type]_parser(stream)
        
        # 3. Process row-by-row (stream-based)
        data = []
        for row in parser:
            # 4. Convert all values to strings
            string_row = {key: str(value) if value is not None else "" 
                         for key, value in row.items()}
            data.append(string_row)
        
        return data
    except Exception as e:
        logger.error(f"Error parsing {file_type} data: {e}")
        raise
```

This architecture ensures that **all future data source implementations** follow the same pattern, making the system robust and easy to extend.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- ClickHouse database
- Git

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/kainam-jg/kimball.git
cd kimball
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure your environment**
```bash
cp config.json.example config.json
# Edit config.json with your ClickHouse connection details
```

4. **Start the FastAPI backend**
```bash
# Windows
start_server.bat

# Or manually
uvicorn kimball.api.main:app --host 0.0.0.0 --port 8000 --reload
```

5. **Test the API endpoints** using curl commands or Postman

## 📁 Project Structure

```
kimball/
├── kimball/                    # Main application package
│   ├── acquire/               # Acquire phase modules
│   │   ├── connectors.py     # Data source connectors
│   │   ├── extractors.py     # Data extraction logic
│   │   ├── transformers.py   # Data transformation
│   │   └── loaders.py         # Bronze layer loading
│   ├── discover/              # Discover phase modules
│   │   ├── metadata_analyzer.py    # Enhanced metadata analysis
│   │   ├── catalog_builder.py      # Catalog generation
│   │   ├── quality_assessor.py     # Data quality assessment
│   │   └── relationship_finder.py  # Relationship discovery
│   ├── model/                 # Model phase modules
│   │   ├── erd_generator.py       # ERD generation
│   │   ├── hierarchy_modeler.py    # Hierarchy modeling
│   │   ├── star_schema_designer.py # Star schema design
│   │   └── schema_transformer.py   # Schema transformation
│   ├── build/                 # Build phase modules
│   │   ├── dag_builder.py         # DAG generation
│   │   ├── sql_generator.py       # SQL generation
│   │   ├── pipeline_orchestrator.py # Pipeline orchestration
│   │   └── monitor.py             # Pipeline monitoring
│   ├── api/                   # FastAPI backend
│   │   ├── main.py               # Main FastAPI app
│   │   ├── acquire_routes.py     # Acquire API routes
│   │   ├── discover_routes.py    # Discover API routes
│   │   ├── model_routes.py       # Model API routes
│   │   └── build_routes.py       # Build API routes
│   └── core/                  # Core infrastructure
│       ├── database.py           # Database management
│       ├── config.py             # Configuration management
│       ├── logger.py             # Logging system
│       └── utils.py              # Common utilities
├── tests/                     # Test suite
├── docs/                      # Documentation
├── config.json               # Configuration file
├── requirements.txt           # Python dependencies
└── README.md                 # This file
```

## 🔧 Configuration

### Database Configuration
Edit `config.json` to configure your ClickHouse connection:

```json
{
    "clickhouse": {
        "host": "your-host",
        "port": 8123,
        "user": "your-username",
        "password": "your-password",
        "database": "kimball"
    }
}
```

### API Configuration
The FastAPI backend runs on `http://localhost:8000` by default. You can configure this in the `config.json` file.

## 📊 Usage Examples

### 1. Discover Phase
```python
from kimball.discover.metadata_analyzer import MetadataAnalyzer

# Initialize analyzer
analyzer = MetadataAnalyzer()

# Connect and analyze
if analyzer.connect():
    catalog = analyzer.build_catalog("bronze")
    analyzer.save_catalog(catalog, "my_catalog.json")
    analyzer.disconnect()
```

### 2. API Usage
```python
import requests

# Start schema analysis
response = requests.post(
    "http://localhost:8000/api/v1/discover/analyze",
    json={
        "schema_name": "bronze",
        "include_quality": True,
        "include_relationships": True
    }
)

catalog_id = response.json()["catalog_id"]
```

## 🎯 Key Features

### Data Acquisition ✅
- **Multi-source connectors** for PostgreSQL, S3, and APIs
- **Connection testing** and validation
- **Data source configuration** via REST API
- **Edit functionality** for updating data source configurations
- **Simplified S3 setup** (access key + secret key only)

### Intelligent Discovery
- **Automated metadata analysis** with 95%+ accuracy
- **Data quality scoring** with actionable recommendations
- **Relationship discovery** with confidence scoring
- **Primary key detection** and foreign key mapping

### Interactive Modeling
- **Visual ERD editing** with drag-and-drop interface
- **Hierarchical relationship modeling** following OLAP standards
- **Star schema design** with automated fact/dimension classification
- **Schema validation** and optimization suggestions

### Production-Ready Builds
- **Automated DAG generation** for Apache Airflow
- **SQL transformation** code with best practices
- **Pipeline orchestration** with monitoring and alerting
- **Scalable architecture** for enterprise deployments

## 🔍 API Documentation

Once the FastAPI backend is running, visit:
- **Interactive API docs**: http://localhost:8000/docs
- **ReDoc documentation**: http://localhost:8000/redoc

### Key Endpoints

#### Acquire Phase ✅
- `GET /api/v1/acquire/datasources` - List data sources
- `POST /api/v1/acquire/datasources` - Create data source
- `PUT /api/v1/acquire/datasources/{source_id}` - Update data source
- `DELETE /api/v1/acquire/datasources/{source_id}` - Delete data source
- `GET /api/v1/acquire/test/{source_id}` - Test connection
- `POST /api/v1/acquire/connect/{source_id}` - Connect to source
- `POST /api/v1/acquire/extract/{source_id}` - Extract data
- `POST /api/v1/acquire/load/{source_id}` - Load to bronze layer

#### Discover Phase
- `POST /api/v1/discover/analyze` - Analyze schema
- `GET /api/v1/discover/catalog/{catalog_id}` - Get catalog
- `POST /api/v1/discover/quality` - Assess data quality
- `POST /api/v1/discover/relationships` - Find relationships


#### Transform Phase
- `GET /api/v1/transform/status` - Get transform status
- `GET /api/v1/transform/transformations` - List all transformations
- `POST /api/v1/transform/transformations` - Create multi-statement transformation
- `PUT /api/v1/transform/transformations/{name}` - Update transformation with upsert logic
- `GET /api/v1/transform/transformations/{name}` - Get specific transformation
- `POST /api/v1/transform/transformations/{name}/execute` - Execute single transformation
- `POST /api/v1/transform/transformations/execute/parallel` - Execute multiple transformations in parallel
- `DELETE /api/v1/transform/transformations/{name}` - Delete transformation

#### Model Phase
- `GET /api/v1/model/status` - Get model status
- `POST /api/v1/model/erd/analyze` - Analyze ERD relationships
- `POST /api/v1/model/hierarchies/analyze` - Analyze hierarchies
- `GET /api/v1/model/erd/metadata` - Get ERD metadata
- `GET /api/v1/model/hierarchies/metadata` - Get hierarchy metadata
- `PUT /api/v1/model/erd/edit` - Edit ERD relationships
- `PUT /api/v1/model/hierarchies/edit` - Edit hierarchies
- `POST /api/v1/model/hierarchies/create` - Create custom hierarchy
- `POST /api/v1/model/erd/create` - Create custom ERD relationship


## 🧪 Testing

### Run Tests
```bash
pytest tests/
```

### Test Coverage
```bash
pytest --cov=kimball tests/
```

## 📈 Performance

- **Schema Analysis**: ~2 minutes for 14 tables with 237 columns
- **Relationship Discovery**: ~30 seconds for complex schemas
- **Quality Assessment**: ~1 minute for comprehensive analysis
- **API Response Time**: <100ms for most operations

## 🔒 Security

- **API Authentication**: JWT-based authentication (coming soon)
- **Data Encryption**: TLS encryption for all connections
- **Access Control**: Role-based access control (RBAC)
- **Audit Logging**: Comprehensive audit trails

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/kainam-jg/kimball/issues)
- **Discussions**: [GitHub Discussions](https://github.com/kainam-jg/kimball/discussions)

## 🚀 Roadmap

### Phase 1 (Current) ✅
- ✅ **Acquire phase** with multi-source connectors (PostgreSQL, S3)
- ✅ **Data source configuration** via API
- ✅ **Connection testing** and validation
- ✅ **Simplified S3 configuration** (no session tokens)
- ✅ **FastAPI backend** with comprehensive APIs
- ✅ **Discover phase** with enhanced metadata analysis
- ✅ **Data quality assessment**

### Phase 2 (Next)
- 🔄 Model phase with ERD generation
- 🔄 Hierarchy modeling and validation
- 🔄 Star schema design
- 🔄 Interactive model editing

### Phase 3 (Future)
- 🔄 Build phase with DAG generation
- 🔄 SQL transformation code generation
- 🔄 Pipeline orchestration
- 🔄 Production deployment

---

**KIMBALL v2.0** - Transforming data into intelligence, one pipeline at a time.