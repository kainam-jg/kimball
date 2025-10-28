# KIMBALL API Testing Guide

This document contains comprehensive CURL commands for testing all KIMBALL API endpoints, organized by phase.

## 📋 **Table of Contents**

1. [Acquire Phase](#acquire-phase)
2. [Discover Phase](#discover-phase)
3. [Transform Phase](#transform-phase)
4. [Model Phase](#model-phase-active)
5. [General API Endpoints](#general-api-endpoints)
6. [Testing Workflows](#testing-workflows)

---

## 🔄 **Acquire Phase**

### **Data Source Management**

#### **Get Acquire Status**
```bash
curl -X GET "http://localhost:8000/api/v1/acquire/status"
```

#### **List Data Sources**
```bash
curl -X GET "http://localhost:8000/api/v1/acquire/datasources"
```

#### **Create Data Source**
```bash
# PostgreSQL Data Source
curl -X POST "http://localhost:8000/api/v1/acquire/datasources" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "PostgreSQL Database",
    "source_type": "postgresql",
    "config": {
      "host": "YOUR_POSTGRES_HOST",
      "port": 5432,
      "user": "YOUR_POSTGRES_USER",
      "password": "YOUR_POSTGRES_PASSWORD",
      "database": "postgres",
      "schema": "vehicles"
    }
  }'

# AWS S3 Data Source
curl -X POST "http://localhost:8000/api/v1/acquire/datasources" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "AWS S3 Bucket",
    "source_type": "s3",
    "config": {
      "aws_access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
      "aws_secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY",
      "aws_region": "us-east-1",
      "s3_bucket_name": "your-bucket-name",
      "s3_prefix": "your-prefix/"
    }
  }'
```

#### **Update Data Source**
```bash
curl -X PUT "http://localhost:8000/api/v1/acquire/datasources/AWS%20S3%20Bucket" \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "aws_access_key_id": "NEW_ACCESS_KEY",
      "aws_secret_access_key": "NEW_SECRET_KEY",
      "aws_region": "us-west-2",
      "s3_bucket_name": "new-bucket-name",
      "s3_prefix": "new-prefix/"
    }
  }'
```

#### **Delete Data Source**
```bash
curl -X DELETE "http://localhost:8000/api/v1/acquire/datasources/AWS%20S3%20Bucket"
```

#### **Test Data Source Connection**
```bash
curl -X POST "http://localhost:8000/api/v1/acquire/datasources/test" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "AWS S3 Bucket"
  }'
```

### **Data Discovery**

#### **Explore S3 Objects**
```bash
curl -X POST "http://localhost:8000/api/v1/acquire/explore/s3" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "AWS S3 Bucket",
    "prefix": "vehicle_sales_data/",
    "search_subdirectories": true
  }'
```

#### **Explore Database Tables**
```bash
curl -X POST "http://localhost:8000/api/v1/acquire/explore/database" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "PostgreSQL Database",
    "schema_name": "vehicles"
  }'
```

#### **Execute Custom SQL Query**
```bash
curl -X POST "http://localhost:8000/api/v1/acquire/explore/sql" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "PostgreSQL Database",
    "query": "SELECT * FROM vehicles.daily_sales LIMIT 10"
  }'
```

### **Data Extraction**

#### **Extract Storage Data to Bronze** ✅ **WORKING**
```bash
# Single file extraction (creates table a priori with header columns)
curl -X POST "http://localhost:8000/api/v1/acquire/extract/storage/AWS%20S3%20Bucket" \
  -H "Content-Type: application/json" \
  -d '{
    "extraction_config": {
      "dealer_sales.csv": ["vehicle_sales_data/dealer_sales.csv"]
    },
    "target_table": "dealer_sales"
  }'

# Multiple files extraction
curl -X POST "http://localhost:8000/api/v1/acquire/extract/storage/AWS%20S3%20Bucket" \
  -H "Content-Type: application/json" \
  -d '{
    "extraction_config": {
      "dealer_regions.csv": ["vehicle_sales_data/dealer_regions.csv"],
      "vehicles.csv": ["vehicle_sales_data/vehicles.csv"],
      "vehicles_description.xlsx": ["vehicle_sales_data/vehicles_description.xlsx"]
    }
  }'
```

#### **Extract Database Data to Bronze** ✅ **WORKING**
```bash
# Extract entire table (with 200K chunking + 1000 batch loading)
curl -X POST "http://localhost:8000/api/v1/acquire/extract/database/Vehicle%20Sales%20Data" \
  -H "Content-Type: application/json" \
  -d '{
    "extraction_config": {
      "daily_sales": ["vehicles.daily_sales"]
    },
    "target_table": "daily_sales"
  }'

# Extract with custom SQL (creates table a priori with query columns)
curl -X POST "http://localhost:8000/api/v1/acquire/extract/sql/Vehicle%20Sales%20Data" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM vehicles.daily_sales WHERE amount_sales > 1000",
    "target_table": "high_value_sales"
  }'
```

**Key Features:**
- **Storage Extraction**: Creates ClickHouse table a priori using file header columns
- **Database Extraction**: Uses efficient 200K chunking from PostgreSQL + 1000 batch loading to ClickHouse
- **SQL Extraction**: Creates table a priori using query result columns
- **Enhanced Logging**: Detailed progress tracking for chunks and batches
- **String Streams**: All data converted to strings for consistent processing

---

## 🔍 **Discover Phase** ✅ **WORKING**

### **Discovery Analysis**

#### **Get Discovery Status**
```bash
curl -X GET "http://localhost:8000/api/v1/discover/status"
```

**Expected Result**: Returns discovery phase status and metadata table information

#### **Analyze Bronze Schema** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "bronze",
    "include_sample_data": true,
    "sample_size": 10
  }'
```

**Expected Result**: 
- Analyzes all tables in bronze schema
- Creates metadata.discover table with proper upsert functionality
- Returns analysis results with inferred types and classifications
- Example: `amount_sales` classified as `fact` (numeric), `sales_date` as `date`
- **Upsert**: Running multiple times won't create duplicates

#### **Get Discovery Metadata** ✅ **WORKING**
```bash
# Get all metadata
curl -X GET "http://localhost:8000/api/v1/discover/metadata"

# Get metadata with limit
curl -X GET "http://localhost:8000/api/v1/discover/metadata?limit=5"

# Get metadata for specific table
curl -X GET "http://localhost:8000/api/v1/discover/metadata?table_name=daily_sales"
```

**Expected Result**: Returns detailed metadata including:
- Original and new table/column names
- Inferred data types (date, numeric, string)
- Classifications (fact vs dimension)
- Cardinality and data quality scores
- Sample values

#### **Debug Table Analysis** ✅ **WORKING**
```bash
curl -X GET "http://localhost:8000/api/v1/discover/debug/daily_sales"
```

**Expected Result**: Step-by-step analysis of table structure and data

#### **Analyze Single Table** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/analyze/table" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "daily_sales",
    "include_sample_data": true,
    "sample_size": 10
  }'
```

**Expected Result**: Detailed analysis of a single table

#### **Test Intelligent Type Inference** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/test/intelligent-inference" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "daily_sales",
    "column_name": "amount_sales",
    "sample_size": 100
  }'
```

**Expected Result**: Tests the intelligent type inference system on a specific column

#### **Edit Discovery Metadata** ✅ **WORKING**
```bash
curl -X PUT "http://localhost:8000/api/v1/discover/metadata/edit" \
  -H "Content-Type: application/json" \
  -d '{
    "original_table_name": "daily_sales",
    "original_column_name": "amount_sales",
    "new_column_name": "sales_amount",
    "inferred_type": "numeric",
    "classification": "fact"
  }'
```

**Expected Result**: Updates metadata for the specified column

#### **Learn from Corrections** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/learn/correction" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "daily_sales",
    "column_name": "sales_date",
    "predicted_type": "string",
    "actual_type": "date",
    "confidence": 0.3
  }'
```

**Expected Result**: Improves future predictions based on user corrections

#### **Analyze Entire Schema**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/analyze/schema" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "bronze"
  }'
```

#### **Export Discovery Metadata**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/export" \
  -H "Content-Type: application/json" \
  -d '{
    "table_names": ["daily_sales", "dealer_regions", "vehicles"],
    "output_format": "json"
  }'
```

#### **Store Discovery Metadata**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/store" \
  -H "Content-Type: application/json" \
  -d '{
    "table_names": ["daily_sales", "dealer_regions", "vehicles"]
  }'
```

### **Metadata Management**

#### **Get Discovery Metadata**
```bash
# Get all metadata
curl -X GET "http://localhost:8000/api/v1/discover/metadata"

# Get metadata for specific table
curl -X GET "http://localhost:8000/api/v1/discover/metadata?table_name=daily_sales"

# Get metadata with limit
curl -X GET "http://localhost:8000/api/v1/discover/metadata?limit=50"
```

#### **Edit Discovery Metadata**
```bash
# Update column name and classification
curl -X PUT "http://localhost:8000/api/v1/discover/metadata/edit" \
  -H "Content-Type: application/json" \
  -d '{
    "original_table_name": "daily_sales",
    "original_column_name": "amount_sales",
    "new_column_name": "sales_amount",
    "inferred_type": "numeric",
    "classification": "fact"
  }'

# Update table name (bulk update for all columns)
curl -X PUT "http://localhost:8000/api/v1/discover/metadata/edit" \
  -H "Content-Type: application/json" \
  -d '{
    "original_table_name": "daily_sales",
    "original_column_name": "amount_sales",
    "new_table_name": "sales_transactions",
    "new_column_name": "sales_amount",
    "classification": "fact"
  }'
```

### **Intelligent Type Inference Testing**

#### **Test Type Inference**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/test/intelligent-inference" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "daily_sales",
    "column_name": "sales_date",
    "sample_values": ["20251026", "20251027", "20251028"]
  }'
```

#### **Learn from Corrections**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/learn/correction" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "daily_sales",
    "column_name": "sales_date",
    "predicted_type": "string",
    "actual_type": "date",
    "confidence": 0.3
  }'
```

---

## 🔄 **Transform Phase** ✅ **ACTIVE**

### **Multi-Statement Transformations** ✅ **WORKING**

#### **Get Transformation Status**
```bash
curl -X GET "http://localhost:8000/api/v1/transform/status"
```

#### **List All Transformations**
```bash
curl -X GET "http://localhost:8000/api/v1/transform/transformations"
```

#### **Get Specific Transformation**
```bash
curl -X GET "http://localhost:8000/api/v1/transform/transformations/daily_sales_stage1"
```

### **Create Multi-Statement Transformations**

#### **Create dealer_regions_stage1 Transformation** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/transform/transformations" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage1",
    "transformation_id": "dealer_regions_stage1",
    "statements": [
      {
        "transformation_id": "dealer_regions_stage1",
        "execution_sequence": 1,
        "sql_statement": "DROP TABLE IF EXISTS silver.dealer_regions_stage1;",
        "statement_type": "DROP",
        "description": "Drop existing dealer_regions_stage1 table"
      },
      {
        "transformation_id": "dealer_regions_stage1",
        "execution_sequence": 2,
        "sql_statement": "CREATE TABLE silver.dealer_regions_stage1 (region String, district String, country_name String, city_name String, dealer_name String, create_date Date) ENGINE = MergeTree() ORDER BY tuple();",
        "statement_type": "CREATE",
        "description": "Create dealer_regions_stage1 table with proper schema"
      },
      {
        "transformation_id": "dealer_regions_stage1",
        "execution_sequence": 3,
        "sql_statement": "INSERT INTO silver.dealer_regions_stage1 SELECT trim(region) AS region, trim(district) AS district, trim(country_name) AS country_name, trim(city_name) AS city_name, trim(dealer_name) AS dealer_name, toDate(parseDateTimeBestEffortOrNull(create_date)) AS create_date FROM bronze.dealer_regions;",
        "statement_type": "INSERT",
        "description": "Insert transformed data from bronze to silver"
      }
    ],
    "source_schema": "bronze",
    "source_table": "dealer_regions",
    "target_schema": "silver",
    "target_table": "dealer_regions_stage1",
    "execution_frequency": "daily"
  }'
```

#### **Create vehicles_stage1 Transformation** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/transform/transformations" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage1",
    "transformation_id": "vehicles_stage1",
    "statements": [
      {
        "transformation_id": "vehicles_stage1",
        "execution_sequence": 1,
        "sql_statement": "DROP TABLE IF EXISTS silver.vehicles_stage1;",
        "statement_type": "DROP",
        "description": "Drop existing vehicles_stage1 table"
      },
      {
        "transformation_id": "vehicles_stage1",
        "execution_sequence": 2,
        "sql_statement": "CREATE TABLE silver.vehicles_stage1 (vehicle_class String, vehicle_type String, vehicle_model String, create_date Date) ENGINE = MergeTree() ORDER BY tuple();",
        "statement_type": "CREATE",
        "description": "Create vehicles_stage1 table with proper schema"
      },
      {
        "transformation_id": "vehicles_stage1",
        "execution_sequence": 3,
        "sql_statement": "INSERT INTO silver.vehicles_stage1 SELECT trim(vehicle_class) AS vehicle_class, trim(vehicle_type) AS vehicle_type, trim(vehicle_model) AS vehicle_model, toDate(parseDateTimeBestEffortOrNull(create_date)) AS create_date FROM bronze.vehicles;",
        "statement_type": "INSERT",
        "description": "Insert transformed data from bronze to silver"
      }
    ],
    "source_schema": "bronze",
    "source_table": "vehicles",
    "target_schema": "silver",
    "target_table": "vehicles_stage1",
    "execution_frequency": "daily"
  }'
```

#### **Create daily_sales_stage1 Transformation** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/transform/transformations" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage1",
    "transformation_id": "daily_sales_stage1",
    "statements": [
      {
        "transformation_id": "daily_sales_stage1",
        "execution_sequence": 1,
        "sql_statement": "DROP TABLE IF EXISTS silver.daily_sales_stage1;",
        "statement_type": "DROP",
        "description": "Drop existing daily_sales_stage1 table"
      },
      {
        "transformation_id": "daily_sales_stage1",
        "execution_sequence": 2,
        "sql_statement": "CREATE TABLE silver.daily_sales_stage1 (sales_amount Decimal(15, 2), dealer_name String, sales_date Date, vehicle_model String, create_date Date) ENGINE = MergeTree() PARTITION BY toStartOfYear(sales_date) ORDER BY (sales_date, dealer_name);",
        "statement_type": "CREATE",
        "description": "Create daily_sales_stage1 table with proper schema and partitioning"
      },
      {
        "transformation_id": "daily_sales_stage1",
        "execution_sequence": 3,
        "sql_statement": "INSERT INTO silver.daily_sales_stage1 WITH replaceRegexpAll(amount_sales, \"[,$]\", \"\") AS am0, if(match(am0, \"^\\\\(.*\\\\)$\"), concat(\"-\", replaceRegexpAll(am0, \"[()]\", \"\")), am0) AS am_norm SELECT toDecimal64OrNull(am_norm, 2) AS sales_amount, trim(dealer_name) AS dealer_name, toDate(parseDateTimeBestEffortOrNull(sales_date)) AS sales_date, trim(vehicle_model) AS vehicle_model, toDate(parseDateTimeBestEffortOrNull(create_date)) AS create_date FROM bronze.daily_sales;",
        "statement_type": "INSERT",
        "description": "Insert transformed data with currency parsing and date conversion"
      }
    ],
    "source_schema": "bronze",
    "source_table": "daily_sales",
    "target_schema": "silver",
    "target_table": "daily_sales_stage1",
    "execution_frequency": "daily"
  }'
```

### **Update Transformations with Upsert Logic** ✅ **WORKING**

#### **Update daily_sales_stage1 with OPTIMIZE Statement**
```bash
curl -X PUT "http://localhost:8000/api/v1/transform/transformations/daily_sales_stage1" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage1",
    "transformation_id": "daily_sales_stage1",
    "statements": [
      {
        "transformation_id": "daily_sales_stage1",
        "execution_sequence": 1,
        "sql_statement": "DROP TABLE IF EXISTS silver.daily_sales_stage1;",
        "statement_type": "DROP",
        "description": "Drop existing daily_sales_stage1 table"
      },
      {
        "transformation_id": "daily_sales_stage1",
        "execution_sequence": 2,
        "sql_statement": "CREATE TABLE silver.daily_sales_stage1 (sales_amount Decimal(15, 2), dealer_name String, sales_date Date, vehicle_model String, create_date Date) ENGINE = MergeTree() PARTITION BY toStartOfYear(sales_date) ORDER BY (sales_date, dealer_name);",
        "statement_type": "CREATE",
        "description": "Create daily_sales_stage1 table with proper schema and partitioning"
      },
      {
        "transformation_id": "daily_sales_stage1",
        "execution_sequence": 3,
        "sql_statement": "INSERT INTO silver.daily_sales_stage1 WITH replaceRegexpAll(amount_sales, \"[,$]\", \"\") AS am0, if(match(am0, \"^\\\\(.*\\\\)$\"), concat(\"-\", replaceRegexpAll(am0, \"[()]\", \"\")), am0) AS am_norm SELECT toDecimal64OrNull(am_norm, 2) AS sales_amount, trim(dealer_name) AS dealer_name, toDate(parseDateTimeBestEffortOrNull(sales_date)) AS sales_date, trim(vehicle_model) AS vehicle_model, toDate(parseDateTimeBestEffortOrNull(create_date)) AS create_date FROM bronze.daily_sales;",
        "statement_type": "INSERT",
        "description": "Insert transformed data with currency parsing and date conversion"
      },
      {
        "transformation_id": "daily_sales_stage1",
        "execution_sequence": 4,
        "sql_statement": "OPTIMIZE TABLE silver.daily_sales_stage1 FINAL;",
        "statement_type": "OPTIMIZE",
        "description": "Optimize table for better performance"
      }
    ],
    "source_schema": "bronze",
    "source_table": "daily_sales",
    "target_schema": "silver",
    "target_table": "daily_sales_stage1",
    "execution_frequency": "daily"
  }'
```

### **Execute Transformations**

#### **Execute Single Transformation**
```bash
curl -X POST "http://localhost:8000/api/v1/transform/transformations/daily_sales_stage1/execute"
```

#### **Execute Multiple Transformations in Parallel**
```bash
curl -X POST "http://localhost:8000/api/v1/transform/transformations/execute/parallel" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_names": ["dealer_regions_stage1", "vehicles_stage1", "daily_sales_stage1"]
  }'
```

#### **Delete Transformation**
```bash
curl -X DELETE "http://localhost:8000/api/v1/transform/transformations/daily_sales_stage1"
```

---

## 🎯 **Demo Phase** ✅ **ACTIVE**

### **Database Management Utilities**

#### **Get Demo Status**
```bash
curl -X GET "http://localhost:8000/api/v1/demo/status"
```

#### **List All Schemas**
```bash
curl -X GET "http://localhost:8000/api/v1/demo/schemas"
```

#### **List Tables in Schema**
```bash
curl -X GET "http://localhost:8000/api/v1/demo/list-tables/bronze"
curl -X GET "http://localhost:8000/api/v1/demo/list-tables/silver"
curl -X GET "http://localhost:8000/api/v1/demo/list-tables/metadata"
```

### **Table Operations**

#### **Drop All Tables in Schemas** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/demo/drop-tables" \
  -H "Content-Type: application/json" \
  -d '{
    "schemas": ["bronze", "silver"],
    "operation": "drop"
  }'
```

#### **Truncate All Tables in Schemas** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/demo/truncate-tables" \
  -H "Content-Type: application/json" \
  -d '{
    "schemas": ["metadata"],
    "operation": "truncate"
  }'
```

#### **Complete Cleanup (Equivalent to Original Script)** ✅ **WORKING**
```bash
curl -X POST "http://localhost:8000/api/v1/demo/cleanup-all"
```

**This endpoint performs the exact same operations as your original script:**
- Drops all tables in `bronze` schema
- Drops all tables in `silver` schema  
- Truncates all tables in `metadata` schema

### **Demo API Features**

- **✅ Direct ClickHouse Connection**: Uses `clickhouse_connect` library independently
- **✅ No Interference**: Does not affect existing DatabaseManager or connection classes
- **✅ Comprehensive Operations**: Drop, truncate, list tables and schemas
- **✅ Error Handling**: Detailed error messages and logging
- **✅ Batch Operations**: Process multiple schemas in single request
- **✅ Complete Cleanup**: One-click equivalent to your original script

---

## 🏗️ **Model Phase** ✅ **ACTIVE**

### **ERD Analysis**

#### **Get Model Status**
```bash
curl -X GET "http://localhost:8000/api/v1/model/status"
```

#### **Perform ERD Analysis**
```bash
curl -X POST "http://localhost:8000/api/v1/model/erd/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "silver",
    "include_relationships": true,
    "min_confidence": 0.5
  }'
```

#### **Get ERD Metadata**
```bash
# Get all ERD metadata
curl -X GET "http://localhost:8000/api/v1/model/erd/metadata"

# Get ERD metadata for specific table
curl -X GET "http://localhost:8000/api/v1/model/erd/metadata?table_name=daily_sales_stage1"
```

#### **Get ERD Relationships**
```bash
curl -X GET "http://localhost:8000/api/v1/model/erd/relationships?min_confidence=0.7&limit=50"
```

### **Hierarchy Analysis**

#### **Perform Hierarchy Analysis**
```bash
curl -X POST "http://localhost:8000/api/v1/model/hierarchies/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "silver",
    "include_cross_hierarchies": true,
    "min_confidence": 0.5
  }'
```

#### **Get Hierarchy Metadata**
```bash
# Get all hierarchy metadata
curl -X GET "http://localhost:8000/api/v1/model/hierarchies/metadata"

# Get hierarchy metadata for specific table
curl -X GET "http://localhost:8000/api/v1/model/hierarchies/metadata?table_name=dealer_regions_stage1"
```

#### **Get Hierarchy Levels**
```bash
# Get all hierarchy levels
curl -X GET "http://localhost:8000/api/v1/model/hierarchies/levels"

# Get hierarchy levels for specific table
curl -X GET "http://localhost:8000/api/v1/model/hierarchies/levels?table_name=dealer_regions_stage1"
```

### **Calendar Dimension Generation**

#### **Generate Calendar Dimension**
```bash
# Generate calendar for a specific date range
curl -X POST "http://localhost:8000/api/v1/model/calendar/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  }'

# Generate calendar for multiple years
curl -X POST "http://localhost:8000/api/v1/model/calendar/generate" \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2020-01-01",
    "end_date": "2030-12-31"
  }'
```

#### **Check Calendar Status**
```bash
# Check if calendar dimension exists and get statistics
curl -X GET "http://localhost:8000/api/v1/model/calendar/status"
```

### **Comprehensive Analysis**

#### **Perform Complete Model Analysis**
```bash
curl -X POST "http://localhost:8000/api/v1/model/analyze/all"
```

### **Expected Response Examples**

#### **ERD Analysis Response**
```json
{
  "status": "success",
  "message": "ERD analysis completed",
  "analysis_timestamp": "2025-10-27T09:06:06.397611",
  "total_tables": 4,
  "total_relationships": 10,
  "summary": {
    "fact_tables": 1,
    "dimension_tables": 3,
    "high_confidence_relationships": 0,
    "primary_key_candidates": 0
  },
  "stored": true
}
```

#### **Hierarchy Analysis Response**
```json
{
  "status": "success",
  "message": "Hierarchy analysis completed",
  "analysis_timestamp": "2025-10-27T09:06:22.673278",
  "total_tables": 4,
  "total_hierarchies": 2,
  "total_cross_relationships": 1,
  "summary": {
    "tables_with_hierarchies": 2,
    "max_hierarchy_depth": 6,
    "high_confidence_cross_relationships": 1,
    "root_nodes": 2,
    "leaf_nodes": 2
  },
  "stored": true
}
```

#### **Calendar Generation Response**
```json
{
  "status": "success",
  "message": "Calendar dimension generated successfully",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "total_records": 366,
  "table_name": "silver.calendar_stage1",
  "statistics": {
    "total_records": 366,
    "date_range": {
      "min_date": "2024-01-01",
      "max_date": "2024-12-31"
    }
  }
}
```

#### **Calendar Status Response**
```json
{
  "status": "exists",
  "message": "Calendar dimension table exists",
  "table_name": "silver.calendar_stage1",
  "statistics": {
    "total_records": 366,
    "date_range": {
      "min_date": "2024-01-01",
      "max_date": "2024-12-31"
    },
    "years_covered": 1
  }
}
```

### **Model Phase Testing Tips**

1. **Start with Status**: Always check `/api/v1/model/status` first to ensure the phase is active
2. **Run Analysis**: Use `/api/v1/model/analyze/all` for comprehensive analysis
3. **Check Metadata**: Verify data is stored in `metadata.erd` and `metadata.hierarchies` tables
4. **Validate Relationships**: Review join relationships and hierarchy structures
5. **Test Confidence**: Adjust `min_confidence` parameters to filter results
6. **Table-Specific Analysis**: Use `table_name` parameter for focused analysis
7. **Edit Relationships**: Use PUT endpoints to modify discovered relationships and hierarchies
8. **Create Custom**: Use POST endpoints to create custom relationships and hierarchies
9. **Delete Unwanted**: Use DELETE endpoints to remove unwanted relationships and hierarchies

### **ERD and Hierarchy Editing**

#### **Edit ERD Relationship**
```bash
curl -X PUT "http://localhost:8000/api/v1/model/erd/edit" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "daily_sales",
    "table_type": "fact",
    "primary_key_candidates": ["transaction_id", "date"],
    "fact_columns": ["amount_sales", "quantity"],
    "dimension_columns": ["dealer_id", "region_id"],
    "relationships": [
      {
        "table1": "daily_sales",
        "column1": "dealer_id",
        "table2": "dealers",
        "column2": "dealer_id",
        "relationship_type": "foreign_key",
        "confidence": 0.95
      }
    ]
  }'
```

#### **Edit Hierarchy**
```bash
curl -X PUT "http://localhost:8000/api/v1/model/hierarchies/edit" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "dealers",
    "hierarchy_name": "geographic_hierarchy",
    "root_column": "region",
    "leaf_column": "dealer_name",
    "intermediate_levels": [
      {
        "level": 1,
        "column_name": "region",
        "cardinality": 5
      },
      {
        "level": 2,
        "column_name": "city",
        "cardinality": 25
      },
      {
        "level": 3,
        "column_name": "dealer_name",
        "cardinality": 100
      }
    ]
  }'
```

### **Custom Creation**

#### **Create Custom Hierarchy**
```bash
curl -X POST "http://localhost:8000/api/v1/model/hierarchies/create" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "vehicles",
    "hierarchy_name": "vehicle_classification",
    "description": "Custom vehicle classification hierarchy",
    "levels": [
      {
        "level": 1,
        "column_name": "vehicle_type",
        "cardinality": 3,
        "description": "Main vehicle type"
      },
      {
        "level": 2,
        "column_name": "make",
        "cardinality": 15,
        "description": "Vehicle manufacturer"
      },
      {
        "level": 3,
        "column_name": "model",
        "cardinality": 50,
        "description": "Specific model"
      }
    ]
  }'
```

#### **Create Custom ERD Relationship**
```bash
curl -X POST "http://localhost:8000/api/v1/model/erd/create" \
  -H "Content-Type: application/json" \
  -d '{
    "table1": "daily_sales",
    "column1": "vehicle_id",
    "table2": "vehicles",
    "column2": "vehicle_id",
    "relationship_type": "foreign_key",
    "confidence": 0.98,
    "description": "Sales to vehicle relationship"
  }'
```

### **Deletion**

#### **Delete ERD Relationships**
```bash
# Delete all relationships for a table
curl -X DELETE "http://localhost:8000/api/v1/model/erd/relationships/daily_sales"

# Delete specific relationship (if relationship_id is provided)
curl -X DELETE "http://localhost:8000/api/v1/model/erd/relationships/daily_sales?relationship_id=rel_123"
```

#### **Delete Hierarchy**
```bash
# Delete hierarchy for a table
curl -X DELETE "http://localhost:8000/api/v1/model/hierarchies/dealers"

# Delete specific hierarchy (if hierarchy_name is provided)
curl -X DELETE "http://localhost:8000/api/v1/model/hierarchies/dealers?hierarchy_name=geographic_hierarchy"
```

### **Model Phase Notes**

- **ERD Analysis**: Discovers table relationships, primary keys, and fact vs dimension classification
- **Hierarchy Analysis**: Creates OLAP-compliant hierarchies with ROOT-to-LEAF progression
- **Metadata Storage**: All analysis results are stored in ClickHouse metadata tables
- **Confidence Scoring**: Relationships are scored for reliability and recommendation
- **Cross-Hierarchy**: Discovers relationships between different table hierarchies
- **Dimensional Modeling**: Provides foundation for star schema and data warehouse design
- **Editing Capabilities**: Full CRUD operations for ERD relationships and hierarchies
- **Custom Creation**: Ability to create user-defined relationships and hierarchies
- **Version Control**: All changes are versioned with timestamps for audit trails
- **Soft Deletion**: Deletion operations preserve history through versioning

---


## 🌐 **General API Endpoints**

### **Root Endpoint**
```bash
curl -X GET "http://localhost:8000/"
```

### **Health Check**
```bash
curl -X GET "http://localhost:8000/health"
```

### **API Documentation**
```bash
# Swagger UI
curl -X GET "http://localhost:8000/docs"

# ReDoc
curl -X GET "http://localhost:8000/redoc"
```

---

## 🧪 **Testing Workflows**

### **Complete Data Pipeline Test**

#### **1. Acquire Phase**
```bash
# Create data sources
curl -X POST "http://localhost:8000/api/v1/acquire/datasources" \
  -H "Content-Type: application/json" \
  -d '{"name": "AWS S3 Bucket", "source_type": "s3", "config": {...}}'

# Test connection
curl -X POST "http://localhost:8000/api/v1/acquire/datasources/test" \
  -H "Content-Type: application/json" \
  -d '{"name": "AWS S3 Bucket"}'

# Explore data
curl -X POST "http://localhost:8000/api/v1/acquire/explore/s3" \
  -H "Content-Type: application/json" \
  -d '{"source_name": "AWS S3 Bucket", "prefix": "vehicle_sales_data/"}'

# Extract data
curl -X POST "http://localhost:8000/api/v1/acquire/extract-data" \
  -H "Content-Type: application/json" \
  -d '{"source_name": "AWS S3 Bucket", "extraction_config": {...}}'
```

#### **2. Discover Phase**
```bash
# Analyze tables
curl -X POST "http://localhost:8000/api/v1/discover/analyze/table" \
  -H "Content-Type: application/json" \
  -d '{"table_name": "daily_sales"}'

# Store metadata
curl -X POST "http://localhost:8000/api/v1/discover/store" \
  -H "Content-Type: application/json" \
  -d '{"table_names": ["daily_sales", "dealer_regions"]}'

# Edit metadata
curl -X PUT "http://localhost:8000/api/v1/discover/metadata/edit" \
  -H "Content-Type: application/json" \
  -d '{"original_table_name": "daily_sales", "original_column_name": "amount_sales", "new_column_name": "sales_amount"}'
```

#### **3. Transform Phase**
```bash
# Create Stage1 transformations
curl -X POST "http://localhost:8000/api/v1/transform/transformations" \
  -H "Content-Type: application/json" \
  -d '{"transformation_stage": "stage1", "transformation_id": "dealer_regions_stage1", ...}'

# Execute transformations
curl -X POST "http://localhost:8000/api/v1/transform/transformations/daily_sales_stage1/execute"

# Check transformation status
curl -X GET "http://localhost:8000/api/v1/transform/status"

# List transformations
curl -X GET "http://localhost:8000/api/v1/transform/transformations"
```

### **Multi-Statement Transformation Testing Workflow**

#### **1. Create Stage1 Transformations**
```bash
# Create dealer_regions_stage1
curl -X POST "http://localhost:8000/api/v1/transform/transformations" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage1",
    "transformation_id": "dealer_regions_stage1",
    "statements": [
      {"execution_sequence": 1, "sql_statement": "DROP TABLE IF EXISTS silver.dealer_regions_stage1;", "statement_type": "DROP"},
      {"execution_sequence": 2, "sql_statement": "CREATE TABLE silver.dealer_regions_stage1 (...)", "statement_type": "CREATE"},
      {"execution_sequence": 3, "sql_statement": "INSERT INTO silver.dealer_regions_stage1 SELECT ...", "statement_type": "INSERT"}
    ],
    "source_schema": "bronze",
    "source_table": "dealer_regions",
    "target_schema": "silver",
    "target_table": "dealer_regions_stage1",
    "execution_frequency": "daily"
  }'
```

#### **2. Execute Transformations**
```bash
# Execute single transformation
curl -X POST "http://localhost:8000/api/v1/transform/transformations/daily_sales_stage1/execute"

# Execute multiple transformations in parallel
curl -X POST "http://localhost:8000/api/v1/transform/transformations/execute/parallel" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_names": ["dealer_regions_stage1", "vehicles_stage1", "daily_sales_stage1"]
  }'
```

#### **3. Update Transformations**
```bash
# Update transformation with additional statements (upsert logic)
curl -X PUT "http://localhost:8000/api/v1/transform/transformations/daily_sales_stage1" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage1",
    "transformation_id": "daily_sales_stage1",
    "statements": [
      {"execution_sequence": 1, "sql_statement": "DROP TABLE IF EXISTS silver.daily_sales_stage1;", "statement_type": "DROP"},
      {"execution_sequence": 2, "sql_statement": "CREATE TABLE silver.daily_sales_stage1 (...)", "statement_type": "CREATE"},
      {"execution_sequence": 3, "sql_statement": "INSERT INTO silver.daily_sales_stage1 SELECT ...", "statement_type": "INSERT"},
      {"execution_sequence": 4, "sql_statement": "OPTIMIZE TABLE silver.daily_sales_stage1 FINAL;", "statement_type": "OPTIMIZE"}
    ],
    "execution_frequency": "daily"
  }'
```

### **Error Testing**

#### **Test Invalid Data Source**
```bash
curl -X POST "http://localhost:8000/api/v1/acquire/datasources/test" \
  -H "Content-Type: application/json" \
  -d '{"name": "NonExistentSource"}'
```

#### **Test Invalid Transformation**
```bash
curl -X POST "http://localhost:8000/api/v1/transform/transformations/non_existent_transformation/execute"
```

---

## 📊 **Expected Response Examples**

### **Success Response**
```json
{
  "status": "success",
  "message": "Operation completed successfully",
  "data": {...}
}
```

### **Error Response**
```json
{
  "detail": "Error message describing what went wrong"
}
```

### **Status Response**
```json
{
  "status": "active",
  "phase": "Transform",
  "description": "ELT transformation orchestration with ClickHouse UDFs",
  "total_udfs": 4,
  "total_schemas": 1
}
```

---

## 🔧 **Testing Tips**

1. **Start with Status Endpoints**: Always check the status of each phase before testing
2. **Use Dry Runs**: Test UDFs with `dry_run: true` before executing
3. **Check Logs**: Monitor server logs for detailed error information
4. **Validate Responses**: Always check the response status and data
5. **Test Error Cases**: Include invalid inputs to test error handling
6. **Use Filters**: Leverage query parameters to filter and limit results
7. **Test Workflows**: Follow the complete pipeline workflow for end-to-end testing

---

## 🔧 **Recent Improvements (October 2025)**

### **Acquire Phase Enhancements**
- ✅ **Fixed Chunking Logic**: Restored efficient 200K chunking for database extractions
- ✅ **A Priori Table Creation**: All extraction types now create ClickHouse tables before loading data
- ✅ **Enhanced Logging**: Detailed progress tracking for chunks and batches
- ✅ **Code Cleanup**: Removed unused functions and optimized performance
- ✅ **String Streams**: Standardized data handling with string conversion
- ✅ **Batch Optimization**: 1000-record batches for optimal ClickHouse insertion

### **Discover Phase Enhancements**
- ✅ **Dictionary-Based Functions**: Created `execute_query_dict()` for Discover phase compatibility
- ✅ **Upsert Functionality**: Fixed metadata.discover table to prevent duplicates
- ✅ **ReplacingMergeTree**: Proper deduplication using composite key (table_name, column_name)
- ✅ **Type Inference**: Intelligent type detection (date, numeric, string) with confidence scoring
- ✅ **Fact/Dimension Classification**: Automatic classification of columns
- ✅ **Learning System**: Ability to learn from user corrections
- ✅ **No Regressions**: Acquire phase continues to work with tuple-based functions

### **Performance Improvements**
- **Database Extraction**: 200K chunks from PostgreSQL → 1000 batches to ClickHouse
- **Storage Extraction**: Header-based column detection and table creation
- **SQL Extraction**: Query result column detection and table creation
- **Memory Management**: Streaming processing prevents memory accumulation

### **Transform Phase Enhancements**
- ✅ **Multi-Statement Transformations**: New API for creating complex transformations with DROP, CREATE, INSERT sequences
- ✅ **Upsert Logic**: Update transformations with automatic deduplication using ReplacingMergeTree
- ✅ **TransformEngine**: Sequential execution engine for multi-statement transformations
- ✅ **Stage1 Transformations**: Complete dealer_regions, vehicles, and daily_sales transformations
- ✅ **Advanced SQL**: Currency parsing, date conversion, and table optimization
- ✅ **Parallel Execution**: Execute multiple transformations concurrently
- ✅ **Schema Management**: transformation_schema_name for better organization

### **Model Phase Enhancements**
- ✅ **Calendar Dimension Generator**: Generate comprehensive time dimensions for silver schema
- ✅ **Holiday Integration**: US holidays with working day calculations
- ✅ **Multi-Level Time Attributes**: Year, quarter, month, week, day hierarchies
- ✅ **Silver Schema Support**: calendar_stage1 table with _stage1 suffix convention
- ✅ **API Endpoints**: Calendar generation and status checking endpoints
- ✅ **SQL Escaping**: Proper handling of special characters in holiday names

### **Bug Fixes**
- Fixed `postgresql` vs `postgres` source type validation
- Fixed indentation errors in acquire_routes.py
- Fixed column name mismatches in ClickHouse table creation
- Fixed hanging issues during large dataset processing
- Fixed tuple vs dictionary access patterns in Discover phase
- Fixed metadata.discover table upsert functionality
- Fixed duplicate records in metadata storage
- Fixed transformation_id assignment for multi-statement transformations
- Fixed ReplacingMergeTree deduplication for transformation updates

---

## 📝 **Notes**

- Replace `localhost:8000` with your actual server URL if different
- URL encode special characters in data source names (e.g., `%20` for spaces)
- Some endpoints require specific data to be present in ClickHouse tables
- Check server logs for detailed error information if requests fail
- Use the Swagger UI at `/docs` for interactive API testing
- Replace placeholder credentials (YOUR_AWS_ACCESS_KEY_ID, YOUR_POSTGRES_HOST, etc.) with your actual values