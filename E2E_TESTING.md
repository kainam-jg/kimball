# KIMBALL End-to-End Testing Guide

This document outlines the comprehensive end-to-end testing process for the KIMBALL application, ensuring all four phases work correctly together.

## Prerequisites

- FastAPI server running on `http://localhost:8000`
- ClickHouse database accessible
- S3 data source configured with test data
- PostgreSQL data source configured with test data

## Test Steps

### Step 1: Cleanup Script (Pre-Test)
Run the cleanup script to prepare a clean environment:
```powershell
.\tests\cleanup_test_environment.ps1
```

This script will:
1. Drop all tables in bronze, silver, and gold schemas
2. Drop all UDFs defined in metadata.transformation1 (ONLY our UDFs)
3. Truncate all metadata tables (discover, transformation1, erd, hierarchies)

### Step 2: Data Extraction
Extract data from both storage and database sources.

#### 2.1: Extract Storage Data
```bash
curl -X POST "http://localhost:8000/api/v1/acquire/extract/storage" \
  -H "Content-Type: application/json" \
  -d '{
    "object_keys": [
      "vehicle_sales_data/dealer_regions.csv",
      "vehicle_sales_data/vehicles.csv", 
      "vehicle_sales_data/vehicles_description.xlsx"
    ]
  }'
```

**Expected Result**: 3 tables created in bronze schema with data

#### 2.2: Extract Database Data
```bash
curl -X POST "http://localhost:8000/api/v1/acquire/extract/database" \
  -H "Content-Type: application/json" \
  -d '{
    "table_names": [
      "vehicles.daily_sales"
    ]
  }'
```

**Expected Result**: 1 table created in bronze schema with data

### Step 3: Discovery Phase
Run discovery APIs to analyze data and create metadata.

#### 3.1: Analyze Schema
```bash
curl -X POST "http://localhost:8000/api/v1/discover/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "table_names": ["daily_sales", "dealer_regions", "vehicles", "vehicles_description"],
    "sample_size": 1000
  }'
```

**Expected Result**: metadata.discover table populated with analysis results

#### 3.2: Apply Custom Naming (Sample Changes)
```bash
# Update table names
curl -X PUT "http://localhost:8000/api/v1/discover/metadata/edit" \
  -H "Content-Type: application/json" \
  -d '{
    "original_table_name": "daily_sales",
    "original_column_name": "amount_sales", 
    "new_table_name": "sales_transactions",
    "new_column_name": "sales_amount"
  }'

curl -X PUT "http://localhost:8000/api/v1/discover/metadata/edit" \
  -H "Content-Type: application/json" \
  -d '{
    "original_table_name": "vehicles",
    "original_column_name": "vehicle_class",
    "new_table_name": "product_catalog", 
    "new_column_name": "product_category"
  }'
```

**Expected Result**: Custom table and column names applied

### Step 4: Stage 1 Transformations
Create and execute Stage 1 transformations.

#### 4.1: Create Stage 1 UDFs
```bash
curl -X POST "http://localhost:8000/api/v1/transform/udfs" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage1",
    "udf_name": "transform_sales_transactions_to_silver",
    "udf_number": 1,
    "udf_logic": "INSERT INTO silver.sales_transactions_stage1 SELECT toDateTime(create_date) as create_date, toFloat64(sales_amount) as sales_amount, dealer_name as dealer_name, toDate(sales_date) as sales_date, vehicle_model as vehicle_model FROM bronze.daily_sales WHERE create_date >= toDate(now()) - INTERVAL 1 DAY",
    "udf_schema_name": "default"
  }'
```

**Expected Result**: UDF created and stored in metadata.transformation1

#### 4.2: Execute Stage 1 Transformations
```bash
curl -X POST "http://localhost:8000/api/v1/transform/transformations/stage1"
```

**Expected Result**: Silver _stage1 tables created with proper data types

### Step 5: Stage 2 Transformations (CDC)
Create and execute Stage 2 CDC transformations.

#### 5.1: Create Stage 2 UDFs
```bash
curl -X POST "http://localhost:8000/api/v1/transform/udfs" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage2",
    "udf_name": "transform_sales_transactions_stage2_cdc",
    "udf_number": 1,
    "udf_logic": "DROP TABLE IF EXISTS silver.sales_transactions_stage2; CREATE TABLE silver.sales_transactions_stage2 AS silver.sales_transactions_stage1 ENGINE = MergeTree() ORDER BY create_date; INSERT INTO silver.sales_transactions_stage2 SELECT * FROM silver.sales_transactions_stage1;",
    "udf_schema_name": "default"
  }'
```

**Expected Result**: Stage 2 UDF created for CDC

#### 5.2: Execute Stage 2 Transformations
```bash
curl -X POST "http://localhost:8000/api/v1/transform/transformations/stage2"
```

**Expected Result**: Silver _stage2 tables created with current data

### Step 6: Model Phase
Run model APIs to generate ERD and hierarchy metadata.

#### 6.1: Analyze ERD Relationships
```bash
curl -X POST "http://localhost:8000/api/v1/model/erd/analyze"
```

**Expected Result**: metadata.erd table populated with relationship analysis

#### 6.2: Analyze Hierarchies
```bash
curl -X POST "http://localhost:8000/api/v1/model/hierarchies/analyze"
```

**Expected Result**: metadata.hierarchies table populated with hierarchy analysis

### Step 7: Verification
Verify all phases completed successfully.

#### 7.1: Check Bronze Layer
```bash
curl -X GET "http://localhost:8000/api/v1/acquire/status"
```

#### 7.2: Check Discovery Metadata
```bash
curl -X GET "http://localhost:8000/api/v1/discover/metadata"
```

#### 7.3: Check Transform Status
```bash
curl -X GET "http://localhost:8000/api/v1/transform/status"
```

#### 7.4: Check Model Metadata
```bash
curl -X GET "http://localhost:8000/api/v1/model/erd/metadata"
curl -X GET "http://localhost:8000/api/v1/model/hierarchies/metadata"
```

## Expected Final State

After completing all steps:

### Bronze Layer Tables
- `bronze.daily_sales` (from PostgreSQL)
- `bronze.dealer_regions` (from S3 CSV)
- `bronze.vehicles` (from S3 CSV) 
- `bronze.vehicles_description` (from S3 Excel)

### Silver Layer Tables
- `silver.sales_transactions_stage1` (renamed from daily_sales)
- `silver.dealer_regions_stage1`
- `silver.product_catalog_stage1` (renamed from vehicles)
- `silver.vehicles_description_stage1`
- `silver.sales_transactions_stage2` (CDC version)
- `silver.dealer_regions_stage2` (CDC version)
- `silver.product_catalog_stage2` (CDC version)
- `silver.vehicles_description_stage2` (CDC version)

### Metadata Tables
- `metadata.discover` - Column analysis and custom naming
- `metadata.transformation1` - UDF definitions for all stages
- `metadata.erd` - Entity relationship analysis
- `metadata.hierarchies` - Dimensional hierarchy analysis

## Troubleshooting

### Common Issues
1. **UDF Creation Fails**: Check if UDF already exists, use different name
2. **Data Extraction Fails**: Verify data source configurations
3. **Transform Execution Fails**: Check UDF logic syntax
4. **Model Analysis Fails**: Ensure Stage 2 tables exist with data

### Validation Commands
```bash
# Check table counts
curl -X GET "http://localhost:8000/api/v1/acquire/status"

# Check UDF status
curl -X GET "http://localhost:8000/api/v1/transform/udfs"

# Check metadata counts
curl -X GET "http://localhost:8000/api/v1/discover/metadata"
```

## Notes

- Each step should be validated before proceeding to the next
- Custom naming changes should be applied consistently
- UDF names should be unique to avoid conflicts
- Stage 2 tables serve as input for Model phase analysis
- All metadata tables should be populated with analysis results
