# KIMBALL API Testing Guide

This document contains comprehensive CURL commands for testing all KIMBALL API endpoints, organized by phase.

## 📋 **Table of Contents**

1. [Acquire Phase](#acquire-phase)
2. [Discover Phase](#discover-phase)
3. [Transformation Phase](#transformation-phase)
4. [Model Phase](#model-phase-coming-soon)
5. [Build Phase](#build-phase-coming-soon)
6. [General API Endpoints](#general-api-endpoints)
7. [Testing Workflows](#testing-workflows)

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

#### **Extract Storage Data to Bronze**
```bash
# Single file extraction
curl -X POST "http://localhost:8000/api/v1/acquire/extract-data" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "AWS S3 Bucket",
    "extraction_config": {
      "dealer_regions.csv": ["vehicle_sales_data/dealer_regions.csv"]
    },
    "target_table": "dealer_regions"
  }'

# Multiple files extraction
curl -X POST "http://localhost:8000/api/v1/acquire/extract-data" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "AWS S3 Bucket",
    "extraction_config": {
      "dealer_regions.csv": ["vehicle_sales_data/dealer_regions.csv"],
      "vehicles.csv": ["vehicle_sales_data/vehicles.csv"],
      "vehicles_description.xlsx": ["vehicle_sales_data/vehicles_description.xlsx"]
    }
  }'
```

#### **Extract Database Data to Bronze**
```bash
# Extract entire table
curl -X POST "http://localhost:8000/api/v1/acquire/extract-data" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "PostgreSQL Database",
    "extraction_config": {
      "daily_sales": ["vehicles.daily_sales"]
    },
    "target_table": "daily_sales"
  }'

# Extract with custom SQL
curl -X POST "http://localhost:8000/api/v1/acquire/extract-data" \
  -H "Content-Type: application/json" \
  -d '{
    "source_name": "PostgreSQL Database",
    "extraction_config": {
      "custom_sales": ["SELECT * FROM vehicles.daily_sales WHERE amount_sales > 1000"]
    },
    "target_table": "high_value_sales"
  }'
```

---

## 🔍 **Discover Phase**

### **Discovery Analysis**

#### **Get Discovery Status**
```bash
curl -X GET "http://localhost:8000/api/v1/discover/status"
```

#### **Test Discovery System**
```bash
curl -X GET "http://localhost:8000/api/v1/discover/test"
```

#### **Analyze Single Table**
```bash
curl -X POST "http://localhost:8000/api/v1/discover/analyze/table" \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "daily_sales"
  }'
```

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

## 🔄 **Transformation Phase**

### **Transformation Management**

#### **Get Transformation Status**
```bash
curl -X GET "http://localhost:8000/api/v1/transformation/status"
```

#### **List All UDFs**
```bash
curl -X GET "http://localhost:8000/api/v1/transformation/udfs"
```

#### **List UDFs with Filtering**
```bash
# Filter by stage
curl -X GET "http://localhost:8000/api/v1/transformation/udfs?stage=stage1"

# Filter by schema
curl -X GET "http://localhost:8000/api/v1/transformation/udfs?schema=default"

# Filter by both stage and schema
curl -X GET "http://localhost:8000/api/v1/transformation/udfs?stage=stage1&schema=default"

# Limit results
curl -X GET "http://localhost:8000/api/v1/transformation/udfs?limit=10"
```

#### **Get Specific UDF**
```bash
curl -X GET "http://localhost:8000/api/v1/transformation/udfs/transform_daily_sales_to_silver"
```

#### **Get All UDF Schemas**
```bash
curl -X GET "http://localhost:8000/api/v1/transformation/schemas"
```

### **UDF Operations**

#### **Create New UDF**
```bash
curl -X POST "http://localhost:8000/api/v1/transformation/udfs" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage1",
    "udf_name": "my_custom_udf",
    "udf_number": 1,
    "udf_logic": "INSERT INTO silver.my_table SELECT * FROM bronze.source_table",
    "udf_schema_name": "default",
    "dependencies": [],
    "execution_frequency": "daily"
  }'
```

#### **Update Existing UDF**
```bash
curl -X PUT "http://localhost:8000/api/v1/transformation/udfs/transform_daily_sales_to_silver" \
  -H "Content-Type: application/json" \
  -d '{
    "udf_logic": "INSERT INTO silver.sales_transactions_stage1 SELECT toDateTime(create_date) as create_date, toFloat64(amount_sales) as amount_sales, dealer_name as dealer_name, toDate(sales_date) as sales_date, vehicle_model as vehicle_model FROM bronze.daily_sales WHERE create_date >= toDate(now()) - INTERVAL 2 DAY",
    "execution_frequency": "hourly"
  }'
```

#### **Create UDF Function in ClickHouse**
```bash
curl -X POST "http://localhost:8000/api/v1/transformation/udfs/create" \
  -H "Content-Type: application/json" \
  -d '{
    "udf_name": "transform_daily_sales_to_silver",
    "transformation_stage": "stage1",
    "create_if_not_exists": true
  }'
```

#### **Execute UDF**
```bash
# Execute with dry run
curl -X POST "http://localhost:8000/api/v1/transformation/udfs/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "udf_name": "transform_daily_sales_to_silver",
    "dry_run": true
  }'

# Execute for real
curl -X POST "http://localhost:8000/api/v1/transformation/udfs/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "udf_name": "transform_daily_sales_to_silver",
    "dry_run": false,
    "create_if_not_exists": true
  }'
```

#### **Execute All Stage 1 Transformations**
```bash
curl -X POST "http://localhost:8000/api/v1/transformation/transformations/stage1"
```

---

## 🏗️ **Model Phase** (Coming Soon)

### **ERD Generation**
```bash
curl -X POST "http://localhost:8000/api/v1/model/erd" \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "silver",
    "tables": ["sales_transactions_stage1", "dealer_regions_stage1"]
  }'
```

### **Hierarchy Modeling**
```bash
curl -X POST "http://localhost:8000/api/v1/model/hierarchies" \
  -H "Content-Type: application/json" \
  -d '{
    "dimension_tables": ["dealer_regions_stage1", "product_catalog_stage1"],
    "fact_tables": ["sales_transactions_stage1"]
  }'
```

### **Star Schema Design**
```bash
curl -X POST "http://localhost:8000/api/v1/model/star-schema" \
  -H "Content-Type: application/json" \
  -d '{
    "fact_table": "sales_transactions_stage1",
    "dimension_tables": ["dealer_regions_stage1", "product_catalog_stage1"]
  }'
```

---

## 🚀 **Build Phase** (Coming Soon)

### **DAG Generation**
```bash
curl -X POST "http://localhost:8000/api/v1/build/dag" \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "sales_pipeline",
    "stages": ["acquire", "discover", "transform", "model"]
  }'
```

### **SQL Generation**
```bash
curl -X POST "http://localhost:8000/api/v1/build/sql" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_type": "bronze_to_silver",
    "source_tables": ["daily_sales", "dealer_regions"],
    "target_schema": "silver"
  }'
```

### **Pipeline Creation**
```bash
curl -X POST "http://localhost:8000/api/v1/build/pipeline" \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "sales_data_pipeline",
    "schedule": "daily",
    "dependencies": ["acquire", "discover", "transform"]
  }'
```

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

#### **3. Transformation Phase**
```bash
# Execute Stage 1 transformations
curl -X POST "http://localhost:8000/api/v1/transformation/transformations/stage1"

# Check transformation status
curl -X GET "http://localhost:8000/api/v1/transformation/status"

# List UDFs
curl -X GET "http://localhost:8000/api/v1/transformation/udfs"
```

### **Custom UDF Testing Workflow**

#### **1. Create Custom UDF**
```bash
curl -X POST "http://localhost:8000/api/v1/transformation/udfs" \
  -H "Content-Type: application/json" \
  -d '{
    "transformation_stage": "stage2",
    "udf_name": "custom_aggregation_udf",
    "udf_number": 1,
    "udf_logic": "INSERT INTO gold.aggregated_sales SELECT dealer_name, SUM(amount_sales) as total_sales, COUNT(*) as transaction_count FROM silver.sales_transactions_stage1 GROUP BY dealer_name",
    "udf_schema_name": "custom",
    "dependencies": ["transform_daily_sales_to_silver"],
    "execution_frequency": "daily"
  }'
```

#### **2. Create UDF Function in ClickHouse**
```bash
curl -X POST "http://localhost:8000/api/v1/transformation/udfs/create" \
  -H "Content-Type: application/json" \
  -d '{
    "udf_name": "custom_aggregation_udf",
    "transformation_stage": "stage2",
    "create_if_not_exists": true
  }'
```

#### **3. Execute Custom UDF**
```bash
curl -X POST "http://localhost:8000/api/v1/transformation/udfs/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "udf_name": "custom_aggregation_udf",
    "dry_run": false
  }'
```

### **Error Testing**

#### **Test Invalid Data Source**
```bash
curl -X POST "http://localhost:8000/api/v1/acquire/datasources/test" \
  -H "Content-Type: application/json" \
  -d '{"name": "NonExistentSource"}'
```

#### **Test Invalid UDF**
```bash
curl -X POST "http://localhost:8000/api/v1/transformation/udfs/execute" \
  -H "Content-Type: application/json" \
  -d '{
    "udf_name": "non_existent_udf",
    "dry_run": false
  }'
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
  "phase": "Transformation",
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

## 📝 **Notes**

- Replace `localhost:8000` with your actual server URL if different
- URL encode special characters in data source names (e.g., `%20` for spaces)
- Some endpoints require specific data to be present in ClickHouse tables
- Check server logs for detailed error information if requests fail
- Use the Swagger UI at `/docs` for interactive API testing
- Replace placeholder credentials (YOUR_AWS_ACCESS_KEY_ID, YOUR_POSTGRES_HOST, etc.) with your actual values