-- Stage1 Transformation: dealer_sales to dealer_sales_stage1
-- Converts bronze layer raw data to silver layer cleaned, typed data
-- Source: bronze.dealer_sales
-- Target: silver.dealer_sales_stage1

-- Drop existing silver table
DROP TABLE IF EXISTS silver.dealer_sales_stage1;

-- Create silver table with proper types
CREATE TABLE silver.dealer_sales_stage1 (
    amount_sales Float64,
    dealer_name String,
    sales_date Date,
    vehicle_model String
) ENGINE = MergeTree()
ORDER BY tuple();

-- Insert data into silver table with type conversions
INSERT INTO silver.dealer_sales_stage1
SELECT 
    toFloat64OrNull(amount_sales) AS amount_sales,
    trim(dealer_name) AS dealer_name,
    toDate(parseDateTimeBestEffort(sales_date)) AS sales_date,
    trim(vehicle_model) AS vehicle_model
FROM bronze.dealer_sales;
