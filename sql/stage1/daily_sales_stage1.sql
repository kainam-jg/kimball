-- Stage1 Transformation: daily_sales to daily_sales_stage1
-- Converts bronze layer raw data to silver layer cleaned, typed data
-- Source: bronze.daily_sales
-- Target: silver.daily_sales_stage1

-- Drop existing silver table
DROP TABLE IF EXISTS silver.daily_sales_stage1;

-- Create silver table with proper types
CREATE TABLE silver.daily_sales_stage1 (
    sales_amount Float64,
    amount_sales Float64,
    dealer_name String,
    sales_date Date,
    vehicle_model String
) ENGINE = MergeTree()
ORDER BY tuple();

-- Insert data into silver table with type conversions
INSERT INTO silver.daily_sales_stage1
SELECT 
    toFloat64OrNull(amount_sales) AS sales_amount,
    toFloat64OrNull(amount_sales) AS amount_sales,
    trim(dealer_name) AS dealer_name,
    toDate(parseDateTimeBestEffort(sales_date)) AS sales_date,
    trim(vehicle_model) AS vehicle_model
FROM bronze.daily_sales;
