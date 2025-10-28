-- Stage1 Transformation: dealer_regions to dealer_regions_stage1
-- Converts bronze layer raw data to silver layer cleaned, typed data
-- Source: bronze.dealer_regions
-- Target: silver.dealer_regions_stage1

-- Drop existing silver table
DROP TABLE IF EXISTS silver.dealer_regions_stage1;

-- Create silver table with proper types
CREATE TABLE silver.dealer_regions_stage1 (
    region String,
    district String,
    country_name String,
    city_name String,
    dealer_name String
) ENGINE = MergeTree()
ORDER BY tuple();

-- Insert data into silver table with type conversions
INSERT INTO silver.dealer_regions_stage1
SELECT 
    trim(region) AS region,
    trim(district) AS district,
    trim(country_name) AS country_name,
    trim(city_name) AS city_name,
    trim(dealer_name) AS dealer_name
FROM bronze.dealer_regions;
