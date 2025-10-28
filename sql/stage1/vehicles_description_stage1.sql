-- Stage1 Transformation: vehicles_description to vehicles_description_stage1
-- Converts bronze layer raw data to silver layer cleaned, typed data
-- Source: bronze.vehicles_description
-- Target: silver.vehicles_description_stage1

-- Drop existing silver table
DROP TABLE IF EXISTS silver.vehicles_description_stage1;

-- Create silver table with proper types
CREATE TABLE silver.vehicles_description_stage1 (
    vehicle_model String,
    vehicle_model_description String
) ENGINE = MergeTree()
ORDER BY tuple();

-- Insert data into silver table with type conversions
INSERT INTO silver.vehicles_description_stage1
SELECT 
    trim(vehicle_model) AS vehicle_model,
    trim(vehicle_model_description) AS vehicle_model_description
FROM bronze.vehicles_description;
