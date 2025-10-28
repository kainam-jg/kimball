-- Stage1 Transformation: vehicles to vehicles_stage1
-- Converts bronze layer raw data to silver layer cleaned, typed data
-- Source: bronze.vehicles
-- Target: silver.vehicles_stage1

-- Drop existing silver table
DROP TABLE IF EXISTS silver.vehicles_stage1;

-- Create silver table with proper types
CREATE TABLE silver.vehicles_stage1 (
    vehicle_class String,
    vehicle_type String,
    vehicle_model String
) ENGINE = MergeTree()
ORDER BY tuple();

-- Insert data into silver table with type conversions
INSERT INTO silver.vehicles_stage1
SELECT 
    trim(vehicle_class) AS vehicle_class,
    trim(vehicle_type) AS vehicle_type,
    trim(vehicle_model) AS vehicle_model
FROM bronze.vehicles;
