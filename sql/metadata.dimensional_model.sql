-- DDL for metadata.dimensional_model
-- Extracted from ClickHouse
-- 
-- IMPORTANT: This file is auto-generated. Manual edits may be overwritten.
-- 

CREATE TABLE metadata.dimensional_model
(
    `id` UInt64,
    `recommendation_timestamp` DateTime,
    `table_type` String,
    `recommended_name` String,
    `source_table` String,
    `original_table_name` String,
    `hierarchy_name` String,
    `root_column` String,
    `leaf_column` String,
    `fact_columns` Array(String),
    `dimension_keys` Array(String),
    `columns` Array(String),
    `column_details` Array(String),
    `total_columns` UInt32,
    `hierarchy_levels` UInt32,
    `relationships` Array(String),
    `metadata_json` String,
    `created_at` DateTime DEFAULT now(),
    `final_name` String
)
ENGINE = ReplacingMergeTree
ORDER BY (recommendation_timestamp, table_type, recommended_name)
SETTINGS index_granularity = 8192;
