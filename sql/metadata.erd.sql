-- DDL for metadata.erd
-- Extracted from ClickHouse
-- 
-- IMPORTANT: This file is auto-generated. Manual edits may be overwritten.
-- 

CREATE TABLE metadata.erd
(
    `id` UInt64,
    `schema_name` String,
    `analysis_timestamp` DateTime,
    `table_name` String,
    `table_type` String,
    `row_count` UInt64,
    `column_count` UInt32,
    `primary_key_candidates` Array(String),
    `fact_columns` Array(String),
    `dimension_columns` Array(String),
    `relationships` Array(String),
    `metadata_json` String,
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (schema_name, table_name, analysis_timestamp)
SETTINGS index_granularity = 8192;
