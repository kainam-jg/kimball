-- DDL for metadata.discover
-- Extracted from ClickHouse
-- 
-- IMPORTANT: This file is auto-generated. Manual edits may be overwritten.
-- 

CREATE TABLE metadata.discover
(
    `original_table_name` String,
    `original_column_name` String,
    `new_table_name` String,
    `new_column_name` String,
    `inferred_type` String,
    `classification` String,
    `cardinality` UInt64,
    `null_count` UInt64,
    `sample_values` Array(String),
    `data_quality_score` Float64,
    `version` UInt64,
    `created_at` DateTime DEFAULT now(),
    `updated_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (original_table_name, original_column_name)
SETTINGS index_granularity = 8192;
