-- DDL for metadata.definitions
-- Extracted from ClickHouse
-- 
-- IMPORTANT: This file is auto-generated. Manual edits may be overwritten.
-- 

CREATE TABLE metadata.definitions
(
    `id` UInt64,
    `schema_name` String,
    `table_name` String,
    `column_name` String,
    `column_type` String,
    `column_precision` String,
    `column_description` String,
    `created_at` DateTime DEFAULT now(),
    `updated_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (schema_name, table_name, column_name)
SETTINGS index_granularity = 8192;
