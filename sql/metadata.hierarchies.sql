-- DDL for metadata.hierarchies
-- Extracted from ClickHouse
-- 
-- IMPORTANT: This file is auto-generated. Manual edits may be overwritten.
-- 

CREATE TABLE metadata.hierarchies
(
    `id` UInt64,
    `schema_name` String,
    `analysis_timestamp` DateTime,
    `table_name` String,
    `original_table_name` String,
    `hierarchy_name` String,
    `total_levels` UInt32,
    `root_column` String,
    `root_cardinality` UInt64,
    `leaf_column` String,
    `leaf_cardinality` UInt64,
    `intermediate_levels` Array(String),
    `parent_child_relationships` Array(String),
    `sibling_relationships` Array(String),
    `cross_hierarchy_relationships` Array(String),
    `metadata_json` String,
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (schema_name, table_name, analysis_timestamp)
SETTINGS index_granularity = 8192;
