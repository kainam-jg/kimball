CREATE TABLE metadata.transformation2
(

    `transformation_stage` String,
    `transformation_id` UInt32,
    `transformation_name` String,
    `transformation_schema_name` String,
    `dependencies` Array(String),
    `execution_frequency` String,
    `execution_sequence` UInt32,
    `statement_type` String,
    `created_at` DateTime DEFAULT now(),
    `updated_at` DateTime DEFAULT now(),
    `version` UInt64 DEFAULT 1,
    `sql_data` String,
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (transformation_id,
 execution_sequence)
SETTINGS index_granularity = 8192;