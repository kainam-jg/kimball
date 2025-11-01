CREATE TABLE metadata.transformation0
(
    `transformation_stage` String DEFAULT 'stage0',
    `transformation_id` UInt32,
    `transformation_name` String,
    `source_id` String,
    `acquisition_logic` String,
    `acquisition_type` String,
    `target_table` String,
    `execution_frequency` String,
    `execution_sequence` UInt32,
    `statement_type` String,
    `dependencies` Array(String),
    `created_at` DateTime DEFAULT now(),
    `updated_at` DateTime DEFAULT now(),
    `version` UInt64 DEFAULT 1,
    `metadata` String
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (transformation_id, execution_sequence)
SETTINGS index_granularity = 8192;

