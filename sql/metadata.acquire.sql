CREATE TABLE metadata.acquire
(
    `source_id` String,
    `source_name` String,
    `source_type` String,
    `connection_config` String,
    `enabled` UInt8 DEFAULT 1,
    `description` String,
    `created_at` DateTime DEFAULT now(),
    `updated_at` DateTime DEFAULT now(),
    `version` UInt64 DEFAULT 1
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (source_id)
SETTINGS index_granularity = 8192;

