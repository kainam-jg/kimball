-- Metadata Definitions Table
-- Stores column-level metadata for all tables across bronze, silver, and gold schemas

CREATE TABLE IF NOT EXISTS metadata.definitions (
    id UInt64,
    schema_name String,
    table_name String,
    column_name String,
    column_type String,
    column_precision String,
    column_description String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (schema_name, table_name, column_name)
SETTINGS index_granularity = 8192;

