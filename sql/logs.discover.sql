-- Logs for Discover Phase
-- Stores all logs related to metadata discovery operations

CREATE TABLE IF NOT EXISTS logs.discover
(
    `timestamp` DateTime DEFAULT now(),
    `logger_name` String,
    `log_level` String,
    `message` String,
    `module` String,
    `function` String,
    `line_number` UInt32,
    `phase` String DEFAULT 'Discover',
    `endpoint` String,
    `method` String,
    `request_data` String,
    `error_type` String,
    `error_traceback` String,
    `metadata` String,
    `schema_name` String,
    `table_name` String
)
ENGINE = MergeTree()
ORDER BY (timestamp, logger_name, log_level)
TTL timestamp + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

