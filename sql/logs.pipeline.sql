-- Logs for Pipeline Phase
-- Stores all logs related to pipeline orchestration and scheduling

CREATE TABLE IF NOT EXISTS logs.pipeline
(
    `timestamp` DateTime DEFAULT now(),
    `logger_name` String,
    `log_level` String,
    `message` String,
    `module` String,
    `function` String,
    `line_number` UInt32,
    `phase` String DEFAULT 'Pipeline',
    `endpoint` String,
    `method` String,
    `request_data` String,
    `error_type` String,
    `error_traceback` String,
    `metadata` String,
    `pipeline_id` String,
    `execution_id` String,
    `schedule_type` String
)
ENGINE = MergeTree()
ORDER BY (timestamp, logger_name, log_level)
TTL timestamp + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

