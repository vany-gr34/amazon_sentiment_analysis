-- Connect to mlplatform database
\c mlplatform;

CREATE TABLE IF NOT EXISTS streaming_metrics (
    id SERIAL PRIMARY KEY,
    batch_id BIGINT,
    metric_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    sentiment TEXT
);

CREATE INDEX IF NOT EXISTS idx_streaming_metrics_time
ON streaming_metrics (metric_time DESC);

CREATE INDEX IF NOT EXISTS idx_streaming_metrics_name
ON streaming_metrics (metric_name);
