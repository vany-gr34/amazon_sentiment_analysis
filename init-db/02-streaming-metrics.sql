CREATE TABLE IF NOT EXISTS streaming_metrics (
    id SERIAL PRIMARY KEY,
    batch_id BIGINT,
    metric_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    sentiment TEXT
);