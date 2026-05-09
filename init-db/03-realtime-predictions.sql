-- Connect to mlplatform database
\c mlplatform;

CREATE TABLE IF NOT EXISTS realtime_predictions (
    id SERIAL PRIMARY KEY,
    batch_id BIGINT,
    review_id TEXT,
    product_id TEXT,
    user_id TEXT,
    score INTEGER,
    true_label TEXT,
    text TEXT,
    cleaned_text TEXT,
    prediction TEXT,
    class_id INTEGER,
    confidence DOUBLE PRECISION,
    api_latency_ms DOUBLE PRECISION,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_realtime_predictions_processed_at
ON realtime_predictions (processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_realtime_predictions_prediction
ON realtime_predictions (prediction);

CREATE INDEX IF NOT EXISTS idx_realtime_predictions_batch_id
ON realtime_predictions (batch_id);

CREATE INDEX IF NOT EXISTS idx_realtime_predictions_score
ON realtime_predictions (score);
