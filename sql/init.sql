-- Initialize AI Log Analytics Database

CREATE TABLE IF NOT EXISTS logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    level VARCHAR(10) NOT NULL,
    message TEXT NOT NULL,
    service VARCHAR(100) NOT NULL,
    host VARCHAR(100),
    environment VARCHAR(50),
    trace_id VARCHAR(100),
    span_id VARCHAR(100),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_logs_timestamp ON logs(timestamp DESC);
CREATE INDEX idx_logs_level ON logs(level);
CREATE INDEX idx_logs_service ON logs(service);
CREATE INDEX idx_logs_trace_id ON logs(trace_id);

CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    service VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    score DECIMAL(5,3),
    description TEXT,
    status VARCHAR(20) DEFAULT 'OPEN',
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_anomalies_timestamp ON anomalies(timestamp DESC);
CREATE INDEX idx_anomalies_service ON anomalies(service);
CREATE INDEX idx_anomalies_severity ON anomalies(severity);

CREATE TABLE IF NOT EXISTS ml_models (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_path TEXT,
    metrics JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
