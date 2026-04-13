-- create database tables

CREATE TABLE IF NOT EXISTS source_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS target_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    amount NUMERIC(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quarantine_records (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    reason VARCHAR(255) NOT NULL,
    row_sample JSONB NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS row_count_snapshots (
    id SERIAL PRIMARY KEY,
    run_ts TIMESTAMP NOT NULL,
    row_count INT NOT NULL,
    null_pct FLOAT NOT NULL,
    col_means JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomaly_events (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    detail TEXT NOT NULL,
    remediation_taken VARCHAR(100),
    notification_sent BOOLEAN DEFAULT FALSE,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);
