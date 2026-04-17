# DataWatch: Autonomous Batch Pipeline Monitoring with ML-Based Anomaly Detection and Fault Tolerance

DataWatch is a batch processing data pipeline that orchestrates heavy data ingestion on a defined schedule. It continuously monitors operational health using statistical and ML-based anomaly detection, enabling proactive failure detection and automated fault mitigation to prevent silent failures from corrupting downstream analytics and ML training models.

## Architecture

1. **Ingestion (Airflow)**: Pulls data from `source_orders` and loads to `target_orders`. Snapshots run metadata (row count, null percentage, column means).
2. **Detectors**:
   - **Volume (Prophet)**: Detects unexpected drops/spikes in row counts using a 95% confidence interval.
   - **Distribution (SciPy KS Test)**: Detects drift in column value distributions between the last batch and the previous 7 days.
   - **Schema (dbt)**: Runs dbt tests to ensure data integrity (unique keys, not null, expected ranges).
3. **Remediation**: Maps anomaly severity to actions (Critical = Quarantine + Slack Alert, High = Alert, Medium = Log).
4. **Observability**: Prometheus scrapes custom metrics exposed by the pipeline, visualized in Grafana.

## Technology Stack

- **Apache Airflow**: Workflow orchestration and task scheduling.
- **dbt (Data Build Tool)**: Hardcoded logical constraints and structural database anomaly detection.
- **Facebook Prophet**: Machine Learning time-series model for predicting volumetric anomaly drops/spikes.
- **SciPy**: Mathematical module utilized for Kolmogorov-Smirnov (KS) statistical distribution drift calculations.
- **PostgreSQL**: Central data warehouse storing raw data, snapshots, quarantine records, and logs.
- **Prometheus & Grafana**: Constant metric scraping and visualization for live pipeline observability.
- **Docker Compose**: Entire ecosystem containerization for instant, isolated deployment.

## Getting Started

### 1. Start Infrastructure
` ` `bash
docker-compose up -d --build
` ` `
This starts PostgreSQL, Apache Airflow, Prometheus, Grafana, and the custom metrics exporter.

### 2. Generate Seed Data
We need history to train Prophet and setup the KS tests.
` ` `bash
docker-compose exec airflow-scheduler python /opt/airflow/seed/generate_historical_data.py
` ` `

### 3. Access the UIs
- **Airflow**: http://localhost:8080 (admin / admin)
- **Grafana**: http://localhost:3000 (admin / admin) - See the "DataWatch Pipeline Health" dashboard.
- **Prometheus**: http://localhost:9090

### 4. Enable DAGs
1. Go to Airflow UI.
2. Enable `ingest_dag`.
3. Enable `anomaly_check_dag`.

## Triggering an Anomaly

To test the fault tolerance manually:

1. Connect to Postgres:
   ` ` `bash
   docker-compose exec postgres psql -U postgres -d postgres
   ` ` `
2. Inject bad data:
   ` ` `sql
   -- This will fail the dbt schema tests (amount is negative, which could be an unaccepted value if configured,
   -- or status is an invalid enum, or inserting a duplicate ID).
   -- For our dbt test, status must be COMPLETED, PENDING, or FAILED. Let's insert a random string.
   
   INSERT INTO target_orders (order_id, user_id, amount, status, created_at, loaded_at) 
   VALUES ('anomaly_999', 'user_1', 100.0, 'INVALID_STATUS', NOW(), NOW());
   ` ` `
3. Run the `anomaly_check_dag` manually in Airflow.
4. Check the logs of the `run_detectors` task: It will catch the schema failure, quarantine the batch, and simulate a Slack alert.
5. Check the Grafana dashboard to see the anomaly count tick up and the quarantine size increase.
