import time
import psycopg2
from prometheus_client import start_http_server, Gauge
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
ROW_COUNT = Gauge('pipeline_last_batch_row_count', 'Row count of the last ingested batch')
ANOMALY_TOTAL = Gauge('pipeline_anomaly_total', 'Total number of detected anomalies')
QUARANTINE_SIZE = Gauge('pipeline_quarantine_size', 'Number of rows currently in quarantine')
LAST_SUCCESS_TS = Gauge('pipeline_last_success_ts', 'Timestamp of the last successful pipeline run')

DB_CONN = os.environ.get('DB_CONN', "postgresql://postgres:postgres@postgres/postgres")

def update_metrics():
    try:
        conn = psycopg2.connect(DB_CONN)
        cur = conn.cursor()
        
        # 1. Update Row Count
        cur.execute("SELECT row_count FROM row_count_snapshots ORDER BY run_ts DESC LIMIT 1")
        res = cur.fetchone()
        if res:
            ROW_COUNT.set(res[0])
            
        # 2. Update Anomaly Total
        cur.execute("SELECT COUNT(*) FROM anomaly_events")
        res = cur.fetchone()
        if res:
            ANOMALY_TOTAL.set(res[0])
            
        # 3. Update Quarantine Size
        cur.execute("SELECT COUNT(*) FROM quarantine_records")
        res = cur.fetchone()
        if res:
            QUARANTINE_SIZE.set(res[0])
            
        # 4. Update Last Success TS
        cur.execute("SELECT extract(epoch from run_ts) FROM row_count_snapshots ORDER BY run_ts DESC LIMIT 1")
        res = cur.fetchone()
        if res:
            LAST_SUCCESS_TS.set(res[0])
            
        logger.info("Metrics updated successfully.")
            
    except Exception as e:
        logger.error(f"Failed to fetch metrics from DB: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")
    
    # Generate some requests.
    while True:
        update_metrics()
        time.sleep(15) # update every 15 seconds
