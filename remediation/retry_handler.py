import logging
import psycopg2
from remediation.quarantine import quarantine_batch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_anomaly_event(db_conn_string, run_id, anomaly_type, severity, detail, remediation_taken, notification_sent=False):
    try:
        conn = psycopg2.connect(db_conn_string)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO anomaly_events (run_id, type, severity, detail, remediation_taken, notification_sent)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (run_id, anomaly_type, severity, detail, remediation_taken, notification_sent))
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to log anomaly event: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def send_slack_alert(message):
    """
    Mocks a Slack webhook alert.
    """
    logger.warning(f"🚨 [SLACK ALERT MOCK] {message}")
    # TODO: Implement real requests.post(WEBHOOK_URL, json={"text": message})
    return False # returning false as literal webhook isn't sent 

def handle_anomaly(db_conn_string, run_id, anomalies_detected):
    """
    Decision tree for anomalies.
    anomalies_detected: dict containing anomaly results, e.g.,
    {
      "volume": {"is_anomaly": True, "detail": {...}},
      "distribution": {"is_anomaly": False, "detail": {...}},
      "schema": {"is_anomaly": True, "detail": {...}}
    }
    """
    
    for anomaly_type, result in anomalies_detected.items():
        if not result.get('is_anomaly'):
            continue
            
        detail = str(result.get('detail', ''))
        
        if anomaly_type == 'schema':
            # Schema failures mean data is corrupt. High severity.
            severity = 'CRITICAL'
            remediation = 'QUARANTINE_AND_ALERT'
            quarantine_batch(db_conn_string, run_id, detail)
            send_slack_alert(f"Critical schema anomaly in run {run_id}. Batch quarantined. Details: {detail}")
            log_anomaly_event(db_conn_string, run_id, anomaly_type, severity, detail, remediation, False)
            
        elif anomaly_type == 'volume':
            # Volume drops can be transient source issues or real drops.
            severity = 'HIGH'
            remediation = 'ALERT_ONLY'
            send_slack_alert(f"Volume anomaly in run {run_id}. Volume outside expected bounds. Details: {detail}")
            log_anomaly_event(db_conn_string, run_id, anomaly_type, severity, detail, remediation, False)
            
        elif anomaly_type == 'distribution':
            # Distribution drift might be a slow change or a sudden issue. Medium severity.
            severity = 'MEDIUM'
            remediation = 'LOG_ONLY'
            logger.info(f"Distribution drift detected. Details: {detail}")
            log_anomaly_event(db_conn_string, run_id, anomaly_type, severity, detail, remediation, False)
            
    return True
