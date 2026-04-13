import psycopg2
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def quarantine_batch(db_conn_string, run_id, reason, sample_limit=10):
    """
    Moves a sample of the most recent batch from target_orders to quarantine_records,
    then deletes the bad batch from target_orders.
    """
    try:
        conn = psycopg2.connect(db_conn_string)
        cur = conn.cursor()
        
        # Get a sample of the recent batch (loaded in the last hour)
        cur.execute("""
            SELECT row_to_json(t) 
            FROM (
                SELECT * FROM target_orders 
                WHERE loaded_at >= NOW() - INTERVAL '1 hour'
                LIMIT %s
            ) t
        """, (sample_limit,))
        
        samples = [row[0] for row in cur.fetchall()]
        sample_json = json.dumps(samples)
        
        # Insert into quarantine
        cur.execute("""
            INSERT INTO quarantine_records (run_id, reason, row_sample)
            VALUES (%s, %s, %s)
        """, (run_id, reason, sample_json))
        
        # Delete the bad batch to prevent it from going downstream
        cur.execute("""
            DELETE FROM target_orders
            WHERE loaded_at >= NOW() - INTERVAL '1 hour'
        """)
        
        conn.commit()
        logger.info(f"Successfully quarantined batch for run {run_id} due to: {reason}")
        
    except Exception as e:
        logger.error(f"Failed to quarantine batch: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    quarantine_batch("postgresql+psycopg2://postgres:postgres@localhost/postgres", "test_run", "Manual test quarantine")
