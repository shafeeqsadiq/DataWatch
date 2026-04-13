from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'datawatch',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # In real life, use days_ago(1)
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ingest_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    target_date = datetime.now()
    
    # Simple extract-load simulation from source_orders to target_orders 
    # For now, let's say it moves current hour's data
    records = pg_hook.get_records(
        "SELECT order_id, user_id, amount, status, created_at FROM source_orders WHERE status = 'COMPLETED' LIMIT 100"
    )
    
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    row_count = 0
    total_amount = 0
    
    for row in records:
        cursor.execute(
            "INSERT INTO target_orders (order_id, user_id, amount, status, created_at) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING",
            (row[0], row[1], row[2], row[3], row[4])
        )
        row_count += 1
        total_amount += row[2]
        
    # Snapshot taking
    avg_amount = total_amount / row_count if row_count > 0 else 0
    
    cursor.execute(
        """INSERT INTO row_count_snapshots (run_ts, row_count, null_pct, col_means) 
           VALUES (%s, %s, %s, %s)""",
        (target_date, row_count, 0.0, json.dumps({"amount": avg_amount}))
    )
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Ingested {row_count} records.")

with DAG(
    'ingest_dag',
    default_args=default_args,
    description='Hourly ingestion pipeline',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_source_to_target',
        python_callable=ingest_data,
        provide_context=True
    )

