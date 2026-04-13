import os
import sys

# Ensure modules in the project root can be imported
sys.path.insert(0, '/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from detectors.volume_detector import check_volume_anomaly
from detectors.distribution_detector import check_distribution_drift
from detectors.schema_detector import run_dbt_tests
from remediation.retry_handler import handle_anomaly

default_args = {
    'owner': 'datawatch',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

DB_CONN = "postgresql://postgres:postgres@postgres/postgres"

def run_anomaly_checks(**kwargs):
    run_id = kwargs['run_id']
    print(f"Running anomaly checks for run_id: {run_id}")
    
    anomalies = {}
    
    # 1. Volume Check
    is_vol_anomaly, vol_detail = check_volume_anomaly(DB_CONN)
    anomalies['volume'] = {'is_anomaly': is_vol_anomaly, 'detail': vol_detail}
    
    # 2. Distribution Check
    is_dist_drift, dist_detail = check_distribution_drift(DB_CONN)
    anomalies['distribution'] = {'is_anomaly': is_dist_drift, 'detail': dist_detail}
    
    # 3. Schema Check (dbt)
    is_schema_anomaly, schema_detail = run_dbt_tests(dbt_project_dir="/opt/airflow/dbt")
    anomalies['schema'] = {'is_anomaly': is_schema_anomaly, 'detail': schema_detail}
    
    # Handle any detected anomalies
    handle_anomaly(DB_CONN, run_id, anomalies)
    
    print("Anomaly checks completed.")

with DAG(
    'anomaly_check_dag',
    default_args=default_args,
    description='Runs data quality and anomaly detectors',
    schedule_interval='5 * * * *', # Runs at 5 mins past the hour, after ingest
    catchup=False
) as dag:

    check_task = PythonOperator(
        task_id='run_detectors',
        python_callable=run_anomaly_checks,
        provide_context=True
    )

