import subprocess
import json
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_dbt_tests(dbt_project_dir="/opt/airflow/dbt"):
    """
    Runs `dbt test` as a subprocess and parses the `run_results.json` to find failures.
    """
    # Run dbt test
    try:
        result = subprocess.run(
            ["dbt", "test", "--project-dir", dbt_project_dir, "--profiles-dir", dbt_project_dir],
            capture_output=True,
            text=True,
            check=False
        )
    except FileNotFoundError:
        logger.error("dbt executable not found.")
        return False, {"error": "dbt not installed"}

    target_json_path = os.path.join(dbt_project_dir, "target", "run_results.json")
    
    if not os.path.exists(target_json_path):
        logger.warning("dbt run_results.json not found. Assuming no tests ran or dbt failed completely.")
        return False, {"error": "run_results.json missing", "stdout": result.stdout}
        
    with open(target_json_path, 'r') as f:
        run_results = json.load(f)
        
    failures = []
    
    for res in run_results.get("results", []):
        if res.get("status") in ["fail", "error"]:
            failures.append({
                "test_name": res.get("unique_id"),
                "status": res.get("status"),
                "message": res.get("message")
            })
            
    is_schema_anomaly = len(failures) > 0
    
    detail = {
        "failed_test_count": len(failures),
        "failures": failures
    }
    
    if is_schema_anomaly:
        logger.warning(f"Schema Test Failures Detected! Count: {len(failures)}")
        
    return is_schema_anomaly, detail

if __name__ == "__main__":
    # Point to the local dbt dir for testing
    is_anomaly, detail = run_dbt_tests("../dbt")
    print(is_anomaly, detail)
