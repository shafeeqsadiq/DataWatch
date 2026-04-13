import pandas as pd
from prophet import Prophet
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_snapshot_data(db_conn_string, days=60):
    query = """
        SELECT run_ts as ds, row_count as y
        FROM row_count_snapshots
        ORDER BY run_ts DESC
        LIMIT %s
    """
    df = pd.read_sql(query, db_conn_string, params=(days,))
    return df.sort_values(by="ds")

def check_volume_anomaly(db_conn_string):
    """
    Trains a Prophet model on past row_count_snapshots and checks if the most recent
    row_count falls outside the 95% confidence interval.
    """
    df = get_snapshot_data(db_conn_string)
    if df.empty or len(df) < 10:
        logger.info("Not enough data to train volume detector.")
        return False, {}

    # The last row is our "current" batch to evaluate
    history_df = df.iloc[:-1]
    current_obs = df.iloc[-1]
    
    # Train model
    m = Prophet(interval_width=0.95, yearly_seasonality=False)
    m.fit(history_df)
    
    # Predict for the current observation timestamp
    future = pd.DataFrame({'ds': [current_obs['ds']]})
    forecast = m.predict(future)
    
    yhat_lower = forecast['yhat_lower'].values[0]
    yhat_upper = forecast['yhat_upper'].values[0]
    actual_y = current_obs['y']
    
    is_anomaly = actual_y < yhat_lower or actual_y > yhat_upper
    
    detail = {
        'actual': actual_y,
        'expected_lower': yhat_lower,
        'expected_upper': yhat_upper,
        'timestamp': current_obs['ds'].isoformat()
    }
    
    if is_anomaly:
        logger.warning(f"Volume Anomaly Detected! Actual: {actual_y}, Expected bound: [{yhat_lower}, {yhat_upper}]")
    
    return is_anomaly, detail

if __name__ == "__main__":
    is_anomaly, details = check_volume_anomaly("postgresql+psycopg2://postgres:postgres@localhost/postgres")
    print(is_anomaly, details)
