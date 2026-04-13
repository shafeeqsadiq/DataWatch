import pandas as pd
from scipy.stats import ks_2samp
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_distribution_drift(db_conn_string, column='amount', batch_interval_hours=1):
    """
    Compares the distribution of `column` in the most recent batch against 
    the previous 7 days using the Kolmogorov-Smirnov test.
    """
    
    recent_query = f"""
        SELECT {column}
        FROM target_orders
        WHERE created_at >= NOW() - INTERVAL '{batch_interval_hours} hours'
    """
    
    historical_query = f"""
        SELECT {column}
        FROM target_orders
        WHERE created_at >= NOW() - INTERVAL '7 days'
          AND created_at < NOW() - INTERVAL '{batch_interval_hours} hours'
    """
    
    try:
        recent_df = pd.read_sql(recent_query, db_conn_string)
        historical_df = pd.read_sql(historical_query, db_conn_string)
        
        if recent_df.empty or historical_df.empty:
            logger.info("Insufficient data for KS test.")
            return False, {}
            
        recent_data = recent_df[column].dropna().values
        historical_data = historical_df[column].dropna().values
        
        # Perform KS test
        statistic, p_value = ks_2samp(historical_data, recent_data)
        
        # If p-value < 0.05, we reject the null hypothesis that the distributions are the same
        is_drift = p_value < 0.05
        
        detail = {
            'column': column,
            'ks_statistic': float(statistic),
            'p_value': float(p_value)
        }
        
        if is_drift:
            logger.warning(f"Distribution Drift Detected on {column}! p-value: {p_value}")
            
        return is_drift, detail

    except Exception as e:
        logger.error(f"Error in distribution drift check: {e}")
        return False, {'error': str(e)}

if __name__ == "__main__":
    is_drift, detail = check_distribution_drift("postgresql+psycopg2://postgres:postgres@localhost/postgres")
    print(is_drift, detail)
