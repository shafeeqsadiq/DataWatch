import psycopg2
import random
from datetime import datetime, timedelta
import json
from faker import Faker

fake = Faker()

DB_HOST = "postgres" # Used inside the docker network
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "postgres"

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def generate_data(days=60):
    conn = get_connection()
    cur = conn.cursor()
    
    start_date = datetime.now() - timedelta(days=days)
    
    # 3 Anomaly days index
    anomaly_days = random.sample(range(10, days-5), 3)
    
    for i in range(days):
        current_date = start_date + timedelta(days=i)
        is_weekend = current_date.weekday() >= 5
        
        # Base daily count
        base_count = random.randint(800, 1200) if not is_weekend else random.randint(300, 500)
        
        if i in anomaly_days:
            # Drop the count severely to create a silent failure anomaly
            base_count = random.randint(10, 50)
            print(f"Injected anomaly on {current_date.date()} with volume {base_count}")
            
        daily_amount_sum = 0
            
        for _ in range(base_count):
            amount = round(random.uniform(10.0, 150.0), 2)
            daily_amount_sum += amount
            cur.execute(
                "INSERT INTO source_orders (order_id, user_id, amount, status, created_at) VALUES (%s, %s, %s, %s, %s)",
                (fake.uuid4(), fake.user_name(), amount, random.choice(['COMPLETED', 'PENDING', 'FAILED']), current_date)
            )
            
        # Snapshot for the day
        avg_amount = round(daily_amount_sum / base_count, 2) if base_count > 0 else 0
        
        cur.execute(
            """INSERT INTO row_count_snapshots (run_ts, row_count, null_pct, col_means) 
               VALUES (%s, %s, %s, %s)""",
            (current_date, base_count, random.uniform(0, 0.05), json.dumps({"amount": avg_amount}))
        )
        
    conn.commit()
    cur.close()
    conn.close()
    print("Historical data generation complete.")

if __name__ == "__main__":
    generate_data()
