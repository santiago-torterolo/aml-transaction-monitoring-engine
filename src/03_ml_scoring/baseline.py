


import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")

def build_customer_baselines():
    """
    ML COMPONENT: Customer Behavioral Baseline
    
    Purpose:
    Create statistical profiles for each customer based on their transaction history.
    This allows us to detect deviations from normal behavior (anomalies).
    
    Metrics Calculated:
    - Average transaction amount
    - Standard deviation of amounts
    - Transaction frequency
    - Typical transaction types
    - Common beneficiaries
    """
    print("[INFO] Building customer behavioral baselines...")
    
    conn = duckdb.connect(DB_FILE)
    
    query = """
        CREATE OR REPLACE TABLE customer_baselines AS
        SELECT 
            nameOrig as client_id,
            COUNT(*) as total_transactions,
            CAST(AVG(amount) AS DECIMAL(18,2)) as avg_amount,
            CAST(STDDEV(amount) AS DECIMAL(18,2)) as stddev_amount,
            CAST(MIN(amount) AS DECIMAL(18,2)) as min_amount,
            CAST(MAX(amount) AS DECIMAL(18,2)) as max_amount,
            COUNT(DISTINCT type) as unique_txn_types,
            COUNT(DISTINCT nameDest) as unique_recipients,
            CAST(SUM(amount) AS DECIMAL(18,2)) as lifetime_volume
        FROM transactions
        GROUP BY nameOrig
        HAVING COUNT(*) >= 2
    """
    
    try:
        conn.execute(query)
        
        count = conn.execute("SELECT COUNT(*) FROM customer_baselines").fetchone()[0]
        
        print(f"[SUCCESS] Customer baselines created: {count} profiles")
        
        print("\n[INFO] Sample baseline profiles:")
        sample = conn.execute("SELECT * FROM customer_baselines LIMIT 5").df()
        print(sample)
        
    except Exception as e:
        print(f"[ERROR] Failed to build baselines: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    build_customer_baselines()
