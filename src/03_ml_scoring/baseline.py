


"""
Customer Baseline Creation
Creates statistical profiles for each customer
"""

import duckdb

def create_baselines():
    """
    Create customer behavioral baselines
    """
    
    conn = duckdb.connect('data/fraud_data.duckdb')
    
    # Drop and recreate table
    conn.execute("DROP TABLE IF EXISTS customer_baselines")
    
    conn.execute("""
        CREATE TABLE customer_baselines AS
        SELECT 
            nameOrig as customer_id,
            COUNT(*) as tx_count,
            AVG(amount) as avg_amount,
            STDDEV(amount) as std_amount,
            MAX(amount) as max_amount,
            MIN(amount) as min_amount,
            AVG(oldbalanceOrg) as avg_balance,
            COUNT(DISTINCT type) as tx_types
        FROM transactions
        GROUP BY nameOrig
        HAVING COUNT(*) >= 2
    """)
    
    count = conn.execute("SELECT COUNT(*) FROM customer_baselines").fetchone()[0]
    print(f"[INFO] Created baselines for {count:,} customers")
    
    conn.close()

if __name__ == "__main__":
    create_baselines()
