


import duckdb
import os

# CONFIGURATION
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")

def detect_structuring():
    """
    RULE #1: Structuring Detection (Smurfing)
    
    Logic:
    Identify customers who perform multiple small transactions (just below a threshold)
    that essentially add up to a significant amount, likely to evade reporting limits.
    
    Criteria:
    - Transaction Type: CASH_OUT or TRANSFER
    - Amount: Small (< $10,000)
    - Frequency: High (> 5 transactions) within the dataset window
    - Total Volume: Significant (> $10,000)
    """
    print("[INFO] Running Rule: Structuring Detection (Smurfing)...")
    
    conn = duckdb.connect(DB_FILE)
    
    # SQL Query for Structuring
    # We group by user (nameOrig) and look for patterns
    query = """
        SELECT 
            nameOrig as client_id,
            COUNT(*) as txn_count,
            CAST(SUM(amount) AS DECIMAL(18,2)) as total_volume,
            CAST(AVG(amount) AS DECIMAL(18,2)) as avg_amount,
            'Structuring' as alert_type,
            90 as risk_score
        FROM transactions
        WHERE type IN ('CASH_OUT', 'TRANSFER')
          AND amount < 10000  -- Typical threshold evasion
        GROUP BY nameOrig
        HAVING count >= 5             -- Multiple operations
           AND total_volume > 10000   -- Total exceeds reporting threshold
        ORDER BY total_volume DESC
    """
    
    try:
        # Execute query
        alerts_df = conn.execute(query).df()
        
        # Save alerts to a permanent table in DuckDB
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                client_id VARCHAR, 
                txn_count INTEGER, 
                total_volume DECIMAL, 
                avg_amount DECIMAL, 
                alert_type VARCHAR, 
                risk_score INTEGER
            )
        """)
        
        # Clear previous alerts of this type to avoid duplication during testing
        conn.execute("DELETE FROM alerts WHERE alert_type = 'Structuring'")
        
        # Insert new alerts
        conn.execute("INSERT INTO alerts SELECT * FROM alerts_df")
        
        print(f"[SUCCESS] Structuring Alerts Found: {len(alerts_df)}")
        print("\n[INFO] Top 5 Suspicious Clients:")
        print(alerts_df.head(5))
        
    except Exception as e:
        print(f"[ERROR] Executing rule: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    detect_structuring()
