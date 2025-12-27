


import duckdb
import os

# CONFIGURATION
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")

def detect_structuring():
    """
    RULE #1: Structuring Detection (Smurfing)
    
    Logic:
    Identify customers who perform multiple small transactions that sum to 
    significant amounts, potentially to evade reporting thresholds.
    
    Criteria:
    - Transaction Type: CASH_OUT, TRANSFER, or PAYMENT
    - Amount: Below $50,000 per transaction
    - Frequency: Minimum 3 transactions per client
    - Total Volume: Exceeds $10,000
    """
    print("[INFO] Running Rule: Structuring Detection...")
    
    conn = duckdb.connect(DB_FILE)
    
    query = """
        SELECT 
            nameOrig as client_id,
            COUNT(*) as txn_count,
            CAST(SUM(amount) AS DECIMAL(18,2)) as total_volume,
            CAST(AVG(amount) AS DECIMAL(18,2)) as avg_amount,
            'Structuring' as alert_type,
            80 as risk_score
        FROM transactions
        WHERE type IN ('CASH_OUT', 'TRANSFER', 'PAYMENT')
          AND amount < 50000
        GROUP BY nameOrig
        HAVING COUNT(*) >= 3
           AND SUM(amount) > 10000
        ORDER BY total_volume DESC
        LIMIT 50
    """
    
    try:
        alerts_df = conn.execute(query).df()
        
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
        
        conn.execute("DELETE FROM alerts WHERE alert_type = 'Structuring'")
        
        if not alerts_df.empty:
            conn.execute("INSERT INTO alerts SELECT * FROM alerts_df")
            print(f"[SUCCESS] Structuring Alerts Found: {len(alerts_df)}")
            print(alerts_df.head(5))
        else:
            print("[WARNING] No structuring patterns detected.")
        
    except Exception as e:
        print(f"[ERROR] Executing rule: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    detect_structuring()
