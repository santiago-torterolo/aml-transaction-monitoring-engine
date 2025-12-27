


import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")

def detect_round_amounts():
    """
    RULE #3: Round Amount Pattern Detection
    
    Logic:
    Transactions in exact round numbers are statistically unusual and often
    indicate manual structuring or coordinated laundering activities.
    
    Criteria:
    - Amount divisible by 1000 or 5000
    - Minimum amount of $1,000
    - Client performs at least 2 such transactions
    """
    print("[INFO] Running Rule: Round Amount Pattern Detection...")
    
    conn = duckdb.connect(DB_FILE)
    
    query = """
        SELECT 
            nameOrig as client_id,
            COUNT(*) as round_txn_count,
            CAST(SUM(amount) AS DECIMAL(18,2)) as total_volume,
            'Round_Amount' as alert_type,
            65 as risk_score
        FROM transactions
        WHERE (amount % 1000 = 0 OR amount % 5000 = 0)
          AND amount >= 1000
        GROUP BY nameOrig
        HAVING COUNT(*) >= 2
        ORDER BY round_txn_count DESC
        LIMIT 50
    """
    
    try:
        alerts_df = conn.execute(query).df()
        
        if not alerts_df.empty:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    client_id VARCHAR, 
                    round_txn_count INTEGER,
                    total_volume DECIMAL, 
                    alert_type VARCHAR, 
                    risk_score INTEGER
                )
            """)
            
            conn.execute("DELETE FROM alerts WHERE alert_type = 'Round_Amount'")
            conn.execute("INSERT INTO alerts SELECT * FROM alerts_df")
            
            print(f"[SUCCESS] Round Amount Alerts Found: {len(alerts_df)}")
            print(alerts_df.head(5))
        else:
            print("[WARNING] No round amount patterns detected.")
    
    except Exception as e:
        print(f"[ERROR] Round amount rule failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    detect_round_amounts()
