


import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")

def detect_velocity_abuse():
    """
    RULE #2: Velocity Abuse Detection
    
    Logic:
    Detect users performing multiple transactions within short time intervals.
    High frequency activity in brief windows indicates automated fraud or account takeover.
    
    Criteria:
    - Same client performs transactions within 2 hours (step delta <= 2)
    - Minimum 2 rapid transactions detected
    """
    print("[INFO] Running Rule: Velocity Abuse Detection...")
    
    conn = duckdb.connect(DB_FILE)
    
    query = """
        WITH user_txn_sequence AS (
            SELECT 
                nameOrig,
                step,
                amount,
                LAG(step) OVER (PARTITION BY nameOrig ORDER BY step) as prev_step
            FROM transactions
        )
        SELECT 
            nameOrig as client_id,
            COUNT(*) as rapid_txn_count,
            CAST(SUM(amount) AS DECIMAL(18,2)) as total_volume,
            'Velocity_Abuse' as alert_type,
            75 as risk_score
        FROM user_txn_sequence
        WHERE (step - prev_step) <= 2
        GROUP BY nameOrig
        HAVING COUNT(*) >= 2
        ORDER BY rapid_txn_count DESC
        LIMIT 50
    """
    
    try:
        alerts_df = conn.execute(query).df()
        
        if not alerts_df.empty:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    client_id VARCHAR, 
                    rapid_txn_count INTEGER,
                    total_volume DECIMAL, 
                    alert_type VARCHAR, 
                    risk_score INTEGER
                )
            """)
            
            conn.execute("DELETE FROM alerts WHERE alert_type = 'Velocity_Abuse'")
            conn.execute("INSERT INTO alerts SELECT * FROM alerts_df")
            
            print(f"[SUCCESS] Velocity Alerts Found: {len(alerts_df)}")
            print(alerts_df.head(5))
        else:
            print("[WARNING] No velocity abuse patterns detected.")
    
    except Exception as e:
        print(f"[ERROR] Velocity rule execution failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    detect_velocity_abuse()
