


import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")

def detect_beneficiary_rotation():
    """
    RULE #4: Beneficiary Rotation Detection
    
    Logic:
    Frequent changes in transaction recipients suggest layering activities
    common in money laundering schemes.
    
    Criteria:
    - Transaction types: TRANSFER or PAYMENT
    - Client sends to 3 or more unique recipients
    - Minimum 3 total transactions
    """
    print("[INFO] Running Rule: Beneficiary Rotation Detection...")
    
    conn = duckdb.connect(DB_FILE)
    
    query = """
        SELECT 
            nameOrig as client_id,
            COUNT(*) as txn_count,
            COUNT(DISTINCT nameDest) as unique_recipients,
            CAST(SUM(amount) AS DECIMAL(18,2)) as total_volume,
            'Beneficiary_Rotation' as alert_type,
            70 as risk_score
        FROM transactions
        WHERE type IN ('TRANSFER', 'PAYMENT')
        GROUP BY nameOrig
        HAVING COUNT(DISTINCT nameDest) >= 3
           AND COUNT(*) >= 3
        ORDER BY unique_recipients DESC
        LIMIT 50
    """
    
    try:
        alerts_df = conn.execute(query).df()
        
        if not alerts_df.empty:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    client_id VARCHAR, 
                    txn_count INTEGER,
                    unique_recipients INTEGER,
                    total_volume DECIMAL, 
                    alert_type VARCHAR, 
                    risk_score INTEGER
                )
            """)
            
            conn.execute("DELETE FROM alerts WHERE alert_type = 'Beneficiary_Rotation'")
            conn.execute("INSERT INTO alerts SELECT * FROM alerts_df")
            
            print(f"[SUCCESS] Beneficiary Rotation Alerts Found: {len(alerts_df)}")
            print(alerts_df.head(5))
        else:
            print("[WARNING] No beneficiary rotation patterns detected.")
    
    except Exception as e:
        print(f"[ERROR] Beneficiary rotation rule failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    detect_beneficiary_rotation()
