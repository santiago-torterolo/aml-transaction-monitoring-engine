


"""
Velocity Check Rule
Detects rapid transaction sequences
"""

import duckdb

def detect_velocity_abuse():
    """
    Detect suspicious velocity patterns
    """
    
    conn = duckdb.connect('data/fraud_data.duckdb')
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rule_alerts (
            alert_id INTEGER PRIMARY KEY,
            customer_id VARCHAR,
            rule_name VARCHAR,
            detection_date DATE,
            amount DECIMAL(18,2),
            description TEXT
        )
    """)
    
    max_id = conn.execute("SELECT COALESCE(MAX(alert_id), 0) FROM rule_alerts").fetchone()[0]
    
    # PARAMETROS MAS FLEXIBLES: amount > 100000 (antes 50000)
    query = """
    WITH velocity_check AS (
        SELECT 
            nameOrig as customer_id,
            step,
            amount,
            LAG(step) OVER (PARTITION BY nameOrig ORDER BY step) as prev_step,
            step - LAG(step) OVER (PARTITION BY nameOrig ORDER BY step) as time_diff
        FROM transactions
        WHERE type IN ('CASH_OUT', 'TRANSFER')
          AND amount > 100000
    )
    INSERT INTO rule_alerts (alert_id, customer_id, rule_name, detection_date, amount, description)
    SELECT 
        ROW_NUMBER() OVER () + ? as alert_id,
        customer_id,
        'Velocity_Abuse' as rule_name,
        CURRENT_DATE as detection_date,
        amount,
        'Suspicious velocity: Transaction of $' || ROUND(amount, 2) || ' within ' || time_diff || ' steps'
    FROM velocity_check
    WHERE time_diff <= 2
    LIMIT 50
    """
    
    conn.execute(query, [max_id])
    
    count = conn.execute("""
        SELECT COUNT(*) FROM rule_alerts 
        WHERE rule_name = 'Velocity_Abuse'
    """).fetchone()[0]
    
    print(f"[INFO] Velocity Check: {count} alerts generated")
    
    conn.close()

if __name__ == "__main__":
    detect_velocity_abuse()
