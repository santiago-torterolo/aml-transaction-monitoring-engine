


"""
Round Amount Detection Rule
Detects suspicious round number patterns
"""

import duckdb

def detect_round_amounts():
    """
    Detect suspicious use of round amounts
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
    
    # PARAMETROS MAS FLEXIBLES: 100000 divisible por 100000
    query = """
    WITH round_amounts AS (
        SELECT 
            nameOrig as customer_id,
            amount,
            step
        FROM transactions
        WHERE amount % 100000 = 0
          AND amount >= 100000
          AND type IN ('TRANSFER', 'CASH_OUT')
    )
    INSERT INTO rule_alerts (alert_id, customer_id, rule_name, detection_date, amount, description)
    SELECT 
        ROW_NUMBER() OVER () + ? as alert_id,
        customer_id,
        'Round_Amount_Pattern' as rule_name,
        CURRENT_DATE as detection_date,
        amount,
        'Suspicious exact round amount: $' || ROUND(amount, 2)
    FROM round_amounts
    LIMIT 50
    """
    
    conn.execute(query, [max_id])
    
    count = conn.execute("""
        SELECT COUNT(*) FROM rule_alerts 
        WHERE rule_name = 'Round_Amount_Pattern'
    """).fetchone()[0]
    
    print(f"[INFO] Round Amounts: {count} alerts generated")
    
    conn.close()

if __name__ == "__main__":
    detect_round_amounts()
