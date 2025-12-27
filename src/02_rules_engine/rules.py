


"""
Structuring Detection Rule
Detects multiple small transactions below reporting threshold
"""

import duckdb

def detect_structuring():
    """
    Detect potential structuring (smurfing) patterns
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
    
    # PARAMETROS MAS FLEXIBLES: >= 2 transacciones, total > 5000
    query = """
    WITH structuring_candidates AS (
        SELECT 
            nameOrig as customer_id,
            step,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM transactions
        WHERE amount < 50000
          AND amount > 1000
          AND type IN ('CASH_OUT', 'TRANSFER')
        GROUP BY nameOrig, step
        HAVING COUNT(*) >= 2
           AND SUM(amount) > 5000
    )
    INSERT INTO rule_alerts (alert_id, customer_id, rule_name, detection_date, amount, description)
    SELECT 
        ROW_NUMBER() OVER () + ? as alert_id,
        customer_id,
        'Structuring_Detection' as rule_name,
        CURRENT_DATE as detection_date,
        total_amount as amount,
        'Potential structuring: ' || tx_count || ' transactions totaling $' || 
        ROUND(total_amount, 2) || ' (avg: $' || ROUND(avg_amount, 2) || ')'
    FROM structuring_candidates
    LIMIT 50
    """
    
    conn.execute(query, [max_id])
    
    count = conn.execute("""
        SELECT COUNT(*) FROM rule_alerts 
        WHERE rule_name = 'Structuring_Detection'
    """).fetchone()[0]
    
    print(f"[INFO] Structuring Detection: {count} alerts generated")
    
    conn.close()

if __name__ == "__main__":
    detect_structuring()
