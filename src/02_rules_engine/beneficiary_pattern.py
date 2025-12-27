

"""
Beneficiary Rotation Detection Rule
Detects frequent changes in transaction recipients
"""

import duckdb

def detect_beneficiary_rotation():
    """
    Detect beneficiary rotation patterns
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
    
    # PARAMETROS MAS FLEXIBLES: >= 5 beneficiaries
    query = """
    WITH beneficiary_count AS (
        SELECT 
            nameOrig as customer_id,
            COUNT(DISTINCT nameDest) as unique_beneficiaries,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM transactions
        WHERE type IN ('TRANSFER', 'PAYMENT')
          AND nameDest IS NOT NULL
          AND nameDest != ''
        GROUP BY nameOrig
        HAVING COUNT(DISTINCT nameDest) >= 5
           AND COUNT(*) >= 5
    )
    INSERT INTO rule_alerts (alert_id, customer_id, rule_name, detection_date, amount, description)
    SELECT 
        ROW_NUMBER() OVER () + ? as alert_id,
        customer_id,
        'Beneficiary_Rotation' as rule_name,
        CURRENT_DATE as detection_date,
        total_amount,
        'Multiple recipients: ' || unique_beneficiaries || ' different beneficiaries in ' || tx_count || ' transactions'
    FROM beneficiary_count
    LIMIT 50
    """
    
    conn.execute(query, [max_id])
    
    count = conn.execute("""
        SELECT COUNT(*) FROM rule_alerts 
        WHERE rule_name = 'Beneficiary_Rotation'
    """).fetchone()[0]
    
    print(f"[INFO] Beneficiary Rotation: {count} alerts generated")
    
    conn.close()

if __name__ == "__main__":
    detect_beneficiary_rotation()
