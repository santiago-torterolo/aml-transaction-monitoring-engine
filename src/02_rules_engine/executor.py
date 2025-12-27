


"""
Rules Engine Executor
Orchestrates all SQL-based detection rules
"""

import sys
import os

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

print("\n" + "="*60)
print("RULES ENGINE EXECUTOR")
print("="*60 + "\n")

# ============================================
# EXECUTE ALL RULES
# ============================================

# Rule 1: Structuring Detection
print("[INFO] Executing Structuring Detection...")
try:
    from rules import detect_structuring
    detect_structuring()
    print("[SUCCESS] Structuring detection completed")
except Exception as e:
    print(f"[ERROR] Structuring detection failed: {str(e)}")

print()

# Rule 2: Velocity Check
print("[INFO] Executing Velocity Check...")
try:
    from velocity_rule import detect_velocity_abuse
    detect_velocity_abuse()
    print("[SUCCESS] Velocity check completed")
except Exception as e:
    print(f"[ERROR] Velocity check failed: {str(e)}")

print()

# Rule 3: Round Amounts
print("[INFO] Executing Round Amounts Detection...")
try:
    from round_amounts import detect_round_amounts
    detect_round_amounts()
    print("[SUCCESS] Round amounts detection completed")
except Exception as e:
    print(f"[ERROR] Round amounts detection failed: {str(e)}")

print()

# Rule 4: Beneficiary Rotation
print("[INFO] Executing Beneficiary Rotation Detection...")
try:
    from beneficiary_pattern import detect_beneficiary_rotation
    detect_beneficiary_rotation()
    print("[SUCCESS] Beneficiary rotation detection completed")
except Exception as e:
    print(f"[ERROR] Beneficiary rotation detection failed: {str(e)}")

print()

# ============================================
# SUMMARY
# ============================================
print("="*60)
print("RULES ENGINE SUMMARY")
print("="*60)

try:
    import duckdb
    conn = duckdb.connect('data/fraud_data.duckdb', read_only=True)
    
    result = conn.execute("SELECT COUNT(*) as total FROM rule_alerts").fetchone()
    print(f"Total Alerts Generated: {result[0]}")
    
    by_rule = conn.execute("""
        SELECT rule_name, COUNT(*) as count 
        FROM rule_alerts 
        GROUP BY rule_name
        ORDER BY count DESC
    """).fetchdf()
    
    print("\nAlerts by Rule:")
    for _, row in by_rule.iterrows():
        print(f"  - {row['rule_name']}: {row['count']}")
    
    conn.close()
    
except Exception as e:
    print(f"[WARNING] Could not generate summary: {str(e)}")

print("="*60 + "\n")
