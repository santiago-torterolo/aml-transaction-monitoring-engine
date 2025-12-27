


"""
ML Scoring Executor
Orchestrates baseline creation and anomaly detection
"""

import sys
import os

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

print("\n" + "="*60)
print("ML SCORING EXECUTOR")
print("="*60 + "\n")

# ============================================
# PHASE 1: CREATE BASELINES
# ============================================
print("[INFO] Creating customer baselines...")
try:
    from baseline import create_baselines
    create_baselines()
    print("[SUCCESS] Baselines created")
except Exception as e:
    print(f"[ERROR] Baseline creation failed: {str(e)}")

print()

# ============================================
# PHASE 2: ANOMALY DETECTION
# ============================================
print("[INFO] Running anomaly detection...")
try:
    from anomaly_detection import train_and_score
    train_and_score()
    print("[SUCCESS] Anomaly detection completed")
except Exception as e:
    print(f"[ERROR] Anomaly detection failed: {str(e)}")

print()

# ============================================
# SUMMARY
# ============================================
print("="*60)
print("ML SCORING SUMMARY")
print("="*60)

try:
    import duckdb
    conn = duckdb.connect('data/fraud_data.duckdb', read_only=True)
    
    result = conn.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN anomaly_score >= 0.8 THEN 1 ELSE 0 END) as critical,
            SUM(CASE WHEN anomaly_score >= 0.6 THEN 1 ELSE 0 END) as high,
            SUM(CASE WHEN anomaly_score >= 0.5 THEN 1 ELSE 0 END) as medium
        FROM ml_scores
    """).fetchone()
    
    print(f"Total Scored: {result[0]:,}")
    print(f"Critical Risk (>= 0.8): {result[1]}")
    print(f"High Risk (>= 0.6): {result[2]}")
    print(f"Medium Risk (>= 0.5): {result[3]}")
    
    conn.close()
    
except Exception as e:
    print(f"[WARNING] Could not generate summary: {str(e)}")

print("="*60 + "\n")
