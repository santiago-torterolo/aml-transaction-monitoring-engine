

"""
Master Pipeline Orchestrator
Executes the complete AML detection workflow
"""

import sys
import os
from datetime import datetime

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

print("\n" + "="*60)
print("AML TRANSACTION MONITORING ENGINE")
print("="*60)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60 + "\n")

# ============================================
# PHASE 1: RULES ENGINE
# ============================================
print("PHASE 1: Executing Rules Engine...")
print("-" * 60)

try:
    # Import rules executor
    rules_executor_path = os.path.join(project_root, 'src', '02_rules_engine', 'executor.py')
    
    # Execute using subprocess to avoid import conflicts
    import subprocess
    result = subprocess.run(
        [sys.executable, rules_executor_path],
        capture_output=True,
        text=True,
        cwd=project_root
    )
    
    if result.returncode == 0:
        print("[SUCCESS] Rules Engine completed successfully")
        if result.stdout:
            print(result.stdout)
    else:
        print("[ERROR] Rules Engine failed")
        if result.stderr:
            print(result.stderr)
        sys.exit(1)
        
except Exception as e:
    print(f"[ERROR] Error executing Rules Engine: {str(e)}")
    sys.exit(1)

print()

# ============================================
# PHASE 2: ML SCORING
# ============================================
print("PHASE 2: Executing ML Scoring...")
print("-" * 60)

try:
    # Import ML executor
    ml_executor_path = os.path.join(project_root, 'src', '03_ml_scoring', 'executor.py')
    
    # Execute using subprocess
    result = subprocess.run(
        [sys.executable, ml_executor_path],
        capture_output=True,
        text=True,
        cwd=project_root
    )
    
    if result.returncode == 0:
        print("[SUCCESS] ML Scoring completed successfully")
        if result.stdout:
            print(result.stdout)
    else:
        print("[ERROR] ML Scoring failed")
        if result.stderr:
            print(result.stderr)
        sys.exit(1)
        
except Exception as e:
    print(f"[ERROR] Error executing ML Scoring: {str(e)}")
    sys.exit(1)

print()

# ============================================
# PHASE 3: SUMMARY
# ============================================
print("="*60)
print("PIPELINE SUMMARY")
print("="*60)

try:
    import duckdb
    
    conn = duckdb.connect('data/fraud_data.duckdb', read_only=True)
    
    # Get statistics
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_transactions,
            (SELECT COUNT(*) FROM rule_alerts) as rule_alerts,
            (SELECT COUNT(*) FROM ml_scores WHERE anomaly_score >= 0.5) as ml_alerts
        FROM transactions
    """).fetchdf()
    
    total_tx = stats['total_transactions'].iloc[0]
    rule_alerts = stats['rule_alerts'].iloc[0]
    ml_alerts = stats['ml_alerts'].iloc[0]
    alert_rate = ((rule_alerts + ml_alerts) / total_tx * 100)
    
    print(f"Total Transactions: {total_tx:,}")
    print(f"Rule-based Alerts: {rule_alerts}")
    print(f"ML Anomalies: {ml_alerts}")
    print(f"Alert Rate: {alert_rate:.4f}%")
    
    conn.close()
    
except Exception as e:
    print(f"[WARNING] Could not generate summary: {str(e)}")

print("="*60)
print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("[SUCCESS] Pipeline completed successfully!")
print("="*60 + "\n")

print("NEXT STEPS:")
print("  - Launch API: python src/05_api/app.py")
print("  - Launch Dashboard: streamlit run src/06_dashboard/app.py")
print()
