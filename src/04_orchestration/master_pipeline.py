


"""
Master Orchestration Pipeline
Executes the complete AML monitoring workflow in sequence.

Execution Order:
1. ETL: Load transaction data into DuckDB
2. Rules Engine: Execute all SQL-based detection rules
3. ML Pipeline: Build baselines and train/score anomaly detection
4. Summary Report: Generate execution metrics

Production Use:
This script would be scheduled (cron/Airflow) to run:
- Daily: For new transaction batches
- Weekly: For full reprocessing with model retraining
"""

import sys
import os
from datetime import datetime

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '01_etl'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '02_rules_engine'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '03_ml_scoring'))

from load_data import load_data
from executor import run_all_rules
from executor import run_ml_pipeline

import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")

def generate_summary_report():
    """
    Generate execution summary with key metrics.
    In production, this would be sent to monitoring dashboards or email.
    """
    print("\n" + "=" * 60)
    print("EXECUTION SUMMARY REPORT")
    print("=" * 60)
    
    conn = duckdb.connect(DB_FILE, read_only=True)
    
    try:
        # Total transactions in database
        total_txns = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        print(f"Total Transactions in Database: {total_txns:,}")
        
        # Rule-based alerts
        rule_alerts = conn.execute("SELECT COUNT(*), alert_type FROM alerts GROUP BY alert_type").fetchall()
        print("\nRule-Based Alerts:")
        total_rule_alerts = 0
        for count, alert_type in rule_alerts:
            print(f"  - {alert_type}: {count}")
            total_rule_alerts += count
        print(f"  Total: {total_rule_alerts}")
        
        # ML alerts
        ml_alerts_count = conn.execute("SELECT COUNT(*) FROM ml_alerts").fetchone()[0]
        print(f"\nML Anomaly Alerts: {ml_alerts_count}")
        
        # Overall alert rate
        total_alerts = total_rule_alerts + ml_alerts_count
        alert_rate = (total_alerts / total_txns * 100) if total_txns > 0 else 0
        print(f"\nOverall Alert Rate: {alert_rate:.3f}%")
        
        # High risk alerts
        high_risk = conn.execute("SELECT COUNT(*) FROM alerts WHERE risk_score >= 80").fetchone()[0]
        print(f"High Risk Alerts (score >= 80): {high_risk}")
        
    except Exception as e:
        print(f"[WARNING] Could not generate full summary: {e}")
    finally:
        conn.close()
    
    print("=" * 60)

def run_master_pipeline(skip_etl=False):
    """
    Execute the complete AML monitoring pipeline.
    
    Args:
        skip_etl: If True, skips data loading (useful when data already exists)
    """
    start_time = datetime.now()
    
    print("=" * 60)
    print("AML MONITORING ENGINE - MASTER PIPELINE")
    print(f"Execution started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    steps = []
    
    if not skip_etl:
        steps.append(("ETL - Data Loading", load_data))
    
    steps.extend([
        ("Rules Engine - SQL Detection", run_all_rules),
        ("ML Pipeline - Anomaly Detection", run_ml_pipeline),
        ("Summary Report Generation", generate_summary_report)
    ])
    
    for i, (step_name, step_func) in enumerate(steps, 1):
        print(f"\n{'=' * 60}")
        print(f"STEP {i}/{len(steps)}: {step_name}")
        print("=" * 60)
        
        step_start = datetime.now()
        
        try:
            step_func()
            step_duration = (datetime.now() - step_start).total_seconds()
            print(f"\n[SUCCESS] {step_name} completed in {step_duration:.2f}s")
        except Exception as e:
            print(f"\n[CRITICAL ERROR] {step_name} failed: {e}")
            print("[ABORT] Pipeline execution stopped.")
            return
    
    total_duration = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print(f"Total execution time: {total_duration:.2f} seconds")
    print("=" * 60)

if __name__ == "__main__":
    """
    Execution modes:
    
    1. Full pipeline (first run):
       python src/04_orchestration/master_pipeline.py
    
    2. Skip ETL (data already loaded):
       Modify run_master_pipeline(skip_etl=True) below
    
    Production scheduling examples:
    - Cron: 0 2 * * * cd /opt/aml-engine && python src/04_orchestration/master_pipeline.py
    - Airflow: Define DAG with task dependencies
    - AWS Batch: Containerized job triggered by EventBridge
    """
    
    # For demo: Skip ETL since data is already loaded
    # Set skip_etl=False if running on a fresh database
    run_master_pipeline(skip_etl=True)
