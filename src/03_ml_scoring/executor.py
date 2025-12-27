


"""
ML Scoring Engine Orchestrator
Builds customer profiles and trains anomaly detection models.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from baseline import build_customer_baselines
from anomaly_detection import train_anomaly_model, score_anomalies

def run_ml_pipeline():
    """
    Execute the complete ML pipeline:
    1. Build customer behavioral baselines
    2. Train Isolation Forest model
    3. Score transactions for anomalies
    """
    print("=" * 60)
    print("ML SCORING ENGINE - PIPELINE EXECUTION")
    print("=" * 60)
    
    steps = [
        ("Customer Baseline Builder", build_customer_baselines),
        ("Anomaly Model Training", train_anomaly_model),
        ("Anomaly Scoring", score_anomalies)
    ]
    
    for step_name, step_func in steps:
        print(f"\n[STEP] {step_name}")
        print("-" * 60)
        try:
            step_func()
        except Exception as e:
            print(f"[CRITICAL ERROR] Step '{step_name}' failed: {e}")
            return
        print("-" * 60)
    
    print("\n" + "=" * 60)
    print("[COMPLETED] ML Pipeline executed successfully.")
    print("=" * 60)

if __name__ == "__main__":
    run_ml_pipeline()
