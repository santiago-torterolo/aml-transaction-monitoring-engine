


"""
Daily Anomaly Scoring Module

Purpose:
This script is designed for PRODUCTION ENVIRONMENTS where new transactions 
arrive continuously (e.g., payment systems, banking platforms, e-commerce).

Usage Pattern:
- TRAINING: Executed once initially, then periodically (weekly/monthly)
- SCORING: Executed DAILY (or in real-time batch) on new transactions

In this demo project, we use a static PaySim dataset, so we score a sample.
In a real system, this would score transactions from the last 24 hours.

Production Deployment Example:
- Cron job: Execute daily at 2:00 AM
- Airflow DAG: Scheduled batch pipeline
- Real-time: Kafka stream processing with model serving
"""

import duckdb
import pandas as pd
import pickle
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")
MODEL_PATH = os.path.join(BASE_DIR, "data", "isolation_forest.pkl")

def score_new_transactions():
    """
    Score new transactions using pre-trained Isolation Forest model.
    
    Production Behavior:
    In a real payment system, this function would:
    1. Query transactions from the last 24 hours (WHERE created_at >= NOW() - INTERVAL 1 DAY)
    2. Apply the trained model to detect anomalies
    3. Write alerts to a monitoring dashboard or notification system
    4. Return metrics for observability (alert count, processing time)
    
    Demo Behavior:
    Since PaySim is static, we score a random sample to simulate daily processing.
    """
    print(f"[INFO] Daily Scoring Job - Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not os.path.exists(MODEL_PATH):
        print("[ERROR] Trained model not found. Please run training first.")
        print("       Execute: python src/03_ml_scoring/executor.py")
        return

    print("[INFO] Loading pre-trained Isolation Forest model...")
    with open(MODEL_PATH, 'rb') as f:
        artifacts = pickle.load(f)
        model = artifacts['model']
        scaler = artifacts['scaler']
        feature_cols = artifacts['features']
    
    conn = duckdb.connect(DB_FILE)
    
    # PRODUCTION QUERY (commented example):
    # In a real system with timestamp columns, you would use:
    # WHERE transaction_date >= CURRENT_DATE - INTERVAL 1 DAY
    
    # DEMO QUERY (static dataset):
    # We score a sample to simulate daily batch processing
    print("[INFO] Loading transactions to score...")
    print("       Note: In production, this would query last 24h of data")
    
    query = f"""
        SELECT 
            nameOrig as client_id,
            {', '.join(feature_cols)}
        FROM transactions
        USING SAMPLE 10 PERCENT
        LIMIT 100000
    """
    
    df = conn.execute(query).df()
    print(f"[INFO] Loaded {len(df):,} transactions for scoring")
    
    X = df[feature_cols]
    X_scaled = scaler.transform(X)
    
    df['anomaly_score'] = model.decision_function(X_scaled)
    df['is_anomaly'] = model.predict(X_scaled)
    
    anomalies = df[df['is_anomaly'] == -1].copy()
    anomalies['alert_type'] = 'ML_Anomaly'
    anomalies['risk_score'] = 85
    anomalies['detection_date'] = datetime.now().strftime('%Y-%m-%d')
    
    if not anomalies.empty:
        anomalies_to_save = anomalies[['client_id', 'anomaly_score', 'alert_type', 'risk_score', 'detection_date']]
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ml_alerts (
                client_id VARCHAR,
                anomaly_score DOUBLE,
                alert_type VARCHAR,
                risk_score INTEGER,
                detection_date VARCHAR
            )
        """)
        
        # In production, you would typically APPEND (not DELETE) to maintain history
        # For demo purposes, we replace to keep the database small
        conn.execute("DELETE FROM ml_alerts WHERE alert_type = 'ML_Anomaly'")
        conn.execute("INSERT INTO ml_alerts SELECT * FROM anomalies_to_save")
        
        print(f"[SUCCESS] Anomalies detected: {len(anomalies)} / {len(df)}")
        print(f"          Alert rate: {(len(anomalies)/len(df)*100):.2f}%")
        
        print("\n[INFO] Top 5 Most Suspicious Transactions:")
        top_anomalies = anomalies.nsmallest(5, 'anomaly_score')[['client_id', 'amount', 'anomaly_score', 'risk_score']]
        print(top_anomalies)
        
        # PRODUCTION ACTION (examples):
        # - Send high-risk alerts to Slack/Email
        # - Update Tableau/Looker dashboard
        # - Trigger SAR investigation workflow
        # - Log metrics to Datadog/Prometheus
        
    else:
        print("[INFO] No anomalies detected in this batch.")
        print("      This could indicate: clean transaction period or model needs retraining.")
    
    conn.close()
    print(f"[COMPLETED] Scoring job finished at {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    """
    Execution Modes:
    
    1. MANUAL (Development/Testing):
       python src/03_ml_scoring/scoring_only.py
    
    2. CRON (Linux Production):
       0 2 * * * cd /opt/aml-engine && /usr/bin/python3 src/03_ml_scoring/scoring_only.py >> logs/scoring.log 2>&1
    
    3. AIRFLOW (Orchestrated Pipeline):
       PythonOperator(
           task_id='daily_ml_scoring',
           python_callable=score_new_transactions,
           dag=aml_monitoring_dag
       )
    
    4. AWS Lambda (Serverless):
       Triggered daily by EventBridge schedule
    """
    score_new_transactions()
