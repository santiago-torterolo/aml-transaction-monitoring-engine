

"""
Anomaly Detection using Isolation Forest
"""

import duckdb
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pickle

def train_and_score():
    """
    Train Isolation Forest and score all transactions
    """
    
    conn = duckdb.connect('data/fraud_data.duckdb')
    
    print("[INFO] Loading transaction data...")
    
    # Use valid columns and sampling
    df_sample = conn.execute("""
        SELECT 
            amount,
            oldbalanceOrg,
            newbalanceOrig,
            oldbalanceDest,
            newbalanceDest,
            CASE type
                WHEN 'PAYMENT' THEN 1
                WHEN 'TRANSFER' THEN 2
                WHEN 'CASH_OUT' THEN 3
                WHEN 'DEBIT' THEN 4
                WHEN 'CASH_IN' THEN 5
                ELSE 0
            END as type_encoded
        FROM transactions
        WHERE amount > 0
        LIMIT 50000
    """).fetchdf()
    
    print(f"[INFO] Training sample: {len(df_sample):,} transactions")
    
    # Prepare features
    features = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 
                'oldbalanceDest', 'newbalanceDest', 'type_encoded']
    
    X_train = df_sample[features].fillna(0)
    
    # Normalize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_train)
    
    # Train Isolation Forest
    print("[INFO] Training Isolation Forest...")
    iso_forest = IsolationForest(
        contamination=0.005,
        random_state=42,
        n_estimators=100,
        max_samples=0.8,
        n_jobs=-1
    )
    
    iso_forest.fit(X_scaled)
    
    # Save model
    with open('data/isolation_forest.pkl', 'wb') as f:
        pickle.dump((iso_forest, scaler), f)
    
    print("[INFO] Model saved to data/isolation_forest.pkl")
    
    # Score all transactions
    print("[INFO] Scoring all transactions...")
    
    df_all = conn.execute("""
        SELECT 
            nameOrig,
            step,
            amount,
            oldbalanceOrg,
            newbalanceOrig,
            oldbalanceDest,
            newbalanceDest,
            CASE type
                WHEN 'PAYMENT' THEN 1
                WHEN 'TRANSFER' THEN 2
                WHEN 'CASH_OUT' THEN 3
                WHEN 'DEBIT' THEN 4
                WHEN 'CASH_IN' THEN 5
                ELSE 0
            END as type_encoded
        FROM transactions
        WHERE amount > 0
    """).fetchdf()
    
    X_all = df_all[features].fillna(0)
    X_all_scaled = scaler.transform(X_all)
    
    # Get anomaly scores
    scores = iso_forest.score_samples(X_all_scaled)
    
    # Normalize to 0-1 range (lower score = more anomalous)
    # IsolationForest.score_samples returns the opposite of the anomaly score (lower is more anomalous)
    # We'll normalize it so higher is more anomalous for the summary
    scores_normalized = 1 / (1 + np.exp(scores))
    
    df_all['anomaly_score'] = scores_normalized
    
    # Create ml_scores table (using nameOrig and step as a composite key of sorts for demo)
    conn.execute("DROP TABLE IF EXISTS ml_scores")
    
    conn.execute("""
        CREATE TABLE ml_scores (
            customer_id VARCHAR,
            step INTEGER,
            anomaly_score DOUBLE
        )
    """)
    
    # Insert scores
    conn.execute("""
        INSERT INTO ml_scores 
        SELECT nameOrig, step, anomaly_score 
        FROM df_all
    """)
    
    # Get statistics
    high_risk = len(df_all[df_all['anomaly_score'] >= 0.5])
    
    print(f"[INFO] Scored {len(df_all):,} transactions")
    print(f"[INFO] High risk anomalies (>= 0.5): {high_risk}")
    
    conn.close()

if __name__ == "__main__":
    train_and_score()
