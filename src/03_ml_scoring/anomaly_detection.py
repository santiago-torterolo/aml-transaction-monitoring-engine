

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
    
    # Sample 10% for training (usando step como identificador)
    df_sample = conn.execute("""
        SELECT 
            step,
            nameOrig,
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
            END as type_encoded,
            ROW_NUMBER() OVER () as row_id
        FROM transactions
        WHERE amount > 0
    """).fetchdf()
    
    # Sample 10%
    df_sample = df_sample[df_sample['row_id'] % 10 == 0].copy()
    
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
            step,
            nameOrig,
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
            END as type_encoded,
            ROW_NUMBER() OVER () as row_id
        FROM transactions
        WHERE amount > 0
    """).fetchdf()
    
    X_all = df_all[features].fillna(0)
    X_all_scaled = scaler.transform(X_all)
    
    # Get anomaly predictions
    predictions = iso_forest.predict(X_all_scaled)
    
    # Get decision scores
    decision_scores = iso_forest.decision_function(X_all_scaled)
    
    # Filter only anomalies
    df_anomalies = df_all[predictions == -1].copy()
    anomaly_decision_scores = decision_scores[predictions == -1]
    
    # Normalize anomaly scores to 0-1
    if len(df_anomalies) > 0:
        min_score = anomaly_decision_scores.min()
        max_score = anomaly_decision_scores.max()
        
        if max_score != min_score:
            normalized_scores = (max_score - anomaly_decision_scores) / (max_score - min_score)
        else:
            normalized_scores = np.ones(len(df_anomalies)) * 0.5
        
        df_anomalies['anomaly_score'] = normalized_scores
    
    # Create ml_scores table
    conn.execute("DROP TABLE IF EXISTS ml_scores")
    
    conn.execute("""
        CREATE TABLE ml_scores (
            row_id INTEGER PRIMARY KEY,
            step INTEGER,
            customer_id VARCHAR,
            anomaly_score DOUBLE
        )
    """)
    
    # Insert only anomalies
    if len(df_anomalies) > 0:
        for _, row in df_anomalies.iterrows():
            conn.execute("""
                INSERT INTO ml_scores VALUES (?, ?, ?, ?)
            """, [int(row['row_id']), int(row['step']), row['nameOrig'], float(row['anomaly_score'])])
    
    # Get statistics
    high_risk = (df_anomalies['anomaly_score'] >= 0.7).sum() if len(df_anomalies) > 0 else 0
    medium_risk = (df_anomalies['anomaly_score'] >= 0.5).sum() if len(df_anomalies) > 0 else 0
    
    print(f"[INFO] Scored {len(df_all):,} transactions")
    print(f"[INFO] Anomalies detected: {len(df_anomalies)} ({len(df_anomalies)/len(df_all)*100:.4f}%)")
    print(f"[INFO] High risk (score >= 0.7): {high_risk}")
    print(f"[INFO] Medium risk (score >= 0.5): {medium_risk}")
    
    conn.close()

if __name__ == "__main__":
    train_and_score()
