


import duckdb
import pandas as pd
import pickle
import os
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")
MODEL_PATH = os.path.join(BASE_DIR, "data", "isolation_forest.pkl")

def train_anomaly_model():
    """
    ML COMPONENT: Unsupervised Anomaly Detection
    Algorithm: Isolation Forest
    
    Purpose:
    Detect transactions that are statistically unusual compared to the overall 
    transaction population, without requiring labeled fraud data.
    
    Features:
    - Transaction amount
    - Balance changes (origin and destination)
    - Time step (temporal patterns)
    
    Output:
    Trained model saved to disk for scoring new transactions.
    """
    print("[INFO] Training Isolation Forest model...")
    
    conn = duckdb.connect(DB_FILE)
    
    print("[INFO] Loading transaction sample for training...")
    query = """
        SELECT 
            amount,
            oldbalanceOrg,
            newbalanceOrig,
            oldbalanceDest,
            newbalanceDest,
            step
        FROM transactions
        USING SAMPLE 10 PERCENT
    """
    df = conn.execute(query).df()
    print(f"[INFO] Training sample size: {len(df):,} records")
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df)
    
    print("[INFO] Fitting Isolation Forest (contamination=0.005)...")
    iso_forest = IsolationForest(
        n_estimators=100, 
        contamination=0.005,
        random_state=42, 
        n_jobs=-1,
        verbose=0
    )
    iso_forest.fit(X_scaled)
    
    with open(MODEL_PATH, 'wb') as f:
        pickle.dump({'model': iso_forest, 'scaler': scaler, 'features': df.columns.tolist()}, f)
    
    print(f"[SUCCESS] Model trained and saved to {MODEL_PATH}")
    conn.close()

def score_anomalies():
    """
    Apply trained Isolation Forest to detect anomalies in transaction population.
    Anomaly scores are added to the alerts table for further investigation.
    """
    print("[INFO] Scoring transactions for anomalies...")
    
    if not os.path.exists(MODEL_PATH):
        print("[ERROR] Model file not found. Run training first.")
        return

    with open(MODEL_PATH, 'rb') as f:
        artifacts = pickle.load(f)
        model = artifacts['model']
        scaler = artifacts['scaler']
        feature_cols = artifacts['features']
    
    conn = duckdb.connect(DB_FILE)
    
    print("[INFO] Loading transactions to score (sample 50k for demo)...")
    query = f"""
        SELECT 
            nameOrig as client_id,
            {', '.join(feature_cols)}
        FROM transactions
        LIMIT 50000
    """
    df = conn.execute(query).df()
    
    X = df[feature_cols]
    X_scaled = scaler.transform(X)
    
    df['anomaly_score'] = model.decision_function(X_scaled)
    df['is_anomaly'] = model.predict(X_scaled)
    
    anomalies = df[df['is_anomaly'] == -1].copy()
    anomalies['alert_type'] = 'ML_Anomaly'
    anomalies['risk_score'] = 85
    
    if not anomalies.empty:
        anomalies_to_save = anomalies[['client_id', 'anomaly_score', 'alert_type', 'risk_score']]
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ml_alerts (
                client_id VARCHAR,
                anomaly_score DOUBLE,
                alert_type VARCHAR,
                risk_score INTEGER
            )
        """)
        
        conn.execute("DELETE FROM ml_alerts WHERE alert_type = 'ML_Anomaly'")
        conn.execute("INSERT INTO ml_alerts SELECT * FROM anomalies_to_save")
        
        print(f"[SUCCESS] Anomalies detected: {len(anomalies)} out of {len(df)}")
        print("\n[INFO] Top 5 Most Anomalous Transactions:")
        print(anomalies.nsmallest(5, 'anomaly_score')[['client_id', 'amount', 'anomaly_score']])
    else:
        print("[WARNING] No anomalies detected with current threshold.")
    
    conn.close()

if __name__ == "__main__":
    train_anomaly_model()
    score_anomalies()
