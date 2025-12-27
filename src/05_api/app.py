


"""
AML Monitoring Engine - REST API
Exposes detection capabilities via HTTP endpoints for system integration.

Use Cases:
- Integration with case management systems
- Real-time transaction scoring from payment gateways
- Webhook notifications for downstream services
- Investigator portal backend
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import duckdb
import os
import sys

# Add ML scoring to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '03_ml_scoring'))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_FILE = os.path.join(BASE_DIR, "data", "fraud_data.duckdb")

app = Flask(__name__)
CORS(app)

def get_db():
    """Get database connection"""
    return duckdb.connect(DB_FILE, read_only=True)

@app.route('/api/v1/health', methods=['GET'])
def health_check():
    """
    Health check endpoint for monitoring systems.
    
    Response:
    {
        "status": "healthy",
        "database": "connected",
        "version": "1.0.0"
    }
    """
    try:
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        db_status = "connected"
    except:
        db_status = "disconnected"
    
    return jsonify({
        "status": "healthy" if db_status == "connected" else "degraded",
        "database": db_status,
        "version": "1.0.0"
    })

@app.route('/api/v1/alerts', methods=['GET'])
def get_alerts():
    """
    Retrieve all alerts from rules engine.
    
    Query Parameters:
    - alert_type: Filter by specific alert type
    - min_risk_score: Minimum risk score threshold
    - limit: Maximum number of results (default 100)
    
    Response:
    {
        "count": 10,
        "alerts": [...]
    }
    """
    alert_type = request.args.get('alert_type')
    min_risk_score = request.args.get('min_risk_score', type=int, default=0)
    limit = request.args.get('limit', type=int, default=100)
    
    conn = get_db()
    
    query = "SELECT * FROM alerts WHERE risk_score >= ?"
    params = [min_risk_score]
    
    if alert_type:
        query += " AND alert_type = ?"
        params.append(alert_type)
    
    query += f" ORDER BY risk_score DESC LIMIT {limit}"
    
    try:
        alerts = conn.execute(query, params).df().to_dict('records')
        conn.close()
        
        return jsonify({
            "count": len(alerts),
            "alerts": alerts
        })
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/alerts/ml', methods=['GET'])
def get_ml_alerts():
    """
    Retrieve ML anomaly detection alerts.
    
    Query Parameters:
    - limit: Maximum number of results (default 50)
    
    Response:
    {
        "count": 244,
        "anomalies": [...]
    }
    """
    limit = request.args.get('limit', type=int, default=50)
    
    conn = get_db()
    
    try:
        query = f"SELECT * FROM ml_alerts ORDER BY anomaly_score ASC LIMIT {limit}"
        anomalies = conn.execute(query).df().to_dict('records')
        conn.close()
        
        return jsonify({
            "count": len(anomalies),
            "anomalies": anomalies
        })
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/customer/<client_id>', methods=['GET'])
def get_customer_profile(client_id):
    """
    Get detailed profile for a specific customer.
    
    Parameters:
    - client_id: Customer identifier
    
    Response:
    {
        "client_id": "C363736674",
        "profile": {...},
        "alerts": [...]
    }
    """
    conn = get_db()
    
    try:
        # Customer profile
        profile_query = """
            SELECT 
                nameOrig as client_id,
                COUNT(*) as total_transactions,
                CAST(SUM(amount) AS DECIMAL(18,2)) as total_volume,
                CAST(AVG(amount) AS DECIMAL(18,2)) as avg_amount,
                COUNT(DISTINCT type) as transaction_types,
                COUNT(DISTINCT nameDest) as unique_recipients
            FROM transactions
            WHERE nameOrig = ?
            GROUP BY nameOrig
        """
        profile = conn.execute(profile_query, [client_id]).df()
        
        if profile.empty:
            conn.close()
            return jsonify({"error": "Customer not found"}), 404
        
        # Associated alerts
        alerts_query = "SELECT * FROM alerts WHERE client_id = ?"
        alerts = conn.execute(alerts_query, [client_id]).df().to_dict('records')
        
        conn.close()
        
        return jsonify({
            "client_id": client_id,
            "profile": profile.to_dict('records')[0],
            "alerts": alerts,
            "alert_count": len(alerts)
        })
    
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/v1/stats', methods=['GET'])
def get_statistics():
    """
    Get overall system statistics and metrics.
    
    Response:
    {
        "total_transactions": 6362620,
        "total_alerts": 245,
        "alert_rate": 0.004,
        ...
    }
    """
    conn = get_db()
    
    try:
        stats = {}
        
        stats['total_transactions'] = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        stats['total_rule_alerts'] = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        stats['total_ml_alerts'] = conn.execute("SELECT COUNT(*) FROM ml_alerts").fetchone()[0]
        stats['high_risk_alerts'] = conn.execute("SELECT COUNT(*) FROM alerts WHERE risk_score >= 80").fetchone()[0]
        
        total_alerts = stats['total_rule_alerts'] + stats['total_ml_alerts']
        stats['alert_rate'] = round(total_alerts / stats['total_transactions'] * 100, 4) if stats['total_transactions'] > 0 else 0
        
        conn.close()
        
        return jsonify(stats)
    
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    """
    Development server (not for production):
    python src/05_api/app.py
    
    Production deployment:
    - Gunicorn: gunicorn -w 4 -b 0.0.0.0:8000 src.05_api.app:app
    - Docker: Containerize with nginx reverse proxy
    - AWS ECS/Lambda: Serverless deployment with API Gateway
    """
    print("=" * 60)
    print("AML Monitoring Engine - REST API")
    print("=" * 60)
    print("Endpoints available:")
    print("  GET  /api/v1/health")
    print("  GET  /api/v1/alerts")
    print("  GET  /api/v1/alerts/ml")
    print("  GET  /api/v1/customer/<client_id>")
    print("  GET  /api/v1/stats")
    print("=" * 60)
    print("\nStarting development server on http://localhost:5000")
    print("Press CTRL+C to stop\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
