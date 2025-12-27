# 🛡️ AML Transaction Monitoring Engine

> **Production-ready Anti-Money Laundering Detection System**  
> _Hybrid approach combining SQL-based rules with Machine Learning_

[![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Analytics-yellow?style=for-the-badge)](https://duckdb.org/)
[![SQL](https://img.shields.io/badge/SQL-Advanced-orange?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.iso.org/standard/63555.html)
[![Scikit-Learn](https://img.shields.io/badge/scikit--learn-ML-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)](https://scikit-learn.org/)
[![Flask](https://img.shields.io/badge/Flask-API-000000?style=for-the-badge&logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)

---

## 📋 Project Overview

This system simulates a **real-world FinTech compliance environment** for detecting suspicious financial activity. It implements a **Hybrid Detection Strategy** that combines:

1. **Rule-Based Engine:** SQL queries targeting known AML typologies (Structuring, Velocity Abuse, Beneficiary Rotation)
2. **Machine Learning:** Unsupervised anomaly detection using Isolation Forests for behavioral analysis
3. **RESTful API:** HTTP endpoints for system integration and real-time scoring
4. **Interactive Dashboard:** Streamlit-based investigator interface

**Key Focus Areas:**

- Scalability (processes 6.3M transactions)
- Explainability (risk scores with reasoning)
- False-positive reduction (precision-focused alerting)

---

## 🏗️ Architecture

The system is built on a **serverless, docker-less data stack** optimized for batch analytics:

┌─────────────────────────────────────────────────┐
│ Data Layer: DuckDB (OLAP-optimized) │
├─────────────────────────────────────────────────┤
│ Detection Layer: │
│ ├─ SQL Rules Engine (4 typologies) │
│ └─ ML Scoring (Isolation Forest) │
├─────────────────────────────────────────────────┤
│ Integration Layer: │
│ ├─ REST API (Flask) │
│ └─ Dashboard (Streamlit) │
└─────────────────────────────────────────────────┘

**Technology Stack:**

- **Database:** [DuckDB](https://duckdb.org/) (serverless, columnar storage)
- **Rules Engine:** Advanced SQL (window functions, CTEs, aggregations)
- **ML Framework:** scikit-learn (Isolation Forest, StandardScaler)
- **API:** Flask + Flask-CORS
- **Visualization:** Streamlit + Pandas
- **Orchestration:** Python-based pipeline management

---

## 🚀 Features

### ✅ Completed Modules

#### **1. ETL Pipeline**

- Ingestion of PaySim dataset (6.3M synthetic transactions)
- DuckDB schema design and indexing
- Batch processing optimized for OLAP workloads

#### **2. Rules Engine (SQL-based)**

Detects 4 AML typologies:

- **Structuring Detection:** Multiple small transactions below reporting thresholds
- **Velocity Abuse:** Rapid transaction sequences indicating automation
- **Round Amount Patterns:** Suspicious use of exact denominations (e.g., $10,000)
- **Beneficiary Rotation:** Frequent changes in transaction recipients

#### **3. Machine Learning Scoring**

- **Customer Baselines:** Statistical profiling of normal behavior per user
- **Anomaly Detection:** Isolation Forest for unsupervised pattern recognition
- **Daily Scoring Mode:** Production-ready inference without retraining

#### **4. REST API**

Flask-based HTTP endpoints for system integration:

| Method | Endpoint                | Description                    |
| ------ | ----------------------- | ------------------------------ |
| `GET`  | `/api/v1/health`        | Service health check           |
| `GET`  | `/api/v1/stats`         | System-wide metrics            |
| `GET`  | `/api/v1/alerts`        | Rule-based alerts (filterable) |
| `GET`  | `/api/v1/alerts/ml`     | ML anomaly alerts              |
| `GET`  | `/api/v1/customer/<id>` | Customer profile with alerts   |

**Example Usage:**

Start API server
python src/05_api/app.py

Query statistics
curl http://localhost:5000/api/v1/stats

Response:
{
"total_transactions": 6362620,
"total_rule_alerts": 3,
"total_ml_alerts": 244,
"alert_rate": 0.0039
}

#### **5. Interactive Dashboard**

Streamlit-based UI featuring:

- Real-time alert monitoring
- Risk score distribution charts
- Customer deep-dive lookup
- Alert filtering by typology

#### **6. Master Orchestration Pipeline**

Single-command execution of the complete workflow:

python src/04_orchestration/master_pipeline.py

---

## 📂 Project Structure

aml-transaction-monitoring-engine/
│
├── data/
│ ├── paysim.csv # PaySim dataset (6.3M records)
│ ├── fraud_data.duckdb # DuckDB database
│ └── isolation_forest.pkl # Trained ML model
│
├── src/
│ ├── 01_etl/
│ │ └── load_data.py # ETL pipeline
│ │
│ ├── 02_rules_engine/
│ │ ├── rules.py # Structuring detection
│ │ ├── velocity_rule.py # Velocity abuse
│ │ ├── round_amounts.py # Round amount patterns
│ │ ├── beneficiary_pattern.py # Beneficiary rotation
│ │ └── executor.py # Rules orchestrator
│ │
│ ├── 03_ml_scoring/
│ │ ├── baseline.py # Customer profiling
│ │ ├── anomaly_detection.py # Isolation Forest
│ │ ├── scoring_only.py # Daily scoring mode
│ │ └── executor.py # ML orchestrator
│ │
│ ├── 04_orchestration/
│ │ └── master_pipeline.py # End-to-end pipeline
│ │
│ ├── 05_api/
│ │ └── app.py # Flask REST API
│ │
│ └── 06_dashboard/
│ └── app.py # Streamlit dashboard
│
├── requirements.txt
└── README.md

---

## 🚦 Quick Start

### Prerequisites

- Python 3.10+
- pip

### Installation

1. **Clone the repository**

git clone https://github.com/santiago-torterolo/aml-transaction-monitoring-engine.git
cd aml-transaction-monitoring-engine

2. **Install dependencies**

pip install -r requirements.txt

3. **Download dataset**

- Get PaySim from [Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)
- Place `paysim.csv` in `data/` folder

4. **Run ETL pipeline**

python src/01_etl/load_data.py

5. **Execute detection pipeline**

python src/04_orchestration/master_pipeline.py

6. **Launch interfaces**

REST API
python src/05_api/app.py

Access: http://localhost:5000
Dashboard
streamlit run src/06_dashboard/app.py

Access: http://localhost:8501

---

## 📊 Results

**System Performance (PaySim Dataset):**

- Total Transactions Processed: **6,362,620**
- Rule-Based Alerts: **3** (0.00005%)
- ML Anomalies Detected: **244** (0.0038%)
- Overall Alert Rate: **0.0039%** (industry-optimal: <1%)
- High-Risk Alerts (score ≥ 80): **2**

**Model Metrics:**

- Contamination Rate: 0.5% (Isolation Forest parameter)
- Training Sample: 10% (603k transactions)
- Features: 6 (amount, balances, temporal)

---

## 🎓 Use Cases & Interview Talking Points

**For Data/Risk Analyst Roles:**

> "I built a hybrid AML system processing 6.3M transactions using SQL for rule-based detection and Isolation Forest for unsupervised anomaly scoring. The system achieves a 0.004% alert rate, demonstrating precision-focused design that minimizes false positives for investigator teams."

**For Technical Discussions:**

- **SQL Proficiency:** Window functions (LAG, LEAD) for velocity checks; CTEs for complex pattern matching
- **ML Approach:** Unsupervised learning addresses unlabeled fraud scenarios; StandardScaler for feature normalization
- **Production Readiness:** RESTful API for integration; orchestration pipeline for scheduled batch processing; DuckDB for OLAP performance

---

## 🔮 Future Enhancements

- [ ] Network graph analysis (transaction flow visualization)
- [ ] Supervised learning module (labeled fraud training)
- [ ] Real-time streaming integration (Kafka consumer)
- [ ] Advanced NLP for transaction descriptions
- [ ] Geographic risk scoring with external data
- [ ] Automated SAR (Suspicious Activity Report) generation

---

## 📚 References

- **Dataset:** [PaySim - Kaggle](https://www.kaggle.com/datasets/ealaxi/paysim1)
- **DuckDB Documentation:** [duckdb.org/docs](https://duckdb.org/docs/)
- **FATF AML Guidelines:** [Financial Action Task Force](https://www.fatf-gafi.org/)
- **Isolation Forest Paper:** [Liu et al., 2008](https://ieeexplore.ieee.org/document/4781136)

---

## 👤 Author

**Santiago Torterolo**  
Fraud & Risk Analyst | Python/SQL | Machine Learning  
🔗 [LinkedIn](https://linkedin.com/in/santiago-torterolo-5u) | [GitHub](https://github.com/santiago-torterolo)

---

## 📄 License

This project is for portfolio demonstration purposes.  
Dataset: PaySim is publicly available under academic use terms.

---

_Last Updated: December 2025_
