# 🛡️ AML Transaction Monitoring Engine

> **🚧 WORK IN PROGRESS**  
> *Active development started: December 2025*  
> *Target release: January 2026*

## 📋 Project Overview
Building a production-ready **Anti-Money Laundering (AML) System** designed to detect suspicious patterns in high-volume financial transactions. This engine moves beyond simple static rules by implementing a **Hybrid Detection Strategy**: combining SQL-based typology detection with Unsupervised Machine Learning for behavioral anomaly scoring.

The goal is to simulate a real-world FinTech compliance environment, focusing on scalability, explainability, and false-positive reduction.

## 🏗️ Architecture (Planned)
The system is built on a modern, docker-less data stack for high-performance batch analytics:

*   **Database:** [DuckDB](https://duckdb.org/) (OLAP optimized, serverless)
*   **Rules Engine:** SQL-based detection for known typologies (Structuring, Velocity, Geographic anomalies)
*   **ML Core:** `scikit-learn` (Isolation Forests & K-Means for behavioral segmentation)
*   **Orchestration:** Python-based ETL & Pipeline management
*   **Visualization:** Streamlit / Tableau for Investigator Dashboards

## 🚀 Roadmap

- [ ] **Phase 1: Infrastructure & ETL** (Current Focus)
    - [ ] DuckDB setup & Schema design
    - [ ] Ingestion pipeline for PaySim dataset (6.3M transactions)
- [ ] **Phase 2: Rules Engine**
    - [ ] Implement SQL detection for Structuring & Smurfing
    - [ ] Implement Velocity checks
- [ ] **Phase 3: Machine Learning Layer**
    - [ ] Customer Segmentation (Clustering)
    - [ ] Anomaly Detection (Isolation Forest)
- [ ] **Phase 4: Risk Scoring & Dashboard**
    - [ ] Hybrid Risk Score calculation
    - [ ] Investigator Dashboard (Streamlit)

## 🛠️ Tech Stack
![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-Analytics-yellow?style=for-the-badge)
![SQL](https://img.shields.io/badge/SQL-Advanced-orange?style=for-the-badge&logo=postgresql&logoColor=white)
![Scikit-Learn](https://img.shields.io/badge/scikit--learn-ML-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)

---
*Created by [Santiago Torterolo](https://github.com/santiago-torterolo)*
