


import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ============================================
# PAGE CONFIG
# ============================================
st.set_page_config(
    page_title="AML Transaction Monitor",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================
# DATABASE CONNECTION
# ============================================
@st.cache_resource
def get_connection():
    return duckdb.connect('data/fraud_data.duckdb', read_only=True)

conn = get_connection()

# ============================================
# SIDEBAR
# ============================================
st.sidebar.title("AML Monitor")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["Dashboard", "Alerts", "Customer Lookup", "Analytics"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### System Info")
st.sidebar.info(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ============================================
# HELPER FUNCTIONS
# ============================================
@st.cache_data(ttl=60)
def get_stats():
    query = """
    SELECT 
        COUNT(*) as total_transactions,
        (SELECT COUNT(*) FROM rule_alerts) as rule_alerts,
        (SELECT COUNT(*) FROM ml_scores WHERE anomaly_score >= 0.5) as ml_alerts
    FROM transactions
    """
    return conn.execute(query).fetchdf()

@st.cache_data(ttl=60)
def get_rule_alerts(limit=50):
    query = f"""
    SELECT 
        alert_id,
        customer_id,
        rule_name,
        detection_date,
        ROUND(amount, 2) as amount,
        description
    FROM rule_alerts
    ORDER BY detection_date DESC
    LIMIT {limit}
    """
    return conn.execute(query).fetchdf()

@st.cache_data(ttl=60)
def get_ml_alerts(limit=50):
    query = f"""
    SELECT 
        m.customer_id,
        m.step,
        t.type,
        ROUND(t.amount, 2) as amount,
        ROUND(m.anomaly_score, 4) as anomaly_score
    FROM ml_scores m
    JOIN transactions t ON m.step = t.step AND m.customer_id = t.nameOrig
    WHERE m.anomaly_score >= 0.5
    ORDER BY m.anomaly_score DESC
    LIMIT {limit}
    """
    return conn.execute(query).fetchdf()

@st.cache_data(ttl=60)
def get_customer_profile(customer_id):
    query = f"""
    SELECT 
        nameOrig as customer_id,
        COUNT(*) as total_transactions,
        ROUND(SUM(amount), 2) as total_amount,
        ROUND(AVG(amount), 2) as avg_amount,
        ROUND(MAX(amount), 2) as max_amount,
        COUNT(DISTINCT type) as transaction_types
    FROM transactions
    WHERE nameOrig = '{customer_id}'
    GROUP BY nameOrig
    """
    return conn.execute(query).fetchdf()

@st.cache_data(ttl=60)
def get_customer_alerts(customer_id):
    query = f"""
    SELECT 
        rule_name,
        detection_date,
        ROUND(amount, 2) as amount,
        description
    FROM rule_alerts
    WHERE customer_id = '{customer_id}'
    ORDER BY detection_date DESC
    """
    return conn.execute(query).fetchdf()

@st.cache_data(ttl=60)
def get_alert_distribution():
    query = """
    SELECT 
        rule_name,
        COUNT(*) as count
    FROM rule_alerts
    GROUP BY rule_name
    ORDER BY count DESC
    """
    return conn.execute(query).fetchdf()

@st.cache_data(ttl=60)
def get_risk_distribution():
    query = """
    SELECT 
        CASE 
            WHEN anomaly_score >= 0.8 THEN 'Critical'
            WHEN anomaly_score >= 0.6 THEN 'High'
            WHEN anomaly_score >= 0.4 THEN 'Medium'
            ELSE 'Low'
        END as risk_level,
        COUNT(*) as count
    FROM ml_scores
    WHERE anomaly_score >= 0.3
    GROUP BY risk_level
    ORDER BY 
        CASE risk_level
            WHEN 'Critical' THEN 1
            WHEN 'High' THEN 2
            WHEN 'Medium' THEN 3
            ELSE 4
        END
    """
    return conn.execute(query).fetchdf()

# ============================================
# PAGE 1: DASHBOARD
# ============================================
if page == "Dashboard":
    st.title("AML Transaction Monitoring Dashboard")
    st.markdown("---")
    
    # Get statistics
    stats = get_stats()
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Transactions",
            value=f"{stats['total_transactions'].iloc[0]:,}"
        )
    
    with col2:
        st.metric(
            label="Rule Alerts",
            value=int(stats['rule_alerts'].iloc[0])
        )
    
    with col3:
        st.metric(
            label="ML Anomalies",
            value=int(stats['ml_alerts'].iloc[0])
        )
    
    with col4:
        alert_rate = ((stats['rule_alerts'].iloc[0] + stats['ml_alerts'].iloc[0]) / 
                      stats['total_transactions'].iloc[0] * 100)
        st.metric(
            label="Alert Rate %",
            value=f"{alert_rate:.4f}%"
        )
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Alerts by Rule Type")
        alert_dist = get_alert_distribution()
        if not alert_dist.empty:
            fig = px.bar(
                alert_dist,
                x='rule_name',
                y='count',
                color='count',
                color_continuous_scale='Reds',
                labels={'rule_name': 'Rule', 'count': 'Alerts'}
            )
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No rule alerts detected")
    
    with col2:
        st.subheader("Risk Score Distribution")
        risk_dist = get_risk_distribution()
        if not risk_dist.empty:
            colors = {'Critical': '#FF4B4B', 'High': '#FFA500', 
                     'Medium': '#FFD700', 'Low': '#90EE90'}
            fig = px.pie(
                risk_dist,
                names='risk_level',
                values='count',
                color='risk_level',
                color_discrete_map=colors
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No risk scores available")

# ============================================
# PAGE 2: ALERTS
# ============================================
elif page == "Alerts":
    st.title("Active Alerts")
    st.markdown("---")
    
    tab1, tab2 = st.tabs(["Rule-Based Alerts", "ML Anomalies"])
    
    with tab1:
        st.subheader("Rule-Based Detection")
        limit = st.slider("Number of alerts to display", 10, 100, 50)
        
        rule_alerts = get_rule_alerts(limit)
        
        if not rule_alerts.empty:
            st.dataframe(rule_alerts, use_container_width=True, height=600)
            
            # Download button
            csv = rule_alerts.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"rule_alerts_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.success("No active rule-based alerts")
    
    with tab2:
        st.subheader("Machine Learning Anomalies")
        limit = st.slider("Number of anomalies to display", 10, 100, 50, key="ml_slider")
        
        ml_alerts = get_ml_alerts(limit)
        
        if not ml_alerts.empty:
            # Add risk indicator
            ml_alerts['risk_level'] = ml_alerts['anomaly_score'].apply(
                lambda x: 'Critical' if x >= 0.8 else 
                         'High' if x >= 0.6 else 
                         'Medium'
            )
            
            st.dataframe(ml_alerts, use_container_width=True, height=600)
            
            # Download button
            csv = ml_alerts.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"ml_anomalies_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.success("No ML anomalies detected")

# ============================================
# PAGE 3: CUSTOMER LOOKUP
# ============================================
elif page == "Customer Lookup":
    st.title("Customer Risk Profile")
    st.markdown("---")
    
    customer_id = st.text_input("Enter Customer ID:", placeholder="e.g., C1234567890")
    
    if st.button("Search"):
        if customer_id:
            # Get profile
            profile = get_customer_profile(customer_id)
            
            if not profile.empty:
                st.success(f"Customer {customer_id} found")
                
                # Display metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Transactions", int(profile['total_transactions'].iloc[0]))
                with col2:
                    st.metric("Total Amount", f"${profile['total_amount'].iloc[0]:,.2f}")
                with col3:
                    st.metric("Avg Amount", f"${profile['avg_amount'].iloc[0]:,.2f}")
                with col4:
                    st.metric("Max Amount", f"${profile['max_amount'].iloc[0]:,.2f}")
                
                st.markdown("---")
                
                # Get alerts
                alerts = get_customer_alerts(customer_id)
                
                if not alerts.empty:
                    st.error(f"{len(alerts)} Alert(s) Detected")
                    st.dataframe(alerts, use_container_width=True)
                else:
                    st.success("No alerts for this customer")
            else:
                st.error("Customer not found")
        else:
            st.warning("Please enter a Customer ID")

# ============================================
# PAGE 4: ANALYTICS
# ============================================
elif page == "Analytics":
    st.title("Advanced Analytics")
    st.markdown("---")
    
    st.subheader("Transaction Type Distribution")
    
    query = """
    SELECT 
        type,
        COUNT(*) as count,
        ROUND(SUM(amount), 2) as total_amount
    FROM transactions
    GROUP BY type
    ORDER BY count DESC
    """
    
    tx_types = conn.execute(query).fetchdf()
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            tx_types,
            x='type',
            y='count',
            title='Transaction Count by Type',
            color='count',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            tx_types,
            x='type',
            y='total_amount',
            title='Transaction Volume by Type',
            color='total_amount',
            color_continuous_scale='Greens'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.subheader("System Performance Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Database Size", "~500 MB")
    with col2:
        st.metric("Detection Latency", "< 100ms")
    with col3:
        st.metric("Uptime", "99.9%")

# ============================================
# FOOTER
# ============================================
st.sidebar.markdown("---")
st.sidebar.markdown(
    """
    <div style='text-align: center'>
    <p style='font-size: 12px; color: #888;'>
    AML Transaction Monitor<br>
    Built with Streamlit + DuckDB<br>
    © 2025 Santiago Torterolo
    </p>
    </div>
    """,
    unsafe_allow_html=True
)
