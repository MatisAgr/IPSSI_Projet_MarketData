"""Dashboard Streamlit (option 2) : KPIs business + anomalies détectées.
Lit uniquement les tables analytics.* alimentées par les DAGs Airflow."""
import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

ACCENT = "#4269d0"  # teinte unique validée (graphiques mono-série)

st.set_page_config(page_title="Marketplace Analytics", page_icon="🛒", layout="wide")
engine = create_engine(os.environ["DWH_URI"])


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, engine)


st.title("🛒 Marketplace Analytics")

daily = query("SELECT dt, orders_count, revenue FROM analytics.daily_revenue ORDER BY dt")
if daily.empty:
    st.info("Aucune donnée : lance les DAGs Airflow (http://localhost:8080) puis rafraîchis.")
    st.stop()

last_dt = daily["dt"].max()
activity = query(f"SELECT * FROM analytics.customer_activity WHERE dt = '{last_dt}'")
anomalies = query("SELECT dt, seller_id, seller_name, revenue, avg_7d, drop_pct "
                  "FROM analytics.anomalies ORDER BY dt DESC, drop_pct DESC")

# --- Ligne de KPIs -----------------------------------------------------------
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("CA total", f"{daily['revenue'].sum():,.0f} €")
k2.metric("Commandes", f"{daily['orders_count'].sum():,}")
k3.metric("Panier moyen", f"{daily['revenue'].sum() / daily['orders_count'].sum():.2f} €")
if not activity.empty:
    k4.metric("Clients actifs (30j)", int(activity["active_customers"][0]))
    k5.metric("Clients dormants", int(activity["dormant_customers"][0]))

st.caption(f"Données du {daily['dt'].min()} au {last_dt}")

# --- CA par jour -------------------------------------------------------------
st.subheader("Chiffre d'affaires par jour")
st.line_chart(daily.set_index("dt")["revenue"], color=ACCENT, height=280)

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 10 vendeurs par revenu")
    top = query("SELECT seller_name, SUM(revenue) AS revenue FROM analytics.seller_revenue "
                "GROUP BY seller_name ORDER BY revenue DESC LIMIT 10")
    st.bar_chart(top.set_index("seller_name")["revenue"], color=ACCENT,
                 horizontal=True, height=320)

with col_right:
    st.subheader("Ventes par catégorie")
    cats = query("SELECT category, SUM(revenue) AS revenue FROM analytics.category_sales "
                 "GROUP BY category ORDER BY revenue DESC")
    st.bar_chart(cats.set_index("category")["revenue"], color=ACCENT,
                 horizontal=True, height=320)

# --- Anomalies ---------------------------------------------------------------
st.subheader(f"🚨 Anomalies détectées — drop de CA > 30% vs moyenne 7 jours ({len(anomalies)})")
if anomalies.empty:
    st.success("Aucune anomalie détectée.")
else:
    st.dataframe(
        anomalies.rename(columns={"dt": "Date", "seller_id": "Vendeur", "seller_name": "Nom",
                                  "revenue": "CA du jour (€)", "avg_7d": "Moyenne 7j (€)",
                                  "drop_pct": "Chute (%)"}),
        use_container_width=True, hide_index=True,
    )

# Table de données brute pour l'accessibilité / vérification
with st.expander("Voir les données journalières"):
    st.dataframe(daily, use_container_width=True, hide_index=True)
