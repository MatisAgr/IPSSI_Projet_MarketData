"""Dashboard Streamlit (option 2) : KPIs business + anomalies détectées.
Lit uniquement les tables analytics.* alimentées par les DAGs Airflow."""
import os
from datetime import timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

ACCENT = "#4269d0"  # teinte unique validée (graphiques mono-série)
SEGMENT_COLORS = {
    "Champions": "#4269d0",
    "Fidèles": "#63a375",
    "À risque": "#efb118",
    "Nouveaux": "#8c6dae",
    "Perdus": "#c94141",
    "Standard": "#9498a0",
}

st.set_page_config(page_title="Marketplace Analytics", page_icon="🛒", layout="wide")
engine = create_engine(os.environ["DWH_URI"])


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, engine)


def pct_delta(current: float, previous: float) -> str | None:
    """Formatte un delta % pour st.metric, None si non calculable."""
    if previous in (0, None) or pd.isna(previous):
        return None
    return f"{(current - previous) / previous * 100:+.1f}%"


st.title("🛒 Marketplace Analytics")

daily = query("SELECT dt, orders_count, revenue FROM analytics.daily_revenue ORDER BY dt")
if daily.empty:
    st.info("Aucune donnée : lance les DAGs Airflow (http://localhost:8080) puis rafraîchis.")
    st.stop()
daily["dt"] = pd.to_datetime(daily["dt"])

seller_rev = query("SELECT dt, seller_id, seller_name, orders_count, revenue "
                    "FROM analytics.seller_revenue ORDER BY dt")
seller_rev["dt"] = pd.to_datetime(seller_rev["dt"])
cat_rev = query("SELECT dt, category, quantity, revenue FROM analytics.category_sales ORDER BY dt")
cat_rev["dt"] = pd.to_datetime(cat_rev["dt"])
activity = query("SELECT * FROM analytics.customer_activity ORDER BY dt")
activity["dt"] = pd.to_datetime(activity["dt"])
anomalies = query("SELECT dt, seller_id, seller_name, revenue, avg_7d, drop_pct "
                  "FROM analytics.anomalies ORDER BY dt DESC, drop_pct DESC")
anomalies["dt"] = pd.to_datetime(anomalies["dt"])
rfm = query("SELECT * FROM analytics.customer_rfm")

# --- Filtres (sidebar) -------------------------------------------------------
min_dt, max_dt = daily["dt"].min().date(), daily["dt"].max().date()
st.sidebar.header("Filtres")
date_range = st.sidebar.date_input(
    "Période", value=(min_dt, max_dt), min_value=min_dt, max_value=max_dt,
)
if len(date_range) != 2:
    st.stop()
start_dt, end_dt = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])

all_sellers = sorted(seller_rev["seller_name"].unique())
picked_sellers = st.sidebar.multiselect("Vendeurs", all_sellers, default=[])
all_categories = sorted(cat_rev["category"].unique())
picked_categories = st.sidebar.multiselect("Catégories", all_categories, default=[])

# Période précédente de même durée, pour comparaison
period_days = (end_dt - start_dt).days + 1
prev_start, prev_end = start_dt - timedelta(days=period_days), start_dt - timedelta(days=1)

mask = (daily["dt"] >= start_dt) & (daily["dt"] <= end_dt)
prev_mask = (daily["dt"] >= prev_start) & (daily["dt"] <= prev_end)
daily_f, daily_prev = daily[mask], daily[prev_mask]

st.caption(f"Période analysée : {start_dt.date()} → {end_dt.date()} "
           f"(comparée à {prev_start.date()} → {prev_end.date()})")

# --- Ligne de KPIs avec delta vs période précédente --------------------------
rev, rev_prev = daily_f["revenue"].sum(), daily_prev["revenue"].sum()
orders, orders_prev = daily_f["orders_count"].sum(), daily_prev["orders_count"].sum()
basket = rev / orders if orders else 0
basket_prev = rev_prev / orders_prev if orders_prev else 0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("CA total", f"{rev:,.0f} €", pct_delta(rev, rev_prev))
k2.metric("Commandes", f"{orders:,}", pct_delta(orders, orders_prev))
k3.metric("Panier moyen", f"{basket:.2f} €", pct_delta(basket, basket_prev))
if not activity.empty:
    last_activity = activity[activity["dt"] <= end_dt].tail(1)
    if not last_activity.empty:
        k4.metric("Clients actifs (30j)", int(last_activity["active_customers"].iloc[0]))
        k5.metric("Clients dormants", int(last_activity["dormant_customers"].iloc[0]))

# --- CA par jour + prévision 7 jours -----------------------------------------
st.subheader("Chiffre d'affaires par jour")
fig = go.Figure()
fig.add_trace(go.Scatter(x=daily_f["dt"], y=daily_f["revenue"], mode="lines",
                          name="CA réel", line=dict(color=ACCENT, width=2)))

FORECAST_DAYS = 7
if len(daily_f) >= 5:
    x = np.arange(len(daily_f))
    slope, intercept = np.polyfit(x, daily_f["revenue"].to_numpy(), 1)
    future_x = np.arange(len(daily_f), len(daily_f) + FORECAST_DAYS)
    future_dt = pd.date_range(daily_f["dt"].max() + timedelta(days=1), periods=FORECAST_DAYS)
    forecast = np.clip(slope * future_x + intercept, 0, None)
    fig.add_trace(go.Scatter(
        x=[daily_f["dt"].iloc[-1], *future_dt], y=[daily_f["revenue"].iloc[-1], *forecast],
        mode="lines", name="Tendance (7j)", line=dict(color=ACCENT, width=2, dash="dash"),
    ))
    trend_pct = slope * len(daily_f) / daily_f["revenue"].mean() * 100 if daily_f["revenue"].mean() else 0
    st.caption(f"Tendance régression linéaire : {slope:+,.0f} €/jour "
               f"({trend_pct:+.1f}% de la moyenne de la période).")
fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0),
                   legend=dict(orientation="h", yanchor="bottom", y=1.02))
st.plotly_chart(fig, use_container_width=True)

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 10 vendeurs par revenu")
    top_src = seller_rev[(seller_rev["dt"] >= start_dt) & (seller_rev["dt"] <= end_dt)]
    if picked_sellers:
        top_src = top_src[top_src["seller_name"].isin(picked_sellers)]
    top = top_src.groupby("seller_name", as_index=False)["revenue"].sum() \
                 .sort_values("revenue", ascending=False).head(10)
    st.bar_chart(top.set_index("seller_name")["revenue"], color=ACCENT,
                 horizontal=True, height=320)

with col_right:
    st.subheader("Ventes par catégorie")
    cat_src = cat_rev[(cat_rev["dt"] >= start_dt) & (cat_rev["dt"] <= end_dt)]
    if picked_categories:
        cat_src = cat_src[cat_src["category"].isin(picked_categories)]
    cats = cat_src.groupby("category", as_index=False)["revenue"].sum() \
                  .sort_values("revenue", ascending=False)
    st.bar_chart(cats.set_index("category")["revenue"], color=ACCENT,
                 horizontal=True, height=320)

# --- Tendances vendeurs : progressions / déclins -----------------------------
st.subheader("📈 Tendances vendeurs — 7 derniers jours vs 7 jours précédents")
last_dt_all = seller_rev["dt"].max()
window_a = seller_rev[(seller_rev["dt"] > last_dt_all - timedelta(days=7)) & (seller_rev["dt"] <= last_dt_all)]
window_b = seller_rev[(seller_rev["dt"] > last_dt_all - timedelta(days=14)) & (seller_rev["dt"] <= last_dt_all - timedelta(days=7))]
rev_a = window_a.groupby("seller_name")["revenue"].sum()
rev_b = window_b.groupby("seller_name")["revenue"].sum()
trend = pd.DataFrame({"7j récents": rev_a, "7j précédents": rev_b}).fillna(0)
trend = trend[(trend["7j récents"] > 0) | (trend["7j précédents"] > 0)]
trend["variation_pct"] = ((trend["7j récents"] - trend["7j précédents"])
                           / trend["7j précédents"].replace(0, np.nan) * 100)
trend = trend.dropna(subset=["variation_pct"]).sort_values("variation_pct", ascending=False)

tcol1, tcol2 = st.columns(2)
with tcol1:
    st.caption("🚀 Top 5 progressions")
    st.dataframe(trend.head(5).reset_index(names="Vendeur")
                 .rename(columns={"variation_pct": "Variation (%)"})
                 .style.format({"7j récents": "{:,.0f} €", "7j précédents": "{:,.0f} €", "Variation (%)": "{:+.1f}"}),
                 use_container_width=True, hide_index=True)
with tcol2:
    st.caption("📉 Top 5 déclins")
    st.dataframe(trend.tail(5).sort_values("variation_pct").reset_index(names="Vendeur")
                 .rename(columns={"variation_pct": "Variation (%)"})
                 .style.format({"7j récents": "{:,.0f} €", "7j précédents": "{:,.0f} €", "Variation (%)": "{:+.1f}"}),
                 use_container_width=True, hide_index=True)

# --- Anomalies ---------------------------------------------------------------
anomalies_f = anomalies[(anomalies["dt"] >= start_dt) & (anomalies["dt"] <= end_dt)]
st.subheader(f"🚨 Anomalies détectées — drop de CA > 30% vs moyenne 7 jours ({len(anomalies_f)})")
if anomalies_f.empty:
    st.success("Aucune anomalie détectée sur la période.")
else:
    st.dataframe(
        anomalies_f.rename(columns={"dt": "Date", "seller_id": "Vendeur", "seller_name": "Nom",
                                  "revenue": "CA du jour (€)", "avg_7d": "Moyenne 7j (€)",
                                  "drop_pct": "Chute (%)"}),
        use_container_width=True, hide_index=True,
    )

# --- Segmentation clients (RFM) -----------------------------------------------
st.subheader("👥 Segmentation clients (RFM)")
if rfm.empty:
    st.info("Pas encore de segmentation disponible : lance/relance "
            "`marketplace_analytics_aggregate_daily` pour la calculer.")
else:
    seg_counts = rfm["segment"].value_counts().reindex(SEGMENT_COLORS).dropna()
    seg_revenue = rfm.groupby("segment")["monetary"].sum().reindex(SEGMENT_COLORS).dropna()

    scol1, scol2 = st.columns(2)
    with scol1:
        st.caption("Répartition des clients par segment")
        fig_seg = px.pie(names=seg_counts.index, values=seg_counts.values,
                          color=seg_counts.index, color_discrete_map=SEGMENT_COLORS, hole=0.45)
        fig_seg.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig_seg, use_container_width=True)
    with scol2:
        st.caption("CA cumulé par segment")
        fig_rev = px.bar(x=seg_revenue.values, y=seg_revenue.index, orientation="h",
                          color=seg_revenue.index, color_discrete_map=SEGMENT_COLORS)
        fig_rev.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0), showlegend=False,
                               xaxis_title="CA (€)", yaxis_title=None)
        st.plotly_chart(fig_rev, use_container_width=True)

    with st.expander("🏆 Champions (fort R+F+M) — top 20 par CA"):
        champions = rfm[rfm["segment"] == "Champions"].sort_values("monetary", ascending=False).head(20)
        st.dataframe(champions[["customer_email", "city", "recency_days", "frequency", "monetary"]]
                     .rename(columns={"customer_email": "Email", "city": "Ville",
                                       "recency_days": "Récence (j)", "frequency": "Fréquence",
                                       "monetary": "Montant (€)"}),
                     use_container_width=True, hide_index=True)

    with st.expander("⚠️ Clients à risque — top 20 par CA (à relancer en priorité)"):
        at_risk = rfm[rfm["segment"] == "À risque"].sort_values("monetary", ascending=False).head(20)
        st.dataframe(at_risk[["customer_email", "city", "recency_days", "frequency", "monetary"]]
                     .rename(columns={"customer_email": "Email", "city": "Ville",
                                       "recency_days": "Récence (j)", "frequency": "Fréquence",
                                       "monetary": "Montant (€)"}),
                     use_container_width=True, hide_index=True)

# Table de données brute pour l'accessibilité / vérification
with st.expander("Voir les données journalières"):
    st.dataframe(daily_f, use_container_width=True, hide_index=True)
