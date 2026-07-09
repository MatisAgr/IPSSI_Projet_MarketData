"""DAG 3 — Agrégations analytics lues par le dashboard, déclenché par l'Asset dwh_orders.
Chaque table est reconstruite en DELETE + INSERT par partition dt (idempotent)."""
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from lib.assets import DWH_ORDERS, resolve_dts

# Le CA exclut les commandes annulées / remboursées
AGGREGATIONS = [
    "DELETE FROM analytics.daily_revenue WHERE dt = %(dt)s",
    """INSERT INTO analytics.daily_revenue
       SELECT dt, COUNT(*), SUM(total_amount)
       FROM dwh.fact_orders
       WHERE dt = %(dt)s AND status NOT IN ('cancelled', 'refunded')
       GROUP BY dt""",

    "DELETE FROM analytics.seller_revenue WHERE dt = %(dt)s",
    """INSERT INTO analytics.seller_revenue
       SELECT f.dt, f.seller_id, s.name, COUNT(*), SUM(f.total_amount)
       FROM dwh.fact_orders f JOIN dwh.dim_seller s USING (seller_id)
       WHERE f.dt = %(dt)s AND f.status NOT IN ('cancelled', 'refunded')
       GROUP BY f.dt, f.seller_id, s.name""",

    "DELETE FROM analytics.category_sales WHERE dt = %(dt)s",
    """INSERT INTO analytics.category_sales
       SELECT f.dt, p.category, SUM(f.quantity), SUM(f.total_amount)
       FROM dwh.fact_orders f JOIN dwh.dim_product p USING (product_id)
       WHERE f.dt = %(dt)s AND f.status NOT IN ('cancelled', 'refunded')
       GROUP BY f.dt, p.category""",

    "DELETE FROM analytics.customer_activity WHERE dt = %(dt)s",
    # Actif = a commandé dans les 30 derniers jours, dormant = le reste des inscrits
    """INSERT INTO analytics.customer_activity
       SELECT %(dt)s::date,
              COUNT(DISTINCT customer_id),
              (SELECT COUNT(*) FROM dwh.dim_customer) - COUNT(DISTINCT customer_id)
       FROM dwh.fact_orders
       WHERE dt BETWEEN %(dt)s::date - 29 AND %(dt)s::date""",
]

# Segmentation RFM (Récence/Fréquence/Montant) : snapshot complet recalculé à chaque run,
# scoré en quartiles (NTILE 4) sur tout l'historique disponible, pas de partition dt.
RFM_SQL = """
TRUNCATE analytics.customer_rfm;
INSERT INTO analytics.customer_rfm
WITH calc_date AS (
    SELECT MAX(dt) AS dt FROM dwh.fact_orders
),
cust_stats AS (
    SELECT customer_id,
           (SELECT dt FROM calc_date) - MAX(dt) AS recency_days,
           COUNT(*) AS frequency,
           SUM(total_amount) AS monetary
    FROM dwh.fact_orders
    WHERE status NOT IN ('cancelled', 'refunded')
    GROUP BY customer_id
),
scored AS (
    SELECT cs.customer_id, c.email AS customer_email, c.city,
           cs.recency_days, cs.frequency, cs.monetary,
           NTILE(4) OVER (ORDER BY cs.recency_days DESC) AS r_score,
           NTILE(4) OVER (ORDER BY cs.frequency ASC) AS f_score,
           NTILE(4) OVER (ORDER BY cs.monetary ASC) AS m_score
    FROM cust_stats cs
    JOIN dwh.dim_customer c USING (customer_id)
)
SELECT customer_id, customer_email, city, (SELECT dt FROM calc_date),
       recency_days, frequency, monetary, r_score, f_score, m_score,
       CASE
           WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
           WHEN f_score >= 3 AND m_score >= 3 THEN 'Fidèles'
           WHEN r_score <= 2 AND (f_score >= 3 OR m_score >= 3) THEN 'À risque'
           WHEN f_score <= 1 AND r_score >= 4 THEN 'Nouveaux'
           WHEN r_score <= 1 AND f_score <= 1 AND m_score <= 1 THEN 'Perdus'
           ELSE 'Standard'
       END AS segment
FROM scored;
"""


@dag(
    schedule=[DWH_ORDERS],
    start_date=pendulum.datetime(2026, 4, 8, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["marketplace", "analytics"],
)
def marketplace_analytics_aggregate_daily():

    dts = task(resolve_dts)()

    @task
    def aggregate(dts: list[str]):
        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        for dt in dts:
            pg.run(AGGREGATIONS, parameters={"dt": dt})
            print(f"Tables analytics reconstruites pour dt={dt}")

    @task
    def compute_rfm():
        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        pg.run(RFM_SQL)
        print("Segmentation RFM clients recalculée")

    aggregate(dts) >> compute_rfm()


marketplace_analytics_aggregate_daily()
