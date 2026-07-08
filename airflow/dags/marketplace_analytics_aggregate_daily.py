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

    aggregate(dts)


marketplace_analytics_aggregate_daily()
