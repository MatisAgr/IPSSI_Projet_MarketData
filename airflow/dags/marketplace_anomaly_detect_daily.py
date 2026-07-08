"""DAG 4 — Détection d'anomalies (option 2), déclenché par l'Asset dwh_orders.
Flag les vendeurs dont le CA du jour chute de plus de 30% vs leur moyenne mobile 7 jours,
puis branche : anomalies -> webhook simulé, sinon rien."""
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from lib.assets import DWH_ORDERS, resolve_dts
from lib.marketplace_hook import MarketplaceAPIHook

# CA/vendeur du jour (0 si aucune vente) vs moyenne des 7 jours précédents
INSERT_ANOMALIES = """
INSERT INTO analytics.anomalies (dt, seller_id, seller_name, revenue, avg_7d, drop_pct)
WITH daily AS (
    SELECT seller_id, dt, SUM(total_amount) AS revenue
    FROM dwh.fact_orders
    WHERE status NOT IN ('cancelled', 'refunded')
    GROUP BY seller_id, dt
),
today AS (
    SELECT s.seller_id, s.name, COALESCE(d.revenue, 0) AS revenue
    FROM dwh.dim_seller s
    LEFT JOIN daily d ON d.seller_id = s.seller_id AND d.dt = %(dt)s::date
),
hist AS (
    SELECT seller_id, AVG(revenue) AS avg_7d
    FROM daily
    WHERE dt BETWEEN %(dt)s::date - 7 AND %(dt)s::date - 1
    GROUP BY seller_id
)
SELECT %(dt)s::date, t.seller_id, t.name, t.revenue, ROUND(h.avg_7d, 2),
       ROUND(100 * (1 - t.revenue / h.avg_7d), 1)
FROM today t JOIN hist h USING (seller_id)
WHERE h.avg_7d > 0 AND t.revenue < 0.7 * h.avg_7d
"""


@dag(
    schedule=[DWH_ORDERS],
    start_date=pendulum.datetime(2026, 4, 8, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["marketplace", "anomalies"],
)
def marketplace_anomaly_detect_daily():

    dts = task(resolve_dts)()

    @task
    def detect(dts: list[str]) -> list[dict]:
        """Recalcule les anomalies des partitions traitées et les renvoie (XCom)."""
        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        anomalies = []
        for dt in dts:
            pg.run(["DELETE FROM analytics.anomalies WHERE dt = %(dt)s", INSERT_ANOMALIES],
                   parameters={"dt": dt})
            rows = pg.get_records(
                """SELECT dt::text, seller_id, seller_name, revenue::float, avg_7d::float, drop_pct::float
                   FROM analytics.anomalies WHERE dt = %(dt)s""", parameters={"dt": dt})
            anomalies += [dict(zip(("dt", "seller_id", "seller_name", "revenue", "avg_7d", "drop_pct"), r))
                          for r in rows]
        print(f"{len(anomalies)} anomalie(s) détectée(s)")
        return anomalies

    @task.branch
    def route(anomalies: list[dict]) -> str:
        return "notify_webhook" if anomalies else "no_anomaly"

    @task
    def notify_webhook(anomalies: list[dict]):
        """Alerte le webhook simulé de l'API marketplace."""
        MarketplaceAPIHook().post_webhook({"alert": "seller_revenue_drop", "anomalies": anomalies})

    @task
    def no_anomaly():
        print("Aucune anomalie, pas d'alerte")

    anomalies = detect(dts)
    route(anomalies) >> [notify_webhook(anomalies), no_anomaly()]


marketplace_anomaly_detect_daily()
