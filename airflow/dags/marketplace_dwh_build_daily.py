"""DAG 2 — Construction du DWH (dimensions + faits), déclenché par l'Asset raw_orders.
Dimensions en upsert, faits en DELETE + INSERT par partition dt (idempotent)."""
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from lib.alerting import notify_dag_failure
from lib.assets import DWH_ORDERS, RAW_ORDERS, resolve_dts
from lib.category_reference import CATEGORY_REFERENCE, DEFAULT_CATEGORY_ENTRY
from lib.data_quality import DataQualityOperator

UPSERT_DIMS = [
    """INSERT INTO dwh.dim_seller SELECT * FROM staging.sellers
       ON CONFLICT (seller_id) DO UPDATE SET name=EXCLUDED.name, country=EXCLUDED.country,
                                             joined_date=EXCLUDED.joined_date""",
    """INSERT INTO dwh.dim_customer SELECT * FROM staging.customers
       ON CONFLICT (customer_id) DO UPDATE SET email=EXCLUDED.email, city=EXCLUDED.city,
                                               signup_date=EXCLUDED.signup_date""",
    """INSERT INTO dwh.dim_product SELECT * FROM staging.products
       ON CONFLICT (product_id) DO UPDATE SET name=EXCLUDED.name, category=EXCLUDED.category,
                                              base_price=EXCLUDED.base_price""",
]

BUILD_PARTITION = [
    """INSERT INTO dwh.dim_date (dt, year, month, day_of_week)
       SELECT d, EXTRACT(YEAR FROM d)::int, EXTRACT(MONTH FROM d)::int, EXTRACT(ISODOW FROM d)::int
       FROM (SELECT %(dt)s::date AS d) x
       ON CONFLICT (dt) DO NOTHING""",
    "DELETE FROM dwh.fact_orders WHERE dt = %(dt)s",
    """INSERT INTO dwh.fact_orders (order_id, seller_id, customer_id, product_id, dt,
                                    quantity, total_amount, status)
       SELECT order_id, seller_id, customer_id, product_id, dt, quantity, total_amount, status
       FROM staging.orders WHERE dt = %(dt)s""",
]

UPSERT_CATEGORY = """
INSERT INTO dwh.dim_category (category, department, margin_target_pct, is_seasonal)
VALUES (%(category)s, %(department)s, %(margin_target_pct)s, %(is_seasonal)s)
ON CONFLICT (category) DO UPDATE SET department=EXCLUDED.department,
    margin_target_pct=EXCLUDED.margin_target_pct, is_seasonal=EXCLUDED.is_seasonal
"""

DTS_SQL = "{{ ti.xcom_pull(task_ids='resolve_dts', key='dts_sql') }}"


@dag(
    schedule=[RAW_ORDERS],
    start_date=pendulum.datetime(2026, 4, 8, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=notify_dag_failure,
    tags=["marketplace", "elt"],
)
def marketplace_dwh_build_daily():

    dts = task(resolve_dts)()

    @task
    def build_dims_and_facts(dts: list[str]):
        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        pg.run(UPSERT_DIMS)
        for dt in dts:
            pg.run(BUILD_PARTITION, parameters={"dt": dt})
            print(f"Partition fact_orders dt={dt} reconstruite")

    @task
    def build_dim_category():
        """Enrichit dwh.dim_category depuis le référentiel externe CATEGORY_REFERENCE,
        y compris les catégories vues en staging mais absentes du mapping (valeurs
        par défaut, cf. DEFAULT_CATEGORY_ENTRY)."""
        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        seen = {r[0] for r in pg.get_records("SELECT DISTINCT category FROM staging.products")}
        for category in seen | CATEGORY_REFERENCE.keys():
            entry = CATEGORY_REFERENCE.get(category, DEFAULT_CATEGORY_ENTRY)
            pg.run(UPSERT_CATEGORY, parameters={"category": category, **entry})
        print(f"dim_category mise à jour ({len(seen | CATEGORY_REFERENCE.keys())} catégories)")

    dq_check = DataQualityOperator(
        task_id="dq_check_dwh",
        conn_id="postgres_dwh",
        rules=[
            {"name": "fact_non_vide", "op": "gt",
             "sql": f"SELECT COUNT(*) FROM dwh.fact_orders WHERE dt IN {DTS_SQL}"},
            {"name": "fk_seller_valide",
             "sql": f"""SELECT COUNT(*) FROM dwh.fact_orders f
                        LEFT JOIN dwh.dim_seller s USING (seller_id)
                        WHERE f.dt IN {DTS_SQL} AND s.seller_id IS NULL"""},
            {"name": "fk_product_valide",
             "sql": f"""SELECT COUNT(*) FROM dwh.fact_orders f
                        LEFT JOIN dwh.dim_product p USING (product_id)
                        WHERE f.dt IN {DTS_SQL} AND p.product_id IS NULL"""},
            {"name": "categories_couvertes",
             "sql": """SELECT COUNT(*) FROM dwh.dim_product p
                       LEFT JOIN dwh.dim_category c USING (category)
                       WHERE c.category IS NULL"""},
        ],
    )

    @task(outlets=[DWH_ORDERS])
    def publish_asset(dts: list[str], outlet_events=None):
        """Émet l'Asset dwh_orders -> déclenche les DAGs analytics et anomalies."""
        outlet_events[DWH_ORDERS].extra = {"dts": dts}

    [build_dims_and_facts(dts), build_dim_category()] >> dq_check >> publish_asset(dts)


marketplace_dwh_build_daily()
