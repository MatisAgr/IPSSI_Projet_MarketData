"""DAG 1 — Ingestion quotidienne : API (Custom Hook) -> Garage S3 (raw Parquet) -> staging.
Idempotent : DELETE + INSERT sur la partition dt = {{ ds }}."""
import io
import os

import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from lib.alerting import notify_dag_failure
from lib.assets import RAW_ORDERS
from lib.data_quality import DataQualityOperator
from lib.marketplace_hook import MarketplaceAPIHook

BUCKET = os.environ.get("S3_BUCKET", "raw")

# Colonnes chargées en staging, dans l'ordre des tables SQL
COLUMNS = {
    "sellers": ["seller_id", "name", "country", "joined_date"],
    "products": ["product_id", "name", "category", "base_price"],
    "customers": ["customer_id", "email", "city", "signup_date"],
    "orders": ["order_id", "seller_id", "customer_id", "product_id",
               "quantity", "unit_price", "total_amount", "status", "order_ts"],
}


def write_parquet(s3, rows, key):
    """Sérialise des dicts en Parquet compressé (format colonne du data lake)."""
    out = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), out, compression="snappy")
    s3.load_bytes(out.getvalue(), key=key, bucket_name=BUCKET, replace=True)


def read_parquet(s3, key):
    body = s3.get_key(key, BUCKET).get()["Body"].read()
    return pq.read_table(io.BytesIO(body)).to_pylist()


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 8, tz="UTC"),  # ~3 mois d'historique pour le backfill
    catchup=True,
    max_active_runs=1,
    on_failure_callback=notify_dag_failure,
    tags=["marketplace", "elt"],
)
def marketplace_orders_ingest_daily():

    @task
    def extract_to_raw(ds=None):
        """Extrait les 4 entités via le Custom Hook et dépose du Parquet brut sur Garage."""
        api = MarketplaceAPIHook()
        s3 = S3Hook(aws_conn_id="garage_s3")
        entities = {
            "orders": api.get_orders(ds),
            "sellers": api.get_sellers(),
            "products": api.get_products(),
            "customers": api.get_customers(),
        }
        for name, rows in entities.items():
            key = f"{name}/dt={ds}/{name}.parquet"
            write_parquet(s3, rows, key)
            print(f"{len(rows)} lignes -> s3://{BUCKET}/{key}")

    @task
    def load_staging(ds=None):
        """Relit le raw depuis Garage et charge staging (référentiels: full refresh,
        commandes: DELETE+INSERT sur la partition dt)."""
        s3 = S3Hook(aws_conn_id="garage_s3")
        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        read = lambda name: read_parquet(s3, f"{name}/dt={ds}/{name}.parquet")

        for name in ("sellers", "products", "customers"):
            rows = read(name)
            pg.run(f"TRUNCATE staging.{name}")
            pg.insert_rows(f"staging.{name}", [[r[c] for c in COLUMNS[name]] for r in rows],
                           target_fields=COLUMNS[name])

        orders = read("orders")
        pg.run("DELETE FROM staging.orders WHERE dt = %s", parameters=(ds,))
        cols = COLUMNS["orders"] + ["dt"]
        pg.insert_rows("staging.orders", [[o[c] for c in COLUMNS["orders"]] + [ds] for o in orders],
                       target_fields=cols)
        print(f"{len(orders)} commandes chargées en staging pour dt={ds}")

    # Custom Operator : 5 règles SQL configurables sur la partition du jour
    dq_check = DataQualityOperator(
        task_id="dq_check_staging",
        conn_id="postgres_dwh",
        rules=[
            {"name": "commandes_presentes", "op": "gt",
             "sql": "SELECT COUNT(*) FROM staging.orders WHERE dt = '{{ ds }}'"},
            {"name": "order_id_non_null",
             "sql": "SELECT COUNT(*) FROM staging.orders WHERE dt = '{{ ds }}' AND order_id IS NULL"},
            {"name": "order_id_unique",
             "sql": "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM staging.orders WHERE dt = '{{ ds }}'"},
            {"name": "montant_positif",
             "sql": "SELECT COUNT(*) FROM staging.orders WHERE dt = '{{ ds }}' AND total_amount < 0"},
            {"name": "quantite_positive",
             "sql": "SELECT COUNT(*) FROM staging.orders WHERE dt = '{{ ds }}' AND quantity <= 0"},
        ],
    )

    @task(outlets=[RAW_ORDERS])
    def publish_asset(ds=None, outlet_events=None):
        """Émet l'Asset raw_orders avec la date traitée -> déclenche le DAG dwh_build."""
        outlet_events[RAW_ORDERS].extra = {"dts": [ds]}

    extract_to_raw() >> load_staging() >> dq_check >> publish_asset()


marketplace_orders_ingest_daily()
