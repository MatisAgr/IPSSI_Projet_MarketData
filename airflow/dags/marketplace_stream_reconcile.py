"""DAG 5 — Boucle Lambda : relit le log d'évènements archivé (Parquet sur Garage) et
recalcule la vérité batch du jour, que le dashboard compare au temps réel du speed layer."""
import io
import os

import pendulum
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from lib.alerting import notify_dag_failure

BUCKET = os.environ.get("S3_BUCKET", "raw")


@dag(
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 4, 8, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=notify_dag_failure,
    tags=["marketplace", "streaming", "lambda"],
)
def marketplace_stream_reconcile():

    @task
    def reconcile(ds=None):
        """Recompute complet : relit TOUS les fichiers du jour, donc idempotent par nature."""
        s3 = S3Hook(aws_conn_id="garage_s3")
        keys = s3.list_keys(bucket_name=BUCKET, prefix=f"events/dt={ds}/") or []
        if not keys:
            print(f"Aucun évènement archivé pour dt={ds}")
            return 0

        totals, read = {}, 0
        for key in keys:
            body = s3.get_key(key, BUCKET).get()["Body"].read()
            for row in pq.read_table(io.BytesIO(body)).to_pylist():
                read += 1
                if row["status"] in ("cancelled", "refunded"):
                    continue
                agg = totals.setdefault(row["seller_id"], [0, 0.0])
                agg[0] += 1
                agg[1] += row["total_amount"]

        pg = PostgresHook(postgres_conn_id="postgres_dwh")
        pg.run("DELETE FROM analytics.stream_daily WHERE dt = %s", parameters=(ds,))
        if totals:
            pg.insert_rows(
                "analytics.stream_daily",
                [[ds, seller, count, round(revenue, 2)] for seller, (count, revenue) in totals.items()],
                target_fields=["dt", "seller_id", "orders_count", "revenue"],
            )
        print(f"{read} évènements relus dans {len(keys)} fichiers Parquet -> {len(totals)} vendeurs")
        return read

    reconcile()


marketplace_stream_reconcile()
