"""Archiveur : consomme le MÊME log que le speed layer (groupe distinct) et écrit des
fichiers Parquet partitionnés sur Garage. C'est la source rejouable de la couche batch."""
import io
import os
import json
import time
from collections import defaultdict

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer

BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
BUCKET = os.environ.get("S3_BUCKET", "raw")
FLUSH_EVENTS = int(os.environ.get("ARCHIVE_FLUSH_EVENTS", "500"))
FLUSH_SECONDS = int(os.environ.get("ARCHIVE_FLUSH_SECONDS", "60"))

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name=os.environ.get("S3_REGION", "garage"),
)
consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": "archiver",          # groupe distinct : lit tout le log indépendamment du speed layer
    "auto.offset.reset": "earliest",  # l'archive ne doit rien perdre
    "enable.auto.commit": False,      # on ne commit qu'après écriture réussie (at-least-once)
})
consumer.subscribe([TOPIC])
print(f"Archiveur démarré : flush tous les {FLUSH_EVENTS} évts ou {FLUSH_SECONDS}s", flush=True)


def flush(buffer):
    """Écrit un fichier Parquet par partition dt= puis valide les offsets Kafka."""
    by_dt = defaultdict(list)
    for event in buffer:
        by_dt[event["order_ts"][:10]].append(event)

    for dt, rows in by_dt.items():
        table = pa.Table.from_pylist(rows)
        out = io.BytesIO()
        pq.write_table(table, out, compression="snappy")
        key = f"events/dt={dt}/part-{int(time.time() * 1000)}.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=out.getvalue())
        print(f"{len(rows)} évts -> s3://{BUCKET}/{key} ({out.tell()} octets)", flush=True)

    consumer.commit(asynchronous=False)
    buffer.clear()


buffer, last_flush = [], time.time()
while True:
    msg = consumer.poll(1.0)
    if msg is not None and not msg.error():
        buffer.append(json.loads(msg.value()))
    elif msg is not None:
        print(f"Erreur Kafka : {msg.error()}", flush=True)

    due = len(buffer) >= FLUSH_EVENTS or (buffer and time.time() - last_flush >= FLUSH_SECONDS)
    if due:
        flush(buffer)
        last_flush = time.time()
