"""Producteur d'évènements : simule le flux de commandes temps réel de la marketplace.
C'est la source unique du log Kafka, lu ensuite par le speed layer ET par l'archiveur."""
import json
import os
import random
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
API_URL = os.environ["API_URL"]
API_TOKEN = os.environ["API_TOKEN"]
RATE = float(os.environ.get("EVENTS_PER_SEC", "8"))

STATUSES = ["completed", "shipped", "cancelled", "refunded"]
STATUS_WEIGHTS = [75, 15, 7, 3]
INCIDENT_SECONDS = 90       # durée d'une panne vendeur simulée
INCIDENT_PROBABILITY = 0.002  # ~1 panne toutes les 2 min à 8 évt/s


def fetch_catalog():
    """Récupère les référentiels via l'API (retry : elle peut démarrer après nous)."""
    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {API_TOKEN}"
    for _ in range(60):
        try:
            return (session.get(f"{API_URL}/sellers", timeout=5).json(),
                    session.get(f"{API_URL}/products", timeout=5).json(),
                    session.get(f"{API_URL}/customers", timeout=5).json())
        except Exception as exc:
            print(f"API pas prête ({exc}), retry...", flush=True)
            time.sleep(2)
    raise SystemExit("API injoignable après 120s")


sellers, products, customers = fetch_catalog()
seller_ids = [s["seller_id"] for s in sellers]
producer = Producer({"bootstrap.servers": BROKER, "linger.ms": 50})
print(f"Producteur prêt : {len(products)} produits, {RATE} évt/s -> topic '{TOPIC}'", flush=True)

incident_seller, incident_end, sent = None, 0.0, 0

while True:
    now = time.time()
    # Panne aléatoire d'un vendeur : crée une vraie anomalie à détecter en direct
    if now >= incident_end and random.random() < INCIDENT_PROBABILITY:
        incident_seller, incident_end = random.choice(seller_ids), now + INCIDENT_SECONDS
        print(f"[incident] {incident_seller} en panne {INCIDENT_SECONDS}s", flush=True)

    product = random.choice(products)
    in_panne = product["seller_id"] == incident_seller and now < incident_end
    if in_panne and random.random() < 0.9:
        time.sleep(1 / RATE)
        continue

    quantity = random.randint(1, 4)
    unit_price = round(product["base_price"] * random.uniform(0.9, 1.1), 2)
    event = {
        "order_id": f"live-{int(now * 1000)}-{sent}",
        "seller_id": product["seller_id"],
        "customer_id": random.choice(customers)["customer_id"],
        "product_id": product["product_id"],
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
        "order_ts": datetime.now(timezone.utc).isoformat(),
    }
    # Clé = seller_id : garantit l'ordre par vendeur et distribue la charge entre partitions
    producer.produce(TOPIC, key=event["seller_id"].encode(), value=json.dumps(event).encode())
    producer.poll(0)

    sent += 1
    if sent % 200 == 0:
        producer.flush(5)
        print(f"{sent} évènements publiés", flush=True)
    time.sleep(1 / RATE)
