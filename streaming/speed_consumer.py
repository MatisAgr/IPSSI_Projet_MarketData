"""Speed layer : agrège le log Kafka sur une fenêtre glissante et détecte les chutes
de CA vendeur en quelques secondes, là où la couche batch met 24h."""
import json
import os
import time
from collections import defaultdict, deque

import redis
from confluent_kafka import Consumer

BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ.get("KAFKA_TOPIC", "orders")
WINDOW = int(os.environ.get("SPEED_WINDOW_SECONDS", "60"))      # fenêtre courante
BASELINE = int(os.environ.get("SPEED_BASELINE_SECONDS", "600"))  # référence de comparaison
DROP_THRESHOLD = float(os.environ.get("SPEED_DROP_THRESHOLD", "0.30"))
REFRESH = 2  # période de recalcul du snapshot

rds = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": "speed-layer",
    "auto.offset.reset": "latest",  # le temps réel ne rejoue pas le passé
})
consumer.subscribe([TOPIC])

# ponytail: état en mémoire (deque), suffisant ici ; un state store Kafka Streams/Flink
# serait nécessaire pour survivre à un redémarrage sans perdre la fenêtre.
events = deque()
totals = {"events": 0, "revenue": 0.0}
flagged = set()
last_refresh = 0.0
print(f"Speed layer démarré : fenêtre {WINDOW}s, baseline {BASELINE}s", flush=True)


def build_snapshot(now):
    """Calcule les agrégats de la fenêtre courante et les anomalies vs baseline."""
    while events and events[0][0] < now - BASELINE:
        events.popleft()

    window_rev = defaultdict(float)
    window_orders, window_total = 0, 0.0
    base_rev = defaultdict(float)
    for ts, seller, amount in events:
        if ts >= now - WINDOW:
            window_rev[seller] += amount
            window_orders += 1
            window_total += amount
        else:
            base_rev[seller] += amount

    # Baseline ramenée à la durée de la fenêtre pour être comparable
    scale = WINDOW / max(BASELINE - WINDOW, 1)
    anomalies = []
    for seller, base in base_rev.items():
        expected = base * scale
        current = window_rev.get(seller, 0.0)
        if expected > 50 and current < (1 - DROP_THRESHOLD) * expected:
            anomalies.append({
                "seller_id": seller,
                "revenue_window": round(current, 2),
                "expected": round(expected, 2),
                "drop_pct": round(100 * (1 - current / expected), 1),
            })
    anomalies.sort(key=lambda a: -a["drop_pct"])

    top = sorted(window_rev.items(), key=lambda kv: -kv[1])[:10]
    return {
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now)),
        "window_seconds": WINDOW,
        "orders_in_window": window_orders,
        "revenue_in_window": round(window_total, 2),
        "events_total": totals["events"],
        "revenue_total": round(totals["revenue"], 2),
        "top_sellers": [{"seller_id": s, "revenue": round(r, 2)} for s, r in top],
        "anomalies": anomalies,
    }


while True:
    msg = consumer.poll(0.5)
    if msg is not None and not msg.error():
        event = json.loads(msg.value())
        if event["status"] not in ("cancelled", "refunded"):
            events.append((time.time(), event["seller_id"], event["total_amount"]))
            totals["events"] += 1
            totals["revenue"] += event["total_amount"]
    elif msg is not None:
        print(f"Erreur Kafka : {msg.error()}", flush=True)

    now = time.time()
    if now - last_refresh < REFRESH:
        continue
    last_refresh = now

    snapshot = build_snapshot(now)
    # TTL : si le consumer meurt, le dashboard cesse d'afficher des données "live" périmées
    rds.setex("live:snapshot", 30, json.dumps(snapshot))

    # Historise chaque anomalie une seule fois, au moment où elle apparaît
    current = {a["seller_id"] for a in snapshot["anomalies"]}
    for anomaly in snapshot["anomalies"]:
        if anomaly["seller_id"] not in flagged:
            rds.lpush("live:anomalies", json.dumps({**anomaly, "at": snapshot["updated_at"]}))
            print(f"[ANOMALIE] {anomaly['seller_id']} -{anomaly['drop_pct']}%", flush=True)
    rds.ltrim("live:anomalies", 0, 49)
    flagged = current
