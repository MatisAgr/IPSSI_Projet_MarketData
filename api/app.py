"""API marketplace simulée : référentiels fixes (seed 42) et commandes
déterministes par date -> une même date renvoie toujours les mêmes données (idempotence)."""
import os
import random
from datetime import date, timedelta
from functools import wraps

from flask import Flask, abort, jsonify, request

app = Flask(__name__)
API_TOKEN = os.environ.get("API_TOKEN", "dev-token")

CATEGORIES = ["Mode", "Electronique", "Maison", "Sport", "Beaute", "Jouets"]
COUNTRIES = ["FR", "DE", "ES", "IT", "BE"]
CITIES = ["Paris", "Lyon", "Marseille", "Lille", "Bordeaux", "Nantes"]
STATUSES = ["completed", "shipped", "cancelled", "refunded"]
STATUS_WEIGHTS = [75, 15, 7, 3]

# Référentiels générés une fois avec un seed fixe
_rng = random.Random(42)
SELLERS = [
    {
        "seller_id": f"S{i:03d}",
        "name": f"Boutique {i:03d}",
        "country": _rng.choice(COUNTRIES),
        "joined_date": (date(2024, 1, 1) + timedelta(days=_rng.randrange(500))).isoformat(),
    }
    for i in range(1, 21)
]
PRODUCTS = [
    {
        "product_id": f"P{i:03d}",
        "name": f"Produit {i:03d}",
        "category": _rng.choice(CATEGORIES),
        "base_price": round(_rng.uniform(5, 300), 2),
        "seller_id": _rng.choice(SELLERS)["seller_id"],
    }
    for i in range(1, 101)
]
CUSTOMERS = [
    {
        "customer_id": f"C{i:04d}",
        "email": f"client{i:04d}@mail.com",
        "city": _rng.choice(CITIES),
        "signup_date": (date(2024, 6, 1) + timedelta(days=_rng.randrange(400))).isoformat(),
    }
    for i in range(1, 201)
]

WEBHOOK_CALLS = []  # trace des alertes reçues (webhook simulé)


def require_token(f):
    """Auth Bearer : refuse toute requête sans le bon jeton."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if request.headers.get("Authorization") != f"Bearer {API_TOKEN}":
            abort(401, description="Jeton Bearer invalide ou absent")
        return f(*args, **kwargs)
    return wrapper


def generate_orders(ds: str) -> list[dict]:
    """Commandes du jour, seedées par la date. ~10% des vendeurs ont un 'jour creux'
    (85% de commandes perdues) pour alimenter la détection d'anomalies."""
    rng = random.Random(ds)
    bad_day = {
        s["seller_id"]: random.Random(f"{s['seller_id']}|{ds}").random() < 0.10
        for s in SELLERS
    }
    orders = []
    for i in range(rng.randint(180, 260)):
        product = rng.choice(PRODUCTS)
        if bad_day[product["seller_id"]] and rng.random() < 0.85:
            continue
        qty = rng.randint(1, 4)
        price = round(product["base_price"] * rng.uniform(0.9, 1.1), 2)
        orders.append({
            "order_id": f"{ds}-{i:05d}",
            "seller_id": product["seller_id"],
            "customer_id": rng.choice(CUSTOMERS)["customer_id"],
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": price,
            "total_amount": round(qty * price, 2),
            "status": rng.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
            "order_ts": f"{ds}T{rng.randint(8, 22):02d}:{rng.randint(0, 59):02d}:00",
        })
    return orders


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/sellers")
@require_token
def sellers():
    return jsonify(SELLERS)


@app.get("/products")
@require_token
def products():
    return jsonify(PRODUCTS)


@app.get("/customers")
@require_token
def customers():
    return jsonify(CUSTOMERS)


@app.get("/orders")
@require_token
def orders():
    ds = request.args.get("date", "")
    try:
        date.fromisoformat(ds)
    except ValueError:
        abort(400, description="Paramètre ?date=YYYY-MM-DD requis")
    return jsonify(generate_orders(ds))


@app.post("/webhook")
@require_token
def webhook():
    """Webhook simulé : logge l'alerte et la garde en mémoire (visible sur GET /webhook)."""
    payload = request.get_json(silent=True) or {}
    WEBHOOK_CALLS.append(payload)
    app.logger.warning("ALERTE ANOMALIES reçue: %s", payload)
    return {"received": True}


@app.get("/webhook")
@require_token
def webhook_log():
    return jsonify(WEBHOOK_CALLS)
