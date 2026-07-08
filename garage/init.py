"""Initialisation Garage via son API admin v2 (stdlib uniquement) :
layout mono-nœud, import de la clé S3 et création du bucket. Idempotent."""
import json
import os
import sys
import time
import urllib.error
import urllib.request

ADMIN = "http://garage:3903"
TOKEN = os.environ["GARAGE_ADMIN_TOKEN"]
ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
SECRET_KEY = os.environ["S3_SECRET_KEY"]
BUCKET = os.environ.get("S3_BUCKET", "raw")


def call(method, path, body=None):
    req = urllib.request.Request(
        ADMIN + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


# Attente du démarrage de Garage
for attempt in range(60):
    try:
        status = call("GET", "/v2/GetClusterStatus")
        break
    except Exception as exc:
        print(f"Garage pas encore prêt ({exc}), retry...", flush=True)
        time.sleep(2)
else:
    sys.exit("Garage injoignable après 120s")

# 1. Layout : assigne une capacité au nœud unique si pas déjà fait
node = status["nodes"][0]
if not node.get("role"):
    call("POST", "/v2/UpdateClusterLayout",
         {"roles": [{"id": node["id"], "zone": "dc1", "capacity": 1_000_000_000, "tags": []}]})
    layout = call("GET", "/v2/GetClusterLayout")
    call("POST", "/v2/ApplyClusterLayout", {"version": layout["version"] + 1})
    print("Layout appliqué", flush=True)

# 2. Clé S3 : import de la clé fixe définie dans .env (ignore si déjà importée)
try:
    call("POST", "/v2/ImportKey",
         {"accessKeyId": ACCESS_KEY, "secretAccessKey": SECRET_KEY, "name": "airflow"})
    print("Clé S3 importée", flush=True)
except urllib.error.HTTPError as e:
    print(f"Import clé ignoré ({e.code})", flush=True)

# 3. Bucket : création + droits lecture/écriture pour la clé
try:
    bucket = call("POST", "/v2/CreateBucket", {"globalAlias": BUCKET})
except urllib.error.HTTPError:
    bucket = call("GET", f"/v2/GetBucketInfo?globalAlias={BUCKET}")
call("POST", "/v2/AllowBucketKey", {
    "bucketId": bucket["id"],
    "accessKeyId": ACCESS_KEY,
    "permissions": {"read": True, "write": True, "owner": True},
})
print(f"Bucket '{BUCKET}' prêt, init Garage terminée", flush=True)
