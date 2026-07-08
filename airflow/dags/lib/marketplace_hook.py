"""Custom Hook pour l'API marketplace (auth Bearer via la connexion Airflow)."""
import requests
from airflow.sdk import BaseHook


class MarketplaceAPIHook(BaseHook):
    """Lit host/port/token dans la connexion `marketplace_api` et expose l'API REST."""

    def __init__(self, conn_id: str = "marketplace_api"):
        super().__init__()
        conn = self.get_connection(conn_id)
        self.base_url = f"http://{conn.host}:{conn.port}"
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {conn.password}"

    def _get(self, path: str, **params):
        resp = self.session.get(f"{self.base_url}{path}", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_orders(self, date: str) -> list[dict]:
        return self._get("/orders", date=date)

    def get_sellers(self) -> list[dict]:
        return self._get("/sellers")

    def get_products(self) -> list[dict]:
        return self._get("/products")

    def get_customers(self) -> list[dict]:
        return self._get("/customers")

    def post_webhook(self, payload: dict) -> None:
        """Webhook simulé côté API : notification d'anomalies."""
        resp = self.session.post(f"{self.base_url}/webhook", json=payload, timeout=30)
        resp.raise_for_status()
