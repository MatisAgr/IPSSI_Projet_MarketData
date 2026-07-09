"""Alerting sur échec de DAG : Slack Incoming Webhook si ALERT_WEBHOOK_URL est
configuré, sinon repli sur le webhook simulé de l'API marketplace (même mécanisme
que les alertes d'anomalies, pour rester testable sans dépendance externe)."""
import os

import requests

from lib.marketplace_hook import MarketplaceAPIHook

ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL")


def notify_dag_failure(context):
    """on_failure_callback DAG-level (déclenché quand le DagRun passe en failed)."""
    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    logical_date = str(context.get("logical_date"))
    message = f"Échec du DAG `{dag_id}` (run_id={run_id}, logical_date={logical_date})"

    if ALERT_WEBHOOK_URL:
        try:
            requests.post(ALERT_WEBHOOK_URL, json={"text": f":rotating_light: {message}"}, timeout=10) \
                .raise_for_status()
            return
        except Exception as exc:
            print(f"[alerting] Échec envoi Slack ({exc}), repli sur le webhook simulé")

    try:
        MarketplaceAPIHook().post_webhook({
            "alert": "dag_failure", "dag_id": dag_id, "run_id": run_id, "logical_date": logical_date,
        })
    except Exception as exc:
        print(f"[alerting] Échec envoi webhook simulé : {exc}")
