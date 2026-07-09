#!/usr/bin/env bash
# Backfill manuel d'une plage de dates sur le DAG d'ingestion. Les DAGs avals
# (dwh_build, analytics_aggregate, anomaly_detect) se déclenchent automatiquement
# via leurs Assets à chaque run d'ingestion backfillé (pas besoin de les lancer).
#
# Usage : scripts/backfill.sh 2026-06-01 2026-06-07
set -euo pipefail
cd "$(dirname "$0")/.."

FROM_DATE="${1:?Usage: scripts/backfill.sh YYYY-MM-DD YYYY-MM-DD}"
TO_DATE="${2:?Usage: scripts/backfill.sh YYYY-MM-DD YYYY-MM-DD}"

## --max-active-runs 1 est indispensable : staging.sellers/products/customers sont
## rechargées en TRUNCATE + INSERT complet (non partitionnées par dt), donc deux
## runs d'ingestion concurrents se marchent dessus. Le `backfill create` d'Airflow
## a sa propre concurrence, indépendante du max_active_runs=1 défini sur le DAG.
docker compose exec airflow airflow backfill create \
  --dag-id marketplace_orders_ingest_daily \
  --from-date "${FROM_DATE}" \
  --to-date "${TO_DATE}" \
  --max-active-runs 1 \
  --reprocess-behavior completed
