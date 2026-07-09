#!/usr/bin/env bash
# Exécute les tests pytest (Custom Operator) dans le conteneur airflow, où
# apache-airflow est déjà installé. pytest est installé à la volée (pas de rebuild).
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T airflow bash -c "
  pip install --quiet --user -r /opt/airflow/tests/requirements-test.txt &&
  cd /opt/airflow &&
  python -m pytest tests -v
"
