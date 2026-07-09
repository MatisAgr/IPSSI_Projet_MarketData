# MarketPlace Analytics — Projet A (option 2)

Pipeline **ELT batch idempotent** pour une marketplace e-commerce :
`API Flask simulée → Garage (S3) → PostgreSQL DWH → Dashboard Streamlit + détection d'anomalies`.

> Conformément à la consigne, **MinIO est remplacé par [Garage](https://garagehq.deuxfleurs.fr/)**
> (stockage objet compatible S3), et le groupe a choisi l'**option 2**
> (Streamlit + détection d'anomalies), pas Metabase.

## Démarrage rapide

```bash
docker compose up -d --build
```

Les DAGs sont dépausés automatiquement : le catchup rejoue **~3 mois d'historique** depuis le
`start_date` (2026-04-08), largement assez pour la moyenne mobile 7 jours et des courbes lisibles.

| Service            | URL                          | Rôle                                        |
| ------------------ | ---------------------------- | ------------------------------------------- |
| Airflow 3.1.8      | http://localhost:8080        | Orchestrateur (login désactivé pour le TP)  |
| Streamlit          | http://localhost:8501        | Dashboard KPIs + anomalies                  |
| API marketplace    | http://localhost:5000        | API Flask simulée (auth Bearer, cf. `.env`) |
| Garage S3          | http://localhost:3920        | Stockage objet, bucket `raw`                |
| Garage WebUI       | http://localhost:3909        | Interface web pour explorer Garage          |
| PostgreSQL DWH     | localhost:5434 (`dwh`/`dwh`) | Schémas `staging` / `dwh` / `analytics`     |
| PostgreSQL Airflow | localhost:5432               | Metadata DB                                 |

## Architecture globale

```mermaid
flowchart LR
    api[("API Marketplace<br/>Flask simulée")]
    airflow[["Airflow 3.1.8<br/>DAGs ELT"]]
    garage[("Garage S3<br/>raw/ partition dt=")]
    pg[("PostgreSQL DWH<br/>staging / dwh / analytics")]
    st["Streamlit<br/>KPIs + anomalies"]

    api -- "extract via Custom Hook" --> airflow
    airflow -- "raw JSON" --> garage
    airflow -- "load + transform" --> pg
    pg --> st

    style airflow fill:#4a90d9,color:#fff
    style pg fill:#f5a623,color:#000
    style st fill:#4caf50,color:#fff
```

## Architecture des DAGs

```mermaid
flowchart TD
    subgraph dag1["marketplace_orders_ingest_daily (@daily, catchup)"]
        e1["extract_to_raw"] --> e2["load_staging"] --> e3["dq_check_staging"] --> e4["publish"]
    end
    subgraph dag2["marketplace_dwh_build_daily"]
        d1["resolve_dts"] --> d2["build_dims_and_facts"] --> d3["dq_check_dwh"] --> d4["publish"]
    end
    subgraph dag3["marketplace_analytics_aggregate_daily"]
        a1["resolve_dts"] --> a2["aggregate"]
    end
    subgraph dag4["marketplace_anomaly_detect_daily"]
        n1["resolve_dts"] --> n2["detect"] --> n3{"anomalies ?"}
        n3 -- oui --> n4["notify_webhook"]
        n3 -- non --> n5["no_anomaly"]
    end

    e4 -- "Asset raw_orders" --> d1
    d4 -- "Asset dwh_orders" --> a1
    d4 -- "Asset dwh_orders" --> n1

    style dag1 fill:#e8f5e9
    style dag2 fill:#e3f2fd
    style dag3 fill:#fff3e0
    style dag4 fill:#fce4ec
```

## Modèle de données (étoile)

```mermaid
erDiagram
    DIM_SELLER ||--o{ FACT_ORDERS : vend
    DIM_CUSTOMER ||--o{ FACT_ORDERS : achete
    DIM_PRODUCT ||--o{ FACT_ORDERS : contient
    DIM_DATE ||--o{ FACT_ORDERS : datee

    DIM_SELLER {
        text seller_id PK
        text name
        text country
        date joined_date
    }
    DIM_CUSTOMER {
        text customer_id PK
        text email
        text city
        date signup_date
    }
    DIM_PRODUCT {
        text product_id PK
        text name
        text category
        numeric base_price
    }
    DIM_DATE {
        date dt PK
        int year
        int month
        int day_of_week
    }
    FACT_ORDERS {
        text order_id PK
        text seller_id FK
        text customer_id FK
        text product_id FK
        date dt FK
        int quantity
        numeric total_amount
        text status
    }
```

## Explorer ce qu'il y a sur Garage

Le plus simple : **Garage WebUI** sur http://localhost:3909 → onglet *Buckets* →
`raw` → *Browse* pour naviguer dans les objets (`orders/dt=2026-07-01/orders.json`, etc.)
et voir leur contenu.

En ligne de commande, deux alternatives :

```bash
# 1. CLI garage intégrée au conteneur
docker compose exec garage /garage bucket list
docker compose exec garage /garage bucket info raw

# 2. AWS CLI depuis l'hôte (clés dans .env, endpoint = port hôte 3920)
aws --endpoint-url http://localhost:3920 --region garage s3 ls s3://raw --recursive
aws --endpoint-url http://localhost:3920 --region garage s3 cp s3://raw/orders/dt=2026-07-01/orders.json -
```

(pour l'AWS CLI : `aws configure` avec `S3_ACCESS_KEY` / `S3_SECRET_KEY` du `.env`)

## Points demandés par le sujet

- **Custom Hook** : [`airflow/dags/lib/marketplace_hook.py`](airflow/dags/lib/marketplace_hook.py)
  — `MarketplaceAPIHook` avec auth Bearer et `get_orders(date)` / `get_sellers()` / `get_products()`.
- **Custom Operator** : [`airflow/dags/lib/data_quality.py`](airflow/dags/lib/data_quality.py)
  — `DataQualityOperator`, 5 règles SQL configurables sur le staging (+ 3 sur le DWH).
- **Idempotence** : pattern `DELETE + INSERT` sur la partition `dt = {{ ds }}` partout,
  dimensions en `INSERT ... ON CONFLICT DO UPDATE`, et l'API génère des données
  **déterministes par date** (seed = date). Relancer 3 fois la même date ⇒ même résultat.
- **Modèle en étoile** : [`sql/init_dwh.sql`](sql/init_dwh.sql) — 4 dimensions + `fact_orders`.
- **Anomalies (option 2)** : CA/vendeur du jour comparé à sa moyenne mobile 7 jours,
  flag si chute > 30 %, écriture dans `analytics.anomalies`, branchement vers un
  webhook simulé si anomalies.

## Tester l'idempotence

Dans l'UI Airflow, "Clear" un run de `marketplace_orders_ingest_daily` (même date)
plusieurs fois, puis :

```bash
docker compose exec postgres-dwh psql -U dwh -c \
  "SELECT dt, COUNT(*), SUM(total_amount) FROM dwh.fact_orders GROUP BY dt ORDER BY dt;"
```

Les compteurs et montants restent identiques à chaque relance.

## Fonctionnalités bonus

### Alerting Slack / webhook sur échec de DAG

[`airflow/dags/lib/alerting.py`](airflow/dags/lib/alerting.py) — `notify_dag_failure`, branché en
`on_failure_callback` sur les 4 DAGs (déclenché quand un DagRun passe en `failed`).
Si `ALERT_WEBHOOK_URL` (`.env`) pointe vers une Slack Incoming Webhook, l'alerte y est postée ;
sinon repli automatique sur le webhook simulé de l'API marketplace (même mécanisme que les
alertes d'anomalies), pour rester testable sans dépendance externe :

```bash
curl -s -H "Authorization: Bearer ipssi-marketplace-token" http://localhost:5000/webhook
```

### Tests pytest sur le Custom Operator

[`airflow/tests/test_data_quality_operator.py`](airflow/tests/test_data_quality_operator.py) —
`DataQualityOperator` testé avec `PostgresHook` mocké (aucun Postgres réel requis) :
règles `eq`/`gt`, violations, agrégation des échecs multiples, `conn_id` utilisé.

```bash
bash scripts/run-tests.sh
```

### Backfill manuel

[`scripts/backfill.sh`](scripts/backfill.sh) relance `marketplace_orders_ingest_daily` sur une
plage de dates via `airflow backfill create` ; les DAGs avals (`dwh_build`, `analytics_aggregate`,
`anomaly_detect`) se déclenchent automatiquement via leurs Assets, pas besoin de les lancer :

```bash
bash scripts/backfill.sh 2026-06-01 2026-06-07
```

⚠️ Le flag `--max-active-runs 1` est indispensable : `staging.sellers/products/customers` sont
rechargées en `TRUNCATE` + `INSERT` complet (non partitionnées par `dt`), donc deux runs
d'ingestion concurrents se marchent dessus. Le `backfill create` d'Airflow a sa propre
concurrence, indépendante du `max_active_runs=1` défini sur le DAG.

### `dim_category` enrichie (référentiel externe)

[`airflow/dags/lib/category_reference.py`](airflow/dags/lib/category_reference.py) simule un
mapping externe (taxonomie business) chargé par la tâche `build_dim_category` du DAG
`marketplace_dwh_build_daily`, qui peuple `dwh.dim_category` (`department`, `margin_target_pct`,
`is_seasonal`) en upsert. Une règle DQ (`categories_couvertes`) vérifie que toute catégorie de
`dim_product` a bien une correspondance dans `dim_category`.

Sur un DWH déjà initialisé (volume Postgres existant), la table ne se crée pas toute seule
(`init_dwh.sql` ne rejoue que sur un volume vierge) — appliquer la migration :

```bash
docker compose exec -T postgres-dwh psql -U dwh -d dwh -f - < sql/migrations/001_dim_category.sql
```

## Structure du projet

```
├── docker-compose.yml        # 8 services : airflow, 2 postgres, garage (+init +webui), api, streamlit
├── .env                      # secrets du TP (jetons, clés S3, mots de passe)
├── api/                      # API marketplace simulée (Flask, Bearer, webhook)
├── garage/                   # config Garage + script d'init (layout, clé, bucket)
├── sql/init_dwh.sql          # création des schémas staging / dwh / analytics
├── sql/migrations/           # migrations pour les DWH déjà initialisés
├── scripts/                  # backfill.sh, run-tests.sh
├── airflow/dags/             # les 4 DAGs
│   └── lib/                  # Custom Hook, Custom Operator, Assets, alerting, référentiel catégories
├── airflow/tests/            # tests pytest (Custom Operator, DB mockée)
└── dashboard/                # app Streamlit (option 2)
```

Chaque image custom (`api/`, `dashboard/`) a son propre `requirements.txt`.
