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

Les DAGs sont dépausés automatiquement : le catchup rejoue l'historique depuis le
`start_date` (2026-07-01), ce qui donne assez de profondeur pour la moyenne mobile 7 jours.

| Service | URL | Rôle |
|---|---|---|
| Airflow 3.1.8 | http://localhost:8080 | Orchestrateur (login désactivé pour le TP) |
| Streamlit | http://localhost:8501 | Dashboard KPIs + anomalies |
| API marketplace | http://localhost:5000 | API Flask simulée (auth Bearer, cf. `.env`) |
| Garage S3 | http://localhost:3920 | Stockage objet, bucket `raw` |
| Garage WebUI | http://localhost:3909 | Interface web pour explorer Garage |
| PostgreSQL DWH | localhost:5434 (`dwh`/`dwh`) | Schémas `staging` / `dwh` / `analytics` |
| PostgreSQL Airflow | localhost:5432 | Metadata DB |

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

## Structure du projet

```
├── docker-compose.yml        # 8 services : airflow, 2 postgres, garage (+init +webui), api, streamlit
├── .env                      # secrets du TP (jetons, clés S3, mots de passe)
├── api/                      # API marketplace simulée (Flask, Bearer, webhook)
├── garage/                   # config Garage + script d'init (layout, clé, bucket)
├── sql/init_dwh.sql          # création des schémas staging / dwh / analytics
├── airflow/dags/             # les 4 DAGs
│   └── lib/                  # Custom Hook, Custom Operator, Assets partagés
└── dashboard/                # app Streamlit (option 2)
```

Chaque image custom (`api/`, `dashboard/`) a son propre `requirements.txt`.
