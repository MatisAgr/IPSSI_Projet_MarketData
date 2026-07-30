# MarketPlace Analytics — Projet A (option 2)

**Lambda Architecture** complète pour une marketplace e-commerce : une couche batch idempotente
(historique, fait foi) et une couche temps réel (latence de quelques secondes) qui consomment
**le même log d'évènements**, et convergent dans une couche de service polystore.

> Conformément à la consigne, **MinIO est remplacé par [Garage](https://garagehq.deuxfleurs.fr/)**
> (stockage objet compatible S3), et le groupe a choisi l'**option 2**
> (Streamlit + détection d'anomalies), pas Metabase.

## Démarrage rapide

```bash
docker compose up -d --build
```

Les DAGs sont dépausés automatiquement : le catchup rejoue **~3 mois d'historique** depuis le
`start_date` (2026-04-08). En parallèle, le producteur alimente le log Kafka en continu :
le dashboard affiche du temps réel au bout de ~1 min, et les premières anomalies live après ~3 min
(le temps de constituer la baseline de 10 min).

| Service            | URL                          | Rôle                                        |
| ------------------ | ---------------------------- | ------------------------------------------- |
| Airflow 3.1.8      | http://localhost:8080        | Orchestrateur (login désactivé pour le TP)  |
| Streamlit          | http://localhost:8501        | Dashboard : temps réel + historique batch   |
| Redpanda Console   | http://localhost:8090        | Explorer le log Kafka (topics, groupes)     |
| API marketplace    | http://localhost:5000        | API Flask simulée (auth Bearer, cf. `.env`) |
| Garage WebUI       | http://localhost:3910        | Interface web pour explorer le data lake    |
| Garage S3          | http://localhost:3920        | Stockage objet, bucket `raw`                |
| Kafka (Redpanda)   | localhost:19092              | Broker compatible Kafka, sans ZooKeeper     |
| Redis              | localhost:6379               | Agrégats temps réel du speed layer          |
| PostgreSQL DWH     | localhost:5434 (`dwh`/`dwh`) | Schémas `staging` / `dwh` / `analytics`     |
| PostgreSQL Airflow | localhost:5432               | Metadata DB                                 |

## Avant / après

### Avant — ELT batch pur

Un seul chemin, une seule latence : tout attend le run quotidien. Airflow **tire** les données de
l'API, dépose du JSON sur Garage, le relit, et charge un unique PostgreSQL.

```mermaid
flowchart LR
    api[("API Marketplace<br/>Flask")]
    af[["Airflow<br/>4 DAGs @daily"]]
    gr[("Garage S3<br/>JSON")]
    pg[("PostgreSQL DWH<br/>staging / dwh / analytics")]
    st["Streamlit"]

    api -- "pull 1x/jour" --> af
    af -- "upload raw" --> gr
    gr -- "load staging" --> af
    af -- "transform" --> pg
    pg --> st

    style af fill:#4a90d9,color:#fff
    style pg fill:#f5a623,color:#000
```

> Détection d'une chute de CA : **jusqu'à 24 h de retard**. Aucun flux, un seul moteur de
> stockage, format texte non typé — les 3V ne sont pas adressés.

### Après — Lambda Architecture

Le log Kafka devient le point d'entrée, et **deux chemins indépendants** en partent : l'un
optimisé pour la latence, l'autre pour l'exactitude.

```mermaid
flowchart LR
    api[("API Marketplace<br/>Flask")]
    prod["producer"]
    kafka[("Redpanda<br/>topic orders")]

    subgraph sp["⚡ Speed layer — 2 s"]
        cons["speed-consumer"]
        rd[("Redis")]
    end

    subgraph bt["📦 Batch layer — fait foi"]
        arch["archiver"]
        gr[("Garage S3<br/>Parquet")]
        af[["Airflow<br/>5 DAGs"]]
        pg[("PostgreSQL DWH")]
    end

    st["Streamlit<br/>les 2 vues"]

    api --> prod --> kafka
    kafka -- "groupe speed-layer" --> cons --> rd --> st
    kafka -- "groupe archiver" --> arch --> gr
    api -- "backfill" --> af
    gr --> af --> pg --> st

    style kafka fill:#8b5cf6,color:#fff
    style sp fill:#fce4ec
    style bt fill:#e3f2fd
```

> Détection d'une chute de CA : **~2 secondes** en direct, confirmée par le batch qui fait foi.

### Ce que ça change, V par V

| | Avant | Après |
|---|---|---|
| **Volume** | JSON texte, un seul PostgreSQL calcule tout | Parquet colonne compressé, lac rejouable prêt pour Spark/Trino |
| **Velocity** | ❌ aucune — tout est `@daily` | ✅ log Kafka + fenêtre glissante, latence ~2 s |
| **Variety** | ❌ 3 schémas du **même** Postgres | ✅ polystore : relationnel (Postgres) + clé-valeur (Redis) + objet (Garage) |

## Architecture globale (Lambda)

```mermaid
flowchart LR
    api[("API Marketplace<br/>Flask simulée")]
    prod["producer<br/>flux de commandes"]
    kafka[("Redpanda<br/>topic orders, 3 partitions")]

    subgraph speed["⚡ Speed layer — secondes"]
        cons["speed-consumer<br/>fenêtre glissante 60s"]
        redis[("Redis<br/>snapshot live")]
    end

    subgraph batchl["📦 Batch layer — 24h, fait foi"]
        arch["archiver<br/>groupe Kafka distinct"]
        garage[("Garage S3<br/>Parquet partitionné dt=")]
        airflow[["Airflow 3.1.8<br/>5 DAGs"]]
        pg[("PostgreSQL DWH<br/>staging / dwh / analytics")]
    end

    st["Streamlit<br/>les deux vues"]

    api --> prod --> kafka
    kafka --> cons --> redis --> st
    kafka --> arch --> garage
    api -- "backfill historique" --> airflow
    airflow <--> garage
    airflow --> pg --> st

    style kafka fill:#8b5cf6,color:#fff
    style speed fill:#fce4ec
    style batchl fill:#e3f2fd
```

Le point clé : `speed-consumer` et `archiver` sont **deux groupes de consommateurs Kafka
distincts** sur le même topic. Chacun lit donc **l'intégralité** du log, à son propre rythme et
avec ses propres garanties — c'est exactement ce que Lambda demande, et c'est impossible avec un
simple appel d'API point à point.

|                    | Speed layer                            | Batch layer                                           |
| ------------------ | -------------------------------------- | ----------------------------------------------------- |
| Latence            | ~2 s                                   | 24 h (ou 15 min pour la réconciliation)               |
| Offset de départ   | `latest` (le passé ne l'intéresse pas) | `earliest` (ne doit rien perdre)                      |
| Commit des offsets | automatique                            | **manuel, après écriture S3 réussie** (at-least-once) |
| État               | en mémoire, volatile                   | Parquet sur Garage, rejouable                         |
| Fait foi ?         | non, approximation                     | **oui**                                               |

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

    subgraph dag5["marketplace_stream_reconcile (*/15 min)"]
        r1["reconcile<br/>relit le Parquet archivé"]
    end

    e4 -- "Asset raw_orders" --> d1
    d4 -- "Asset dwh_orders" --> a1
    d4 -- "Asset dwh_orders" --> n1

    style dag1 fill:#e8f5e9
    style dag2 fill:#e3f2fd
    style dag3 fill:#fff3e0
    style dag4 fill:#fce4ec
    style dag5 fill:#ede9fe
```

Les DAGs 1→4 forment la chaîne ELT historique, chaînée par **Assets**. Le DAG 5 est indépendant :
il ferme la boucle Lambda en recalculant, depuis le log archivé, la vérité batch que le dashboard
compare aux agrégats du speed layer.

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

## Le data lake sur Garage

Deux arborescences, toutes deux en **Parquet** (format colonne compressé Snappy) :

```
raw/
├── orders/dt=2026-07-09/orders.parquet        # snapshot batch quotidien (via l'API)
├── sellers|products|customers/dt=…/….parquet  # référentiels
└── events/dt=2026-07-09/part-<ts>.parquet     # log d'évènements archivé depuis Kafka
```

Le plus simple pour explorer : **Garage WebUI** sur http://localhost:3910 → *Buckets* → `raw` →
*Browse*. En ligne de commande :

```bash
# CLI garage intégrée au conteneur
docker compose exec garage /garage bucket list
docker compose exec garage /garage bucket info raw

# AWS CLI depuis l'hôte (clés dans .env, endpoint = port hôte 3920)
aws --endpoint-url http://localhost:3920 --region garage s3 ls s3://raw/events/ --recursive
```

Lire un Parquet sans rien installer (l'image Airflow embarque pyarrow) :

```bash
docker compose exec airflow python -c "
import io, pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
s3 = S3Hook(aws_conn_id='garage_s3')
key = s3.list_keys(bucket_name='raw', prefix='events/')[0]
print(pq.read_table(io.BytesIO(s3.get_key(key, 'raw').get()['Body'].read())).to_pandas().head())"
```

### Pourquoi Parquet et pas JSON

|                       | JSON                    | Parquet                               |
| --------------------- | ----------------------- | ------------------------------------- |
| Stockage              | texte, non compressé    | colonne + Snappy (~5-10× plus petit)  |
| Lecture d'une colonne | relire tout le fichier  | ne lit que la colonne demandée        |
| Typage                | aucun (tout est string) | schéma embarqué                       |
| Écosystème            | universel mais lent     | natif Spark / Trino / DuckDB / Pandas |

## Couche Big Data — le streaming

Trois services dans [`streaming/`](streaming/), une seule image, trois commandes :

| Service          | Fichier                                            | Rôle                                                                                                                                                                                                    |
| ---------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `producer`       | [`producer.py`](streaming/producer.py)             | Simule le flux de commandes, publie sur Kafka. Clé = `seller_id` (ordre garanti par vendeur, charge répartie entre partitions). Provoque des pannes vendeur aléatoires pour générer de vraies anomalies |
| `speed-consumer` | [`speed_consumer.py`](streaming/speed_consumer.py) | Fenêtre glissante 60 s vs baseline 10 min, détecte les chutes > 30 %, publie un snapshot dans Redis toutes les 2 s (avec TTL : si le consumer meurt, le dashboard cesse d'afficher du faux « live »)    |
| `archiver`       | [`archiver.py`](streaming/archiver.py)             | Même topic, **groupe distinct**, offsets commités **manuellement après** l'écriture S3 → aucune perte d'évènement                                                                                       |

Observer le log en direct :

```bash
# Via l'UI : http://localhost:8090 -> Topics -> orders
docker compose exec redpanda rpk topic consume orders --num 5
docker compose exec redpanda rpk group list          # speed-layer et archiver
docker compose exec redpanda rpk group describe archiver   # lag par partition
```

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

## Vérifier la boucle Lambda

Les deux couches calculent la même grandeur par deux chemins différents :

```bash
# Vue temps réel (Redis, état en mémoire du speed layer)
docker compose exec redis redis-cli GET live:snapshot

# Vérité batch (Postgres, recalculée depuis le Parquet archivé)
docker compose exec postgres-dwh psql -U dwh -c \
  "SELECT dt, SUM(orders_count) AS commandes, SUM(revenue) AS ca
   FROM analytics.stream_daily GROUP BY dt;"
```

Le compteur temps réel est **en avance** sur le batch : l'écart correspond aux évènements pas
encore archivés (flush toutes les 60 s) ni réconciliés (DAG toutes les 15 min). C'est exactement
le compromis latence/exactitude que Lambda formalise, et c'est visible dans le dashboard
(section *Boucle Lambda*).

### Simplifications assumées

- **État du speed layer en mémoire** : un redémarrage du consumer repart de `latest` et perd la
  fenêtre courante. En production : Kafka Streams ou Flink, avec state store persistant.
- **Archiveur en at-least-once** : un crash entre l'écriture S3 et le commit d'offset peut
  produire des doublons. Ce n'est pas un défaut à cacher — c'est précisément pourquoi Lambda
  garde une couche batch : le recompute complet du DAG 5 dédoublonne par construction.
- **Beaucoup de petits fichiers Parquet** (un par flush). À l'échelle, il faut une compaction
  périodique, ou un format de table (Iceberg / Delta Lake) qui la gère nativement.

## Passage à l'échelle

Tout tourne sur un PC portable, mais **aucun choix d'architecture ne bloque la montée en charge** :
chaque composant a un chemin de scale-out documenté.

| Composant          | Ici (1 PC)                                   | Millions d'utilisateurs                                                                         |
| ------------------ | -------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Log d'évènements   | Redpanda 1 nœud, 3 partitions, réplication 1 | Cluster 3–5 brokers, réplication 3, N partitions (N = parallélisme max des consommateurs)       |
| Speed layer        | 1 consumer Python                            | N consumers dans le **même groupe** (Kafka réassigne les partitions tout seul), état dans Flink |
| Data lake          | Garage 1 nœud                                | Garage multi-nœuds ou S3 managé — l'API est déjà identique                                      |
| Compute batch      | Airflow LocalExecutor + SQL PostgreSQL       | Spark ou Trino directement sur le Parquet, Airflow en KubernetesExecutor                        |
| Serving analytique | PostgreSQL                                   | ClickHouse / Druid (colonne, distribué) ou Iceberg + Trino                                      |
| Serving temps réel | Redis 1 nœud                                 | Redis Cluster, ou Cassandra / ScyllaDB pour les séries temporelles                              |

Le partitionnement par `seller_id` côté producteur est ce qui rend ce scale-out possible : les
commandes d'un même vendeur restent ordonnées dans une même partition, donc on peut multiplier
les consommateurs sans casser la cohérence des agrégats par vendeur.

### Ce qu'on n'a délibérément pas ajouté

**Spark.** À 18 000 commandes historiques, un cluster distribué ne calcule rien plus vite qu'un
`GROUP BY` PostgreSQL — ce serait une vitrine sans contenu. Le point de bascule se situe quand le
volume dépasse la RAM d'une machine (~100 Go de Parquet) ou quand le recompute batch dépasse sa
fenêtre horaire. Le lac étant **déjà en Parquet**, brancher Spark ou Trino ne demanderait aucune
migration de données.

**Cassandra, Neo4j, Trino.** Chacun résout un problème que ce projet n'a pas encore (écritures
massives, requêtes de graphe, fédération multi-sources). Une brique posée sans besoin est plus
pénalisante qu'une brique absente mais argumentée.

## Structure du projet

```
├── docker-compose.yml        # 15 services (batch + streaming), cf. tableau plus haut
├── .env                      # secrets + réglages du streaming (fenêtres, débit)
├── api/                      # API marketplace simulée (Flask, Bearer, webhook)
├── streaming/                # ⚡ couche temps réel — 1 image, 3 rôles
│   ├── producer.py           #    flux de commandes -> Kafka
│   ├── speed_consumer.py     #    fenêtre glissante + anomalies -> Redis
│   └── archiver.py           #    même log, groupe distinct -> Parquet sur Garage
├── garage/                   # config Garage + script d'init (layout, clé, bucket)
├── sql/init_dwh.sql          # schémas staging / dwh / analytics
├── sql/migrations/           # migrations pour les DWH déjà initialisés
├── scripts/                  # backfill.sh, run-tests.sh
├── airflow/Dockerfile        # image officielle + pyarrow (Parquet)
├── airflow/dags/             # les 5 DAGs
│   └── lib/                  # Custom Hook, Custom Operator, Assets, alerting, catégories
├── airflow/tests/            # tests pytest (Custom Operator, DB mockée)
└── dashboard/                # app Streamlit : temps réel + historique batch
```

Chaque image custom (`api/`, `dashboard/`, `streaming/`, `airflow/`) a son propre `requirements.txt`.

> **DWH déjà initialisé ?** `init_dwh.sql` ne rejoue que sur un volume vierge. Pour ajouter la
> table de la boucle Lambda sur une base existante :
> ```bash
> docker compose exec -T postgres-dwh psql -U dwh -d dwh -f - < sql/migrations/003_stream_daily.sql
> ```
