-- Migration pour les DWH initialisés avant l'ajout de analytics.customer_rfm
-- (table déjà présente dans sql/init_dwh.sql, manquante sur les volumes Postgres
-- créés avant que cette définition n'existe).
-- Application : docker compose exec -T postgres-dwh psql -U dwh -d dwh -f - < sql/migrations/002_customer_rfm.sql

CREATE TABLE IF NOT EXISTS analytics.customer_rfm (
    customer_id    text PRIMARY KEY,
    customer_email text,
    city           text,
    calculated_at  date,
    recency_days   int,
    frequency      int,
    monetary       numeric(12, 2),
    r_score        int,
    f_score        int,
    m_score        int,
    segment        text
);
