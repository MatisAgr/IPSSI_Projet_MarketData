-- Migration pour les DWH déjà initialisés avant l'ajout de dwh.dim_category
-- (init_dwh.sql ne rejoue que sur un volume Postgres vierge).
-- Application : docker compose exec -T postgres-dwh psql -U dwh -d dwh -f - < sql/migrations/001_dim_category.sql

CREATE TABLE IF NOT EXISTS dwh.dim_category (
    category           text PRIMARY KEY,
    department         text,
    margin_target_pct  numeric(5, 1),
    is_seasonal        boolean
);
