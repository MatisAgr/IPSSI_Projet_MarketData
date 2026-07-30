-- Migration 003 : vérité batch recalculée depuis le log d'évènements archivé (boucle Lambda).
-- À appliquer sur un DWH existant (init_dwh.sql ne s'exécute qu'au premier démarrage).

CREATE TABLE IF NOT EXISTS analytics.stream_daily (
    dt           date,
    seller_id    text,
    orders_count int,
    revenue      numeric(12, 2),
    computed_at  timestamp DEFAULT now(),
    PRIMARY KEY (dt, seller_id)
);
