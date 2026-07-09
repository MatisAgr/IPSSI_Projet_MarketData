-- Initialisation du Data Warehouse : 3 schémas (staging -> dwh -> analytics)

CREATE SCHEMA staging;
CREATE SCHEMA dwh;
CREATE SCHEMA analytics;

-- Staging : données brutes chargées depuis Garage (S3)
CREATE TABLE staging.sellers (
    seller_id   text,
    name        text,
    country     text,
    joined_date date
);

CREATE TABLE staging.products (
    product_id text,
    name       text,
    category   text,
    base_price numeric(10, 2)
);

CREATE TABLE staging.customers (
    customer_id text,
    email       text,
    city        text,
    signup_date date
);

CREATE TABLE staging.orders (
    order_id     text,
    seller_id    text,
    customer_id  text,
    product_id   text,
    quantity     int,
    unit_price   numeric(10, 2),
    total_amount numeric(10, 2),
    status       text,
    order_ts     timestamp,
    dt           date
);

-- DWH : modèle dimensionnel (étoile)
CREATE TABLE dwh.dim_seller (
    seller_id   text PRIMARY KEY,
    name        text,
    country     text,
    joined_date date
);

CREATE TABLE dwh.dim_customer (
    customer_id text PRIMARY KEY,
    email       text,
    city        text,
    signup_date date
);

CREATE TABLE dwh.dim_product (
    product_id text PRIMARY KEY,
    name       text,
    category   text,
    base_price numeric(10, 2)
);

CREATE TABLE dwh.dim_date (
    dt          date PRIMARY KEY,
    year        int,
    month       int,
    day_of_week int
);

CREATE TABLE dwh.fact_orders (
    order_id     text PRIMARY KEY,
    seller_id    text REFERENCES dwh.dim_seller,
    customer_id  text REFERENCES dwh.dim_customer,
    product_id   text REFERENCES dwh.dim_product,
    dt           date REFERENCES dwh.dim_date,
    quantity     int,
    total_amount numeric(10, 2),
    status       text
);

-- Analytics : tables d'agrégation lues par le dashboard Streamlit
CREATE TABLE analytics.daily_revenue (
    dt           date PRIMARY KEY,
    orders_count int,
    revenue      numeric(12, 2)
);

CREATE TABLE analytics.seller_revenue (
    dt           date,
    seller_id    text,
    seller_name  text,
    orders_count int,
    revenue      numeric(12, 2),
    PRIMARY KEY (dt, seller_id)
);

CREATE TABLE analytics.category_sales (
    dt       date,
    category text,
    quantity int,
    revenue  numeric(12, 2),
    PRIMARY KEY (dt, category)
);

CREATE TABLE analytics.customer_activity (
    dt                date PRIMARY KEY,
    active_customers  int,
    dormant_customers int
);

CREATE TABLE analytics.anomalies (
    dt          date,
    seller_id   text,
    seller_name text,
    revenue     numeric(12, 2),
    avg_7d      numeric(12, 2),
    drop_pct    numeric(5, 1),
    detected_at timestamp DEFAULT now(),
    PRIMARY KEY (dt, seller_id)
);

-- Segmentation clients RFM (snapshot recalculé en entier à chaque run, pas de partition dt)
CREATE TABLE analytics.customer_rfm (
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
