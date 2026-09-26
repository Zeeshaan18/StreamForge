-- ====================================================================
-- DataPulse Live - PostgreSQL Star Schema & Data Warehouse DDL
-- ====================================================================

-- 1. Create Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 2. Star Schema & Tables Initialization (Safe IF NOT EXISTS)

-- ====================================================================
-- DIMENSION TABLES
-- ====================================================================

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    customer_name VARCHAR(150) NOT NULL,
    email VARCHAR(200) NOT NULL,
    segment VARCHAR(50) DEFAULT 'Standard',  -- VIP, Regular, Enterprise, Standard
    city VARCHAR(100),
    country VARCHAR(100),
    signup_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dim_customers_segment ON dim_customers(segment);
CREATE INDEX IF NOT EXISTS idx_dim_customers_country ON dim_customers(country);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(64) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    base_price NUMERIC(12, 2) NOT NULL CHECK (base_price >= 0),
    cost_price NUMERIC(12, 2) NOT NULL CHECK (cost_price >= 0),
    supplier_id VARCHAR(64),
    inventory_count INT DEFAULT 1000,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dim_products_category ON dim_products(category);
CREATE INDEX IF NOT EXISTS idx_dim_products_price ON dim_products(base_price);

-- Dimension: Locations
CREATE TABLE IF NOT EXISTS dim_locations (
    location_id VARCHAR(64) PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100),
    country VARCHAR(100) NOT NULL,
    postal_code VARCHAR(30),
    region VARCHAR(50), -- North America, EMEA, APAC, LATAM
    latitude NUMERIC(9, 6),
    longitude NUMERIC(9, 6),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_dim_locations_region ON dim_locations(region);
CREATE INDEX IF NOT EXISTS idx_dim_locations_country ON dim_locations(country);

-- Dimension: Payment Methods
CREATE TABLE IF NOT EXISTS dim_payment_methods (
    payment_method_id VARCHAR(50) PRIMARY KEY,
    method_type VARCHAR(50) NOT NULL, -- Credit Card, Debit Card, PayPal, Apple Pay, Crypto, BNPL
    provider VARCHAR(100),            -- Visa, Mastercard, Stripe, PayPal, BitPay, Klarna
    is_active BOOLEAN DEFAULT TRUE
);

-- Dimension: Dates
CREATE TABLE IF NOT EXISTS dim_dates (
    date_key INT PRIMARY KEY,         -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL,         -- 1-7
    day_name VARCHAR(20) NOT NULL,
    month_num INT NOT NULL,           -- 1-12
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL,             -- 1-4
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dim_dates_full_date ON dim_dates(full_date);

-- ====================================================================
-- FACT TABLES
-- ====================================================================

-- Fact: Orders (Grain: One row per order event)
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    customer_id VARCHAR(64) NOT NULL REFERENCES dim_customers(customer_id) ON DELETE CASCADE,
    location_id VARCHAR(64) NOT NULL REFERENCES dim_locations(location_id) ON DELETE CASCADE,
    payment_method_id VARCHAR(50) NOT NULL REFERENCES dim_payment_methods(payment_method_id),
    order_status VARCHAR(50) NOT NULL, -- PENDING, PROCESSING, SHIPPED, DELIVERED, CANCELLED, REFUNDED
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    discount_amount NUMERIC(12, 2) DEFAULT 0.00 CHECK (discount_amount >= 0),
    tax_amount NUMERIC(12, 2) DEFAULT 0.00 CHECK (tax_amount >= 0),
    net_amount NUMERIC(12, 2) NOT NULL CHECK (net_amount >= 0),
    item_count INT NOT NULL CHECK (item_count > 0),
    order_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingest_latency_ms BIGINT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_fact_orders_event_id ON fact_orders(event_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_cust_id ON fact_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_timestamp ON fact_orders(order_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON fact_orders(order_status);
CREATE INDEX IF NOT EXISTS idx_fact_orders_location ON fact_orders(location_id);

-- Fact: Order Items (Grain: Line items within an order)
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL REFERENCES fact_orders(order_id) ON DELETE CASCADE,
    product_id VARCHAR(64) NOT NULL REFERENCES dim_products(product_id) ON DELETE CASCADE,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    total_item_price NUMERIC(12, 2) NOT NULL CHECK (total_item_price >= 0),
    discount_applied NUMERIC(12, 2) DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_order_id ON fact_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_product_id ON fact_order_items(product_id);

-- ====================================================================
-- QUARANTINE & DATA QUALITY TABLES
-- ====================================================================

-- Quarantine: Rejected / Invalid Events
CREATE TABLE IF NOT EXISTS quarantine_orders (
    quarantine_id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100),
    order_id VARCHAR(100),
    raw_payload JSONB NOT NULL,
    error_code VARCHAR(100) NOT NULL,          -- NULL_FIELD, NEGATIVE_PRICE, INVALID_STATUS, CORRUPT_SCHEMA
    rejection_reason TEXT NOT NULL,
    failed_validation_rule VARCHAR(100) NOT NULL,
    quarantined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_quarantine_error_code ON quarantine_orders(error_code);
CREATE INDEX IF NOT EXISTS idx_quarantine_timestamp ON quarantine_orders(quarantined_at);

-- Data Quality Audit Log (Populated by Airflow DQ DAG)
CREATE TABLE IF NOT EXISTS data_quality_audit_log (
    audit_id BIGSERIAL PRIMARY KEY,
    check_name VARCHAR(150) NOT NULL,
    check_type VARCHAR(100) NOT NULL,         -- NULL_CHECK, UNIQUENESS, REFERENTIAL_INTEGRITY, ANOMALY
    table_audited VARCHAR(100) NOT NULL,
    records_evaluated BIGINT NOT NULL,
    records_passed BIGINT NOT NULL,
    records_failed BIGINT NOT NULL,
    pass_rate NUMERIC(6, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,              -- PASSED, FAILED, WARNING
    audit_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);
CREATE INDEX IF NOT EXISTS idx_dq_audit_table ON data_quality_audit_log(table_audited);
CREATE INDEX IF NOT EXISTS idx_dq_audit_timestamp ON data_quality_audit_log(audit_timestamp);

-- Pipeline Health Metrics (Populated by Spark & Airflow Heartbeats)
CREATE TABLE IF NOT EXISTS pipeline_health_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    component VARCHAR(50) NOT NULL,           -- KAFKA, SPARK, POSTGRES, AIRFLOW, GENERATOR
    metric_name VARCHAR(100) NOT NULL,        -- THROUGHPUT_EPS, BATCH_LATENCY_MS, CONSUMER_LAG, DB_CONNECTIONS
    metric_value NUMERIC(14, 4) NOT NULL,
    metric_unit VARCHAR(30) NOT NULL,         -- eps, ms, count, mb, percent
    status_indicator VARCHAR(20) DEFAULT 'HEALTHY', -- HEALTHY, DEGRADED, CRITICAL
    details JSONB
);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_component ON pipeline_health_metrics(component);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_timestamp ON pipeline_health_metrics(timestamp);

-- Hourly Order Aggregates (Rollup Table for High-Speed Analytics)
CREATE TABLE IF NOT EXISTS hourly_order_aggregates (
    hourly_window_start TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    hourly_window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    total_orders BIGINT NOT NULL DEFAULT 0,
    total_revenue NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    avg_order_value NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    total_items_sold BIGINT NOT NULL DEFAULT 0,
    successful_orders BIGINT NOT NULL DEFAULT 0,
    cancelled_orders BIGINT NOT NULL DEFAULT 0,
    quarantined_orders BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_hourly_agg_start ON hourly_order_aggregates(hourly_window_start);

