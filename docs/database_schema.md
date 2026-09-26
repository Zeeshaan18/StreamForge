# DataPulse Live — Database Schema & Data Warehouse Modeling

## 1. Dimensional Star Schema Design

```mermaid
erDiagram
    fact_orders }|..|| dim_customers : "belongs to"
    fact_orders }|..|| dim_locations : "ships to"
    fact_orders }|..|| dim_payment_methods : "paid via"
    fact_order_items }|..|| fact_orders : "contains"
    fact_order_items }|..|| dim_products : "references"

    dim_customers {
        varchar customer_id PK
        varchar customer_name
        varchar email
        varchar segment
        varchar city
        varchar country
        timestamp signup_date
        boolean is_active
    }

    dim_products {
        varchar product_id PK
        varchar product_name
        varchar category
        varchar subcategory
        numeric base_price
        numeric cost_price
        varchar supplier_id
        int inventory_count
    }

    dim_locations {
        varchar location_id PK
        varchar city
        varchar state
        varchar country
        varchar postal_code
        varchar region
        numeric latitude
        numeric longitude
    }

    dim_payment_methods {
        varchar payment_method_id PK
        varchar method_type
        varchar provider
        boolean is_active
    }

    fact_orders {
        varchar order_id PK
        varchar event_id UK
        varchar customer_id FK
        varchar location_id FK
        varchar payment_method_id FK
        varchar order_status
        numeric total_amount
        numeric discount_amount
        numeric tax_amount
        numeric net_amount
        int item_count
        timestamp order_timestamp
        timestamp processed_at
        bigint ingest_latency_ms
    }

    fact_order_items {
        bigserial order_item_id PK
        varchar order_id FK
        varchar product_id FK
        int quantity
        numeric unit_price
        numeric total_item_price
        numeric discount_applied
    }

    quarantine_orders {
        bigserial quarantine_id PK
        varchar event_id
        varchar order_id
        jsonb raw_payload
        varchar error_code
        text rejection_reason
        varchar failed_validation_rule
        timestamp quarantined_at
    }

    data_quality_audit_log {
        bigserial audit_id PK
        varchar check_name
        varchar check_type
        varchar table_audited
        bigint records_evaluated
        bigint records_passed
        bigint records_failed
        numeric pass_rate
        varchar status
        timestamp audit_timestamp
        jsonb details
    }

    hourly_order_aggregates {
        timestamp hourly_window_start PK
        timestamp hourly_window_end
        bigint total_orders
        numeric total_revenue
        numeric avg_order_value
        bigint total_items_sold
        bigint successful_orders
        bigint cancelled_orders
        bigint quarantined_orders
        timestamp updated_at
    }
```

---

## 2. Table Data Dictionary

### Dimension Tables

| Table Name | Primary Key | Description |
| :--- | :--- | :--- |
| `dim_customers` | `customer_id` | Master customer profile with tiers (VIP, Enterprise, Regular, Standard) and registration timestamps. |
| `dim_products` | `product_id` | Catalog SKUs with categories (Electronics, Apparel, Home & Kitchen), base pricing, and supplier info. |
| `dim_locations` | `location_id` | Geographical fulfillment zones with latitude/longitude coordinates and region classifications. |
| `dim_payment_methods` | `payment_method_id` | Supported payment options (Credit Card, PayPal, Apple Pay, BNPL, Crypto). |
| `dim_dates` | `date_key` | Conformed calendar dimension with ISO weeks, months, quarters, and weekend flags. |

### Fact & Audit Tables

| Table Name | Grain | Description |
| :--- | :--- | :--- |
| `fact_orders` | One row per order transaction | Core transactional fact with foreign keys to dimensions, gross/net financial calculations, and latency. |
| `fact_order_items` | One row per basket line item | Detailed line items linking individual SKUs, quantities, and line item discounts. |
| `quarantine_orders` | One row per rejected event | Dead-letter storage holding raw JSON payloads, violation error codes, and audit reasons. |
| `data_quality_audit_log` | One row per Airflow DQ check execution | Historical record of null checks, referential integrity tests, and pass rates. |
| `pipeline_health_metrics` | One row per telemetry sample | Time-series operational metric storage for Spark batch latency, consumer lag, and row counts. |
| `hourly_order_aggregates` | One row per 1-hour temporal window | Analytical rollup mart for rapid reporting queries. |
