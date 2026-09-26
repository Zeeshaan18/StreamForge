# ⚡ DataPulse Live: Real-Time E-Commerce Data Engineering Platform

[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-231F20.svg?logo=apachekafka)](https://kafka.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.1-E25A1C.svg?logo=apachespark)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE.svg?logo=apacheairflow)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg?logo=postgresql)](https://www.postgresql.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.35-FF4B4B.svg?logo=streamlit)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker)](https://www.docker.com/)

**DataPulse Live** is an enterprise-grade real-time streaming data engineering platform. It continuously generates synthetic e-commerce order transactions, streams them through **Apache Kafka**, cleanses and validates them in real-time with **PySpark Structured Streaming**, persists them into an idempotent **PostgreSQL Star Schema**, orchestrates automated Data Quality audits with **Apache Airflow**, and visualizes live streaming metrics on a **Streamlit + Plotly** multi-page dashboard.

---

## 🏛️ System Architecture

```mermaid
flowchart LR
    subgraph Ingestion["1. Ingestion"]
        GEN["Continuous Generator<br/>(Faker / Configurable Rate)"] -->|JSON Payloads| KAFKA["Apache Kafka<br/>Topics: raw_orders, dead_letter"]
    end

    subgraph Processing["2. Stream Processing"]
        KAFKA -->|Continuous Stream| SPARK["PySpark Structured Streaming<br/>- Schema Enforcement<br/>- Validation Rules<br/>- Deduplication"]
    end

    subgraph Storage["3. Storage (PostgreSQL Star Schema)"]
        SPARK -->|Valid Events (Upsert)| FACT["fact_orders & fact_order_items"]
        SPARK -->|Quarantine Routing| QUARANTINE["quarantine_orders"]
        SPARK -->|Batch Telemetry| METRICS["pipeline_health_metrics"]
    end

    subgraph Orchestration["4. Orchestration (Airflow)"]
        AIRFLOW["Apache Airflow DAGs<br/>- DQ Reconciliation<br/>- Hourly Aggregate Mart"] --> Storage
    end

    subgraph Analytics["5. Live Visualization (Streamlit)"]
        Storage --> DASH["Streamlit + Plotly Dashboard<br/>- Executive Overview<br/>- Live Order Monitor<br/>- Pipeline Telemetry<br/>- Data Quality Dashboard<br/>- Historical Analytics"]
    end
```

---

## ✨ Key Features

- **Continuous Synthetic Generation**: Produces unlimited unique e-commerce transactions (`ORD-YYYYMMDD-XXXX`) with realistic customer profiles, multi-item baskets, tax logic, and controlled anomaly injection (4%).
- **PySpark Stream Processing**: Real-time schema enforcement, micro-batch processing, ingestion latency tracking, and dead-letter quarantine routing.
- **Idempotent Data Warehouse**: PostgreSQL Star Schema with `ON CONFLICT (order_id) DO UPDATE` to prevent duplicates during retries.
- **Automated Airflow Orchestration**:
  - `datapulse_dq_reconciliation_dag`: Audits null constraints, referential integrity orphans, and mathematical reconciliation.
  - `datapulse_hourly_aggregate_rollup_dag`: Builds analytical summary rollups for fast reporting.
  - `datapulse_pipeline_health_dag`: Tracks storage growth, query latency, and health indicators.
- **5-Page Live Streamlit Dashboard**:
  1. `Executive Overview`: GMV trajectory, AOV, category revenue, payment method breakdowns.
  2. `Live Order Monitor`: Real-time streaming table feed with auto-refresh, global geographic map, and line item inspector.
  3. `Pipeline Monitoring`: Live status cards for Kafka, Spark, Airflow, and PostgreSQL warehouse with latency distribution metrics.
  4. `Data Quality Dashboard`: Validation pass rate gauge, quarantine failure code bar charts, and raw payload inspector.
  5. `Historical Analytics`: Customer tier cohort spending, order status breakdown, and analytical marts.
- **Full Automated Testing**: Comprehensive `pytest` test suite and end-to-end smoke test verifying the complete data pipeline contract.

---

## 🚀 Quick Start Guide

### Option A: Local Bare-Metal Run (Python 3.10+)

```bash
# 1. Clone repository
git clone https://github.com/your-username/DataPulse-Live.git
cd DataPulse-Live

# 2. Install dependencies
pip install -r requirements.txt

# 3. Seed warehouse dimension tables
python -m database.seed_dimensions

# 4. Run automated test suite
pytest tests/ -v

# 5. Run end-to-end smoke test
python -m tests.test_smoke_e2e

# 6. Launch the Streamlit Live Dashboard
streamlit run dashboard/app.py
```

### Option B: Docker Compose Multi-Container Stack

```bash
# 1. Copy environment configuration
cp .env.example .env

# 2. Start all services in background
docker compose up -d --build

# 3. View running containers
docker compose ps
```

### Access Ports & Services
| Component | URL | Port |
| :--- | :--- | :--- |
| **Streamlit Dashboard** | `http://localhost:8501` | `8501` |
| **Apache Airflow Webserver** | `http://localhost:8080` | `8080` |
| **PostgreSQL Data Warehouse** | `localhost:5432` | `5432` |
| **Apache Kafka Broker** | `localhost:9092` | `9092` |

---

## 📁 Repository Structure

```
StreamForge/
├── .env.example                     # Environment template
├── .gitignore                        # Git ignore rules
├── README.md                         # Project documentation
├── docker-compose.yml                # Docker Compose multi-service spec
├── requirements.txt                  # Production dependencies
├── requirements-dev.txt              # Development & test dependencies
├── config/                           # Application configuration & logging
│   ├── settings.py
│   └── logging_config.py
├── database/                         # Data Warehouse Layer
│   ├── init_schema.sql               # PostgreSQL Star Schema DDL
│   ├── db_connection.py              # Connection pool & failover manager
│   └── seed_dimensions.py            # Master reference dimension seeder
├── generator/                        # Synthetic Stream Generator
│   ├── schema.py                     # Pydantic schemas
│   ├── order_generator.py            # Continuous event generator
│   └── kafka_producer.py             # Resilient Kafka producer
├── spark/                            # Stream Processing Engine
│   ├── stream_processor.py           # PySpark Structured Streaming processor
│   ├── validation.py                 # Data quality validation & quarantine rules
│   ├── transformations.py            # Dimension enrichment & latency computation
│   └── postgres_writer.py            # Idempotent database writer
├── airflow/                          # Orchestration Layer
│   ├── dags/                         # Scheduled DAGs (DQ, Rollup, Health)
│   └── plugins/                      # Custom operators & audit runners
├── dashboard/                        # Streamlit Live Analytics
│   ├── app.py                        # Main entry point
│   ├── pages/                        # Multi-page views (1 to 5)
│   ├── components/                   # UI styling & Plotly visualizers
│   └── db_queries.py                 # Analytical query layer
├── tests/                            # Automated Testing Suite
│   ├── test_generator.py             # Generator uniqueness & schema tests
│   ├── test_validation.py            # Data quality rule tests
│   ├── test_spark_transformations.py # Transformation tests
│   ├── test_idempotent_writes.py     # Idempotency & quarantine tests
│   └── test_smoke_e2e.py             # Full end-to-end smoke test
└── docs/                             # Technical Portfolio Documentation
    ├── architecture.md               # Architecture deep dive
    ├── database_schema.md            # Schema ERD & data dictionary
    ├── deployment_guide.md           # Local & cloud deployment guide
    └── interview_talking_points.md   # Resume bullets & interview Q&A
```

---

## 🧪 Testing & Validation

Run all unit, integration, and smoke tests:
```bash
pytest tests/ -v
```

Output:
```
tests/test_generator.py::test_generator_produces_unique_ids PASSED       [  6%]
tests/test_generator.py::test_valid_order_schema_conformance PASSED      [ 12%]
tests/test_generator.py::test_intentional_anomaly_injection PASSED       [ 18%]
tests/test_generator.py::test_duplicate_emission_simulation PASSED       [ 25%]
tests/test_idempotent_writes.py::test_idempotent_upsert_prevents_duplicates PASSED [ 31%]
tests/test_idempotent_writes.py::test_quarantine_routing PASSED          [ 37%]
tests/test_smoke_e2e.py::test_end_to_end_pipeline_flow PASSED            [ 43%]
tests/test_spark_transformations.py::test_transformation_structure PASSED [ 50%]
tests/test_validation.py::test_valid_event_passes PASSED                 [ 56%]
tests/test_validation.py::test_null_event_id_fails PASSED                [ 62%]
tests/test_validation.py::test_null_customer_id_fails PASSED             [ 68%]
tests/test_validation.py::test_negative_price_fails PASSED               [ 75%]
tests/test_validation.py::test_invalid_status_fails PASSED               [ 81%]
tests/test_validation.py::test_invalid_payment_method_fails PASSED       [ 87%]
tests/test_validation.py::test_empty_items_fails PASSED                  [ 93%]
tests/test_validation.py::test_future_timestamp_fails PASSED             [100%]
====================== 16 passed in 3.99s =======================
```

---

## 📜 License
This project is licensed under the MIT License.
