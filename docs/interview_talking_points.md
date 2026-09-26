# DataPulse Live — Interview Talking Points & Resume Bullets

## 1. Resume Project Bullets

- **DataPulse Live: Real-Time E-Commerce Streaming Platform** *(Python, PySpark, Apache Kafka, Apache Airflow, PostgreSQL, Streamlit, Docker)*
  - Engineered an end-to-end real-time streaming data platform processing continuous e-commerce order events with sub-second latency using **PySpark Structured Streaming** and **Apache Kafka**.
  - Designed a robust **PostgreSQL Star Schema** (`fact_orders`, `fact_order_items`, conformed dimensions) with **idempotent upserts** preventing duplicate writes during network retries and stream re-runs.
  - Implemented an automated **Quarantine & Dead-Letter Architecture** routing corrupted/anomalous payloads to a dedicated inspection store with granular error classification codes.
  - Orchestrated automated **Data Quality Auditing & Reconciliation DAGs** in **Apache Airflow** validating primary/foreign key constraints, financial arithmetic consistency, and incremental hourly summary rollups.
  - Built a 5-page interactive **Streamlit + Plotly** operational analytics dashboard displaying live GMV trajectories, global order heatmaps, consumer lag, and pipeline infrastructure health metrics.
  - Authored a containerized multi-service **Docker Compose** topology with persistent volumes, service health checks, and 100% test coverage across unit and end-to-end smoke test suites.

---

## 2. Key System Design Trade-Offs & Questions

### Q1: Why PySpark Structured Streaming over Apache Flink?
> **Answer**: PySpark Structured Streaming provides a unified API between streaming and batch workloads, seamless integration with Python ML/analytics libraries, robust micro-batching for high-throughput relational upserts, and native checkpointing mechanisms. For sub-second micro-batching requirements, Spark Structured Streaming offers optimal throughput and operational simplicity.

### Q2: How did you ensure idempotency and prevent duplicate records during stream replays?
> **Answer**: We achieved end-to-end idempotency at both the streaming and warehouse layers:
> 1. At the event layer, every event has an immutable business `order_id` and unique `event_id`.
> 2. At the database layer, we used PostgreSQL transactional upserts (`INSERT ... ON CONFLICT (order_id) DO UPDATE SET order_status = EXCLUDED.order_status, processed_at = EXCLUDED.processed_at`).
> 3. For child line items in `fact_order_items`, writes are executed transactionally in the same micro-batch block.

### Q3: How do you handle schema drift and corrupted events in the stream?
> **Answer**: Rather than letting corrupted events crash the streaming job (silent failure or executor crash), we enforce an explicit schema and run validation rules before warehouse commits. Records with null foreign keys, negative amounts, or malformed structures are segregated into a `quarantine_orders` table with diagnostic error codes and raw JSON payloads for offline inspection and replay.

### Q4: Why use an Hourly Aggregate Mart (`hourly_order_aggregates`) instead of querying `fact_orders` directly?
> **Answer**: High-concurrency executive dashboards querying millions of rows in `fact_orders` create unnecessary database load. By using Airflow to pre-aggregate hourly metrics into a summary mart table, analytical queries execute in single-digit milliseconds regardless of warehouse volume.
