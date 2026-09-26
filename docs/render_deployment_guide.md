# StreamForge — Render Cloud Deployment Guide

This guide provides end-to-end instructions for deploying the **StreamForge Real-Time Data Platform** to [Render](https://render.com).

---

## 🏛️ Cloud Architecture on Render

```
  ┌────────────────────────┐
  │  streamforge-generator │  (Background Worker - Docker)
  └───────────┬────────────┘
              │ Events (rate: 5/s)
              ▼
  ┌────────────────────────┐
  │   streamforge-kafka    │  (Private Service - Apache Kafka KRaft)
  └───────────┬────────────┘
              │ Streaming Micro-batches
              ▼
  ┌────────────────────────┐
  │stream-processor (Spark)│  (Background Worker - Docker)
  └───────────┬────────────┘
              │ Idempotent SQL Inserts (ON CONFLICT)
              ▼
  ┌────────────────────────┐
  │  datapulse-warehouse   │  (Render PostgreSQL Managed Database)
  └───────────┬────────────┘
              │ Analytical Queries
              ▼
  ┌────────────────────────┐
  │  streamforge-dashboard │  (Web Service - Streamlit Dashboard)
  └────────────────────────┘
```

---

## 🚀 Option 1: 1-Click Deploy via Render Blueprint (`render.yaml`)

StreamForge includes a pre-configured `render.yaml` Blueprint file at the root of the repository that automatically wires up all 5 services, internal networks, environment variables, and database connections.

### Steps:
1. **Push your code to your GitHub repository** (`StreamForge`).
2. Log in to [Render Dashboard](https://dashboard.render.com).
3. Click the **"New +"** button at the top right and select **"Blueprint"**.
4. Connect your GitHub account and select your **`StreamForge`** repository.
5. Render will automatically detect `render.yaml` and display the blueprint plan:
   - `datapulse-warehouse` (PostgreSQL Database)
   - `streamforge-dashboard` (Web Service)
   - `streamforge-generator` (Background Worker)
   - `streamforge-stream-processor` (Background Worker)
   - `streamforge-kafka` (Private Service)
6. Click **"Apply"** / **"Create Blueprint"**.
7. Render will build and deploy all services simultaneously.

---

## 🛠️ Option 2: Step-by-Step Manual Deployment on Render

If you prefer to configure each service manually in the Render UI:

### Step 1: Create Managed PostgreSQL Database
1. In Render Dashboard, click **New +** → **PostgreSQL**.
2. Configure:
   - **Name**: `datapulse-warehouse`
   - **Database Name**: `datapulse_warehouse`
   - **User**: `datapulse_user`
   - **Region**: Select your preferred region (e.g. `Oregon (US West)` or `Frankfurt (EU)`)
   - **Plan**: `Free`
3. Click **Create Database**.
4. Once created, copy the **Internal Database URL** (e.g., `postgresql://datapulse_user:...@dpg-xxxx-a:5432/datapulse_warehouse`).

---

### Step 2: Create Kafka Broker (Private Service)
1. Click **New +** → **Private Service**.
2. Connect your `StreamForge` repository (or select "Deploy an existing image").
3. If using Docker Image:
   - **Image URL**: `apache/kafka:3.7.0`
   - **Name**: `streamforge-kafka`
   - **Region**: Same as your database.
4. Add Environment Variables:
   - `KAFKA_NODE_ID`: `1`
   - `KAFKA_PROCESS_ROLES`: `broker,controller`
   - `KAFKA_LISTENERS`: `PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093`
   - `KAFKA_ADVERTISED_LISTENERS`: `PLAINTEXT://streamforge-kafka:9092`
   - `KAFKA_CONTROLLER_LISTENER_NAMES`: `CONTROLLER`
   - `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`: `CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT`
   - `KAFKA_CONTROLLER_QUORUM_VOTERS`: `1@localhost:9093`
   - `KAFKA_AUTO_CREATE_TOPICS_ENABLE`: `true`
   - `KAFKA_LOG_DIRS`: `/tmp/kraft-combined-logs`
   - `CLUSTER_ID`: `4L62EnnTTMuzxq1WoE43Aw`
5. Click **Create Private Service**.

*(Alternative: You can also use a free managed Kafka cluster such as [Upstash Kafka](https://upstash.com/kafka) or [Aiven](https://aiven.io) and provide its bootstrap URL and SASL credentials).*

---

### Step 3: Create Streamlit Dashboard (Web Service)
1. Click **New +** → **Web Service**.
2. Connect your `StreamForge` repository.
3. Configure:
   - **Name**: `streamforge-dashboard`
   - **Runtime**: `Docker`
   - **Dockerfile Path**: `dashboard/Dockerfile`
   - **Health Check Path**: `/_stcore/health`
4. Add Environment Variables:
   - `DATABASE_URL`: *(Link to `datapulse-warehouse` or paste the Internal Database URL)*
   - `APP_ENV`: `production`
   - `STREAMLIT_AUTO_REFRESH_INTERVAL`: `3`
5. Click **Create Web Service**.

---

### Step 4: Create Synthetic Event Generator (Background Worker)
1. Click **New +** → **Background Worker**.
2. Connect your `StreamForge` repository.
3. Configure:
   - **Name**: `streamforge-generator`
   - **Runtime**: `Docker`
   - **Dockerfile Path**: `generator/Dockerfile`
4. Add Environment Variables:
   - `KAFKA_BOOTSTRAP_SERVERS`: `streamforge-kafka:9092`
   - `KAFKA_RAW_ORDERS_TOPIC`: `raw_orders`
   - `DATABASE_URL`: *(Link to `datapulse-warehouse`)*
   - `GENERATOR_RATE_PER_SECOND`: `5.0`
   - `GENERATOR_BURST_PROBABILITY`: `0.08`
   - `GENERATOR_ERROR_INJECTION_RATE`: `0.04`
5. Click **Create Background Worker**.

---

### Step 5: Create Stream Processor (Background Worker)
1. Click **New +** → **Background Worker**.
2. Connect your `StreamForge` repository.
3. Configure:
   - **Name**: `streamforge-stream-processor`
   - **Runtime**: `Docker`
   - **Dockerfile Path**: `spark/Dockerfile`
4. Add Environment Variables:
   - `KAFKA_BOOTSTRAP_SERVERS`: `streamforge-kafka:9092`
   - `KAFKA_RAW_ORDERS_TOPIC`: `raw_orders`
   - `KAFKA_CONSUMER_GROUP`: `datapulse-spark-consumer`
   - `DATABASE_URL`: *(Link to `datapulse-warehouse`)*
   - `SPARK_BATCH_DURATION_SECONDS`: `3`
5. Click **Create Background Worker**.

---

## 🗄️ Initializing Schema & Master Dimensions on Render Database

When your Render PostgreSQL instance is created, you can initialize the Star Schema and seed reference dimension tables using either:

### Method A: Connect from local machine using External Database URL
```bash
# In your local StreamForge directory:
$env:DATABASE_URL="<Paste Render External Connection String>"
python -m database.init_db
```

### Method B: Render Web Service Shell
1. Go to your `streamforge-dashboard` service on Render.
2. Click the **"Shell"** tab.
3. Run:
   ```bash
   python -m database.init_db
   ```
4. This will create all dimension and fact tables (with `IF NOT EXISTS`) and seed customer profiles, product catalog, payment methods, and locations.

---

## 🌐 Verification Checklist

- [ ] **Streamlit Dashboard URL**: Accessible publicly at `https://streamforge-dashboard.onrender.com`.
- [ ] **Service Health Indicator**: Shows `🟢 HEALTHY (POSTGRESQL)` in the sidebar.
- [ ] **Real-Time Counters**: Total Orders, Gross Revenue, and Order Rate incrementing live.
- [ ] **Live Order Feed**: Displays new stream batches every 3 seconds.
- [ ] **Pipeline Telemetry & Data Quality**: Metrics recorded in `pipeline_health_metrics` and `quarantine_orders`.
