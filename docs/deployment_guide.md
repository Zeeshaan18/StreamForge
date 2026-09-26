# DataPulse Live — Deployment & Operations Guide

## 1. Local Deployment (Docker Compose)

### Prerequisites
- Docker Engine 24.0+ and Docker Compose v2
- 4GB+ RAM allocated to Docker

### Starting the Full Stack
```bash
# 1. Clone repository
git clone https://github.com/your-username/DataPulse-Live.git
cd DataPulse-Live

# 2. Copy environment configuration
cp .env.example .env

# 3. Build and launch containers
docker compose up -d --build

# 4. Verify running services
docker compose ps
```

### Accessing Endpoints
| Component | URL | Credentials |
| :--- | :--- | :--- |
| **Streamlit Live Dashboard** | `http://localhost:8501` | None |
| **Apache Airflow Web UI** | `http://localhost:8080` | `airflow` / `airflow` |
| **PostgreSQL Warehouse** | `localhost:5432` | `datapulse_user` / `datapulse_secure_password` |
| **Kafka Broker** | `localhost:9092` | None |

---

## 2. Standalone Host Execution (Without Docker)

DataPulse Live includes built-in fallback mechanisms so the complete pipeline and dashboard can run on bare-metal Python environments without external dependencies.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Seed warehouse dimension tables
python -m database.seed_dimensions

# 3. Run automated tests
pytest tests/ -v

# 4. Run end-to-end smoke test
python -m tests.test_smoke_e2e

# 5. Start Streamlit Live Dashboard
streamlit run dashboard/app.py
```

---

## 3. Cloud Deployment Blueprint (AWS / GCP Production)

### AWS Reference Architecture
- **Ingestion**: Amazon Managed Streaming for Apache Kafka (Amazon MSK) with multi-AZ replication.
- **Compute / Stream Processing**: Amazon EMR on EKS running PySpark Structured Streaming with S3 checkpointing.
- **Warehouse**: Amazon RDS for PostgreSQL (Multi-AZ) or Amazon Redshift Serverless.
- **Orchestration**: Managed Workflows for Apache Airflow (Amazon MWAA).
- **Dashboard**: AWS ECS Fargate hosting the Streamlit container behind an Application Load Balancer (ALB).
- **Security & Secrets**: AWS Secrets Manager and IAM Roles for Service Accounts (IRSA).
