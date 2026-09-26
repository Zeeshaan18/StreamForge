"""
Global configuration settings for DataPulse Live using Pydantic.
Supports loading from environment variables, .env file, and Cloud Platform configurations (e.g. Render).
"""
import os
from typing import Optional
from urllib.parse import urlparse
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # General Environment
    APP_ENV: str = Field(default="development", description="Runtime environment")
    LOG_LEVEL: str = Field(default="INFO", description="Logging verbosity")
    APP_NAME: str = Field(default="DataPulse Live", description="Application Title")

    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092")
    KAFKA_RAW_ORDERS_TOPIC: str = Field(default="raw_orders")
    KAFKA_DEAD_LETTER_TOPIC: str = Field(default="dead_letter_orders")
    KAFKA_CONSUMER_GROUP: str = Field(default="datapulse-spark-consumer")
    KAFKA_AUTO_OFFSET_RESET: str = Field(default="latest")
    KAFKA_SECURITY_PROTOCOL: str = Field(default="PLAINTEXT", description="PLAINTEXT, SASL_SSL, SASL_PLAINTEXT, SSL")
    KAFKA_SASL_MECHANISM: str = Field(default="PLAIN", description="PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
    KAFKA_SASL_USERNAME: Optional[str] = Field(default=None, description="Kafka SASL Username for Cloud Brokers")
    KAFKA_SASL_PASSWORD: Optional[str] = Field(default=None, description="Kafka SASL Password for Cloud Brokers")

    # PostgreSQL Data Warehouse Settings
    DATABASE_URL: Optional[str] = Field(default=None, description="Complete PostgreSQL connection URI (e.g., from Render)")
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_DB: str = Field(default="datapulse_warehouse")
    POSTGRES_USER: str = Field(default="datapulse_user")
    POSTGRES_PASSWORD: str = Field(default="datapulse_secure_password")
    POSTGRES_MAX_CONNECTIONS: int = Field(default=20)

    # Generator Settings
    GENERATOR_RATE_PER_SECOND: float = Field(default=5.0, description="Events emitted per second")
    GENERATOR_BURST_PROBABILITY: float = Field(default=0.08, description="Probability of sudden spike")
    GENERATOR_ERROR_INJECTION_RATE: float = Field(default=0.04, description="Rate of corrupt/invalid events")
    GENERATOR_DUPLICATE_RATE: float = Field(default=0.02, description="Rate of intentional duplicate events")

    # Spark Streaming Settings
    SPARK_APP_NAME: str = Field(default="DataPulse-StreamProcessor")
    SPARK_MASTER: str = Field(default="local[*]")
    SPARK_BATCH_DURATION_SECONDS: int = Field(default=3)
    SPARK_CHECKPOINT_DIR: str = Field(default="/tmp/spark-checkpoints/datapulse")

    # Airflow Settings
    AIRFLOW_WEBSERVER_PORT: int = Field(default=8080)
    
    # Streamlit Dashboard Settings
    PORT: Optional[int] = Field(default=None, description="Render dynamic port override")
    STREAMLIT_SERVER_PORT: int = Field(default=8501)
    STREAMLIT_AUTO_REFRESH_INTERVAL: int = Field(default=3)

    @property
    def server_port(self) -> int:
        """Returns active dashboard port, prioritizing Render's PORT environment variable."""
        if self.PORT is not None:
            return self.PORT
        return self.STREAMLIT_SERVER_PORT

    @property
    def postgres_connection_uri(self) -> str:
        """Returns standard PostgreSQL connection string for SQLAlchemy/psycopg2."""
        raw_url = self.DATABASE_URL or os.environ.get("DATABASE_URL")
        if raw_url:
            # Handle Render/Heroku postgres:// vs postgresql:// prefix for SQLAlchemy
            if raw_url.startswith("postgres://"):
                return raw_url.replace("postgres://", "postgresql+psycopg2://", 1)
            elif raw_url.startswith("postgresql://"):
                return raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)
            return raw_url

        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def postgres_jdbc_url(self) -> str:
        """Returns JDBC URL for PySpark PostgreSQL connector."""
        raw_url = self.DATABASE_URL or os.environ.get("DATABASE_URL")
        if raw_url:
            clean = raw_url.replace("postgresql+psycopg2://", "postgresql://").replace("postgres://", "postgresql://")
            parsed = urlparse(clean)
            host = parsed.hostname or self.POSTGRES_HOST
            port = parsed.port or self.POSTGRES_PORT
            db = parsed.path.lstrip("/") or self.POSTGRES_DB
            return f"jdbc:postgresql://{host}:{port}/{db}"

        return f"jdbc:postgresql://{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"


# Singleton configuration instance
settings = Settings()
