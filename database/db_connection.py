"""
Database connection manager and utilities for PostgreSQL Data Warehouse.
Provides connection pooling, transaction execution, and SQLite fallback for local testing.
"""
import os
import sqlite3
from contextlib import contextmanager
from typing import Generator, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.pool import QueuePool
from config.settings import settings
from config.logging_config import setup_logger

logger = setup_logger("database")


class DatabaseManager:
    """Manages connection pools and database operations."""

    def __init__(self, connection_uri: str = None):
        self.connection_uri = connection_uri or settings.postgres_connection_uri
        self._engine: Engine | None = None
        self._is_sqlite_fallback: bool = False

    def _check_postgres_available(self, timeout: float = 0.3) -> bool:
        """Quickly tests if PostgreSQL host and port are listening."""
        import socket
        try:
            with socket.create_connection((settings.POSTGRES_HOST, settings.POSTGRES_PORT), timeout=timeout):
                return True
        except Exception:
            return False

    @property
    def engine(self) -> Engine:
        """Lazy load and return SQLAlchemy engine with connection pooling."""
        if self._engine is None:
            # Refresh connection URI from current settings
            target_uri = self.connection_uri or settings.postgres_connection_uri

            try:
                # Attempt direct PostgreSQL connection with pool pre-ping
                engine = create_engine(
                    target_uri,
                    poolclass=QueuePool,
                    pool_size=settings.POSTGRES_MAX_CONNECTIONS,
                    max_overflow=10,
                    pool_timeout=10,
                    pool_recycle=1800,
                    pool_pre_ping=True,
                    connect_args={"connect_timeout": 8}
                )
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                self._engine = engine
                self._is_sqlite_fallback = False
                logger.info(f"Connected successfully to PostgreSQL Data Warehouse.")
                return self._engine
            except Exception as e:
                logger.warning(f"Could not establish PostgreSQL connection ({e}). Initializing fallback SQLite database for local execution.")
                fallback_db_path = os.path.join(os.path.dirname(__file__), "datapulse_local.db")
                self.connection_uri = f"sqlite:///{fallback_db_path}"
                self._engine = create_engine(self.connection_uri, connect_args={"check_same_thread": False})
                self._is_sqlite_fallback = True
                self._init_sqlite_schema(fallback_db_path)
        return self._engine

    def _init_sqlite_schema(self, db_path: str):
        """Initializes SQLite schema compatible with PostgreSQL DDL for local runs."""
        sqlite_schema = """
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT NOT NULL,
            segment TEXT DEFAULT 'Standard',
            city TEXT,
            country TEXT,
            signup_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dim_products (
            product_id TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT,
            base_price REAL NOT NULL,
            cost_price REAL NOT NULL,
            supplier_id TEXT,
            inventory_count INTEGER DEFAULT 1000,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dim_locations (
            location_id TEXT PRIMARY KEY,
            city TEXT NOT NULL,
            state TEXT,
            country TEXT NOT NULL,
            postal_code TEXT,
            region TEXT,
            latitude REAL,
            longitude REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dim_payment_methods (
            payment_method_id TEXT PRIMARY KEY,
            method_type TEXT NOT NULL,
            provider TEXT,
            is_active INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS dim_dates (
            date_key INTEGER PRIMARY KEY,
            full_date TEXT NOT NULL UNIQUE,
            day_of_week INTEGER NOT NULL,
            day_name TEXT NOT NULL,
            month_num INTEGER NOT NULL,
            month_name TEXT NOT NULL,
            quarter INTEGER NOT NULL,
            year INTEGER NOT NULL,
            is_weekend INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            customer_id TEXT NOT NULL,
            location_id TEXT NOT NULL,
            payment_method_id TEXT NOT NULL,
            order_status TEXT NOT NULL,
            total_amount REAL NOT NULL,
            discount_amount REAL DEFAULT 0.0,
            tax_amount REAL DEFAULT 0.0,
            net_amount REAL NOT NULL,
            item_count INTEGER NOT NULL,
            order_timestamp TIMESTAMP NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ingest_latency_ms INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS fact_order_items (
            order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            total_item_price REAL NOT NULL,
            discount_applied REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS quarantine_orders (
            quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT,
            order_id TEXT,
            raw_payload TEXT NOT NULL,
            error_code TEXT NOT NULL,
            rejection_reason TEXT NOT NULL,
            failed_validation_rule TEXT NOT NULL,
            quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS pipeline_health_metrics (
            metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            component TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL,
            metric_unit TEXT NOT NULL,
            status_indicator TEXT DEFAULT 'HEALTHY',
            details TEXT
        );

        CREATE TABLE IF NOT EXISTS data_quality_audit_log (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            check_name TEXT NOT NULL,
            check_type TEXT NOT NULL,
            table_audited TEXT NOT NULL,
            records_evaluated INTEGER NOT NULL,
            records_passed INTEGER NOT NULL,
            records_failed INTEGER NOT NULL,
            pass_rate REAL NOT NULL,
            status TEXT NOT NULL,
            audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            details TEXT
        );

        CREATE TABLE IF NOT EXISTS hourly_order_aggregates (
            hourly_window_start TEXT PRIMARY KEY,
            hourly_window_end TEXT NOT NULL,
            total_orders INTEGER NOT NULL DEFAULT 0,
            total_revenue REAL NOT NULL DEFAULT 0.0,
            avg_order_value REAL NOT NULL DEFAULT 0.0,
            total_items_sold INTEGER NOT NULL DEFAULT 0,
            successful_orders INTEGER NOT NULL DEFAULT 0,
            cancelled_orders INTEGER NOT NULL DEFAULT 0,
            quarantined_orders INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        with sqlite3.connect(db_path) as conn:
            conn.executescript(sqlite_schema)
        logger.info(f"Initialized local fallback database schema at {db_path}")

    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """Context manager for acquiring and safely closing connections."""
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    def execute_query(self, query: str, params: dict = None) -> list[dict[str, Any]]:
        """Executes a SELECT query and returns rows as dictionaries."""
        with self.get_connection() as conn:
            result = conn.execute(text(query), params or {})
            if result.returns_rows:
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
            return []

    def execute_statement(self, statement: str, params: dict = None) -> int:
        """Executes an INSERT/UPDATE/DELETE statement with auto-commit."""
        with self.engine.begin() as conn:
            result = conn.execute(text(statement), params or {})
            return result.rowcount

    def check_health(self) -> dict[str, Any]:
        """Performs database health ping and measures query latency."""
        import time
        start = time.time()
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            latency_ms = (time.time() - start) * 1000
            return {
                "status": "HEALTHY",
                "engine": "sqlite" if self._is_sqlite_fallback else "postgresql",
                "latency_ms": round(latency_ms, 2),
                "is_fallback": self._is_sqlite_fallback
            }
        except Exception as e:
            return {
                "status": "CRITICAL",
                "error": str(e),
                "latency_ms": None,
                "is_fallback": self._is_sqlite_fallback
            }


# Singleton database instance
db_manager = DatabaseManager()
