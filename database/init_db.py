"""
Database Initialization Utility for StreamForge Data Warehouse.
Applies Star Schema DDL safely (IF NOT EXISTS) and seeds dimension tables.
Can be executed as a pre-deploy or initial setup script in cloud environments (Render, etc.).
"""
import os
import sys
from pathlib import Path

# Ensure root directory is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from database.db_connection import db_manager
from database.seed_dimensions import seed_all_dimensions
from config.logging_config import setup_logger

logger = setup_logger("db_init")


def initialize_database():
    """Initializes PostgreSQL warehouse schema and seeds master dimension data."""
    logger.info("Initializing Data Warehouse schema...")
    
    schema_path = PROJECT_ROOT / "database" / "init_schema.sql"
    if not schema_path.exists():
        logger.error(f"Schema file not found at: {schema_path}")
        return False

    with open(schema_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Execute SQL statements
    with db_manager.engine.begin() as conn:
        # Split statements by semicolon where appropriate or execute statements
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]
        for stmt in statements:
            # Skip extension creations if non-superuser permissions on managed cloud databases
            if "CREATE EXTENSION" in stmt.upper():
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    logger.warning(f"Skipping extension command ({stmt[:30]}...): {e}")
                continue

            try:
                conn.execute(text(stmt))
            except Exception as e:
                logger.error(f"Error executing statement:\n{stmt}\nError: {e}")
                raise

    logger.info("Database schema applied successfully.")

    # Seed dimension tables
    try:
        seed_all_dimensions()
        logger.info("Dimension reference data seeded successfully.")
    except Exception as e:
        logger.error(f"Error seeding dimensions: {e}")
        raise

    return True


if __name__ == "__main__":
    try:
        initialize_database()
        logger.info("Database initialization completed successfully.")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)
