"""
Autonomous Real-Time Streaming Pipeline Supervisor for StreamForge.
Runs a lightweight, non-blocking background streaming ingestion & processing loop
inside the web application to ensure continuous real-time order flow into PostgreSQL
even in single-container cloud deployments (e.g. Render Web Service).
"""
import os
import time
import threading
from datetime import datetime, timezone
from typing import List, Dict, Any

from config.settings import settings
from config.logging_config import setup_logger
from database.db_connection import db_manager
from generator.order_generator import ContinuousOrderGenerator
from spark.postgres_writer import postgres_stream_writer

logger = setup_logger("pipeline_runner")

_pipeline_thread: threading.Thread | None = None
_pipeline_running = False
_pipeline_lock = threading.Lock()


class BackgroundPipelineSupervisor:
    """Manages continuous background generation and ingestion into PostgreSQL."""

    def __init__(self, rate_per_sec: float = None, batch_duration_sec: int = None):
        self.rate_per_sec = rate_per_sec or settings.GENERATOR_RATE_PER_SECOND
        self.batch_duration_sec = batch_duration_sec or settings.SPARK_BATCH_DURATION_SECONDS
        self.generator = ContinuousOrderGenerator()
        self.is_running = False

    def is_external_pipeline_active(self) -> bool:
        """Checks if a separate external worker (Spark/Generator) is actively persisting micro-batches."""
        try:
            # Check if any orders or health metrics have been recorded in the last 12 seconds
            recent_metric = db_manager.execute_query("""
                SELECT timestamp 
                FROM pipeline_health_metrics 
                WHERE component = 'SPARK' AND timestamp >= NOW() - INTERVAL '12 seconds'
                LIMIT 1
            """)
            if recent_metric:
                return True
        except Exception:
            pass
        return False

    def run_loop(self):
        """Continuous micro-batch execution loop."""
        self.is_running = True
        logger.info(f"Background Streaming Pipeline Supervisor started (Target Velocity: {self.rate_per_sec} eps, Window: {self.batch_duration_sec}s)...")
        
        batch_counter = 0

        while self.is_running:
            start_time = time.time()
            
            try:
                # If an external dedicated worker is already actively processing, yield to avoid duplicate generation
                if not self.is_external_pipeline_active():
                    event_count = max(1, int(self.rate_per_sec * self.batch_duration_sec))
                    events: List[Dict[str, Any]] = [
                        self.generator.generate_next_event() for _ in range(event_count)
                    ]

                    # Commit micro-batch to PostgreSQL
                    stats = postgres_stream_writer.write_micro_batch(events)
                    batch_counter += 1
                    
                    if batch_counter % 5 == 0 or batch_counter == 1:
                        logger.info(
                            f"Live Stream Micro-Batch #{batch_counter} Committed: "
                            f"Valid={stats['valid']} | Quarantined={stats['quarantined']} | "
                            f"Duplicates Handled={stats['duplicates']}"
                        )
            except Exception as e:
                logger.warning(f"Background stream processor loop note: {e}")

            elapsed = time.time() - start_time
            sleep_time = max(0.2, self.batch_duration_sec - elapsed)
            time.sleep(sleep_time)

        logger.info("Background Streaming Pipeline Supervisor stopped.")

    def stop(self):
        self.is_running = False


def ensure_background_pipeline_running():
    """Starts background pipeline supervisor thread if not already running."""
    global _pipeline_thread, _pipeline_running

    with _pipeline_lock:
        if _pipeline_running and _pipeline_thread and _pipeline_thread.is_alive():
            return

        supervisor = BackgroundPipelineSupervisor()
        _pipeline_thread = threading.Thread(target=supervisor.run_loop, daemon=True, name="StreamForgePipelineThread")
        _pipeline_thread.start()
        _pipeline_running = True
        logger.info("Spawned autonomous pipeline background daemon thread.")
