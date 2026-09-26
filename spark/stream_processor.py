"""
PySpark Structured Streaming & Resilient Stream Processing Engine.
Consumes continuous events from Kafka, applies validation, cleanses data,
and invokes idempotent warehouse persistence.
"""
import sys
import json
import time
import argparse
import signal
from typing import List, Dict, Any, Optional
from config.settings import settings
from config.logging_config import setup_logger
from spark.postgres_writer import postgres_stream_writer

logger = setup_logger("spark_processor")


class StreamProcessor:
    """Orchestrates stream processing execution."""

    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
        batch_duration_sec: int = None
    ):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or settings.KAFKA_RAW_ORDERS_TOPIC
        self.batch_duration_sec = batch_duration_sec or settings.SPARK_BATCH_DURATION_SECONDS
        self.running = False

    def run_pyspark_streaming_cluster(self):
        """
        Runs full PySpark Structured Streaming cluster job.
        Used inside Docker Compose / Cloud Spark cluster environments.
        """
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import from_json, col
            from spark.transformations import get_spark_order_schema

            logger.info("Initializing SparkSession for Structured Streaming...")
            spark = SparkSession.builder \
                .appName(settings.SPARK_APP_NAME) \
                .master(settings.SPARK_MASTER) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .getOrCreate()

            spark.sparkContext.setLogLevel("WARN")
            order_schema = get_spark_order_schema()

            logger.info(f"Subscribing to Kafka topic '{self.topic}' at {self.bootstrap_servers}...")
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.topic) \
                .option("startingOffsets", settings.KAFKA_AUTO_OFFSET_RESET) \
                .option("failOnDataLoss", "false") \
                .load()

            # Parse JSON value
            parsed_df = df.selectExpr("CAST(value AS STRING) as json_payload") \
                .select(from_json(col("json_payload"), order_schema).alias("data")) \
                .select("data.*")

            def process_micro_batch(batch_df, batch_id):
                logger.info(f"Processing PySpark Micro-Batch ID: {batch_id} (Rows: {batch_df.count()})")
                rows = batch_df.collect()
                events = [row.asDict(recursive=True) for row in rows]
                postgres_stream_writer.write_micro_batch(events)

            query = parsed_df.writeStream \
                .foreachBatch(process_micro_batch) \
                .trigger(processingTime=f"{self.batch_duration_sec} seconds") \
                .option("checkpointLocation", settings.SPARK_CHECKPOINT_DIR) \
                .start()

            logger.info("PySpark Structured Streaming query started successfully.")
            query.awaitTermination()

        except Exception as e:
            logger.warning(f"PySpark cluster streaming initialization failed ({e}). Falling back to native stream consumer.")
            self.run_native_stream_consumer()

    def _check_kafka_available(self, host: str, port: int, timeout: float = 2.0) -> bool:
        """Tests if Kafka broker host and port are reachable."""
        import socket
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            return False

    def run_native_stream_consumer(self, max_batches: int = None):
        """
        High-efficiency streaming consumer loop.
        Processes micro-batches of streaming events from Kafka or generator stream.
        """
        import socket
        self.running = True
        logger.info(f"Starting resilient streaming consumer loop (Batch Window: {self.batch_duration_sec}s)...")

        # Connection check for Kafka broker
        first_broker = self.bootstrap_servers.split(",")[0].strip()
        host = first_broker.split(":")[0] if ":" in first_broker else first_broker
        port = int(first_broker.split(":")[1]) if ":" in first_broker else 9092

        consumer = None
        consumer_kwargs = {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "auto_offset_reset": settings.KAFKA_AUTO_OFFSET_RESET,
            "enable_auto_commit": True,
            "group_id": settings.KAFKA_CONSUMER_GROUP,
            "value_deserializer": lambda x: json.loads(x.decode("utf-8")),
            "consumer_timeout_ms": 1000
        }

        if settings.KAFKA_SECURITY_PROTOCOL and settings.KAFKA_SECURITY_PROTOCOL.upper() != "PLAINTEXT":
            consumer_kwargs["security_protocol"] = settings.KAFKA_SECURITY_PROTOCOL.upper()
            consumer_kwargs["sasl_mechanism"] = settings.KAFKA_SASL_MECHANISM or "PLAIN"
            if settings.KAFKA_SASL_USERNAME and settings.KAFKA_SASL_PASSWORD:
                consumer_kwargs["sasl_plain_username"] = settings.KAFKA_SASL_USERNAME
                consumer_kwargs["sasl_plain_password"] = settings.KAFKA_SASL_PASSWORD

        if self._check_kafka_available(host, port, timeout=2.0):
            try:
                from kafka import KafkaConsumer
                consumer = KafkaConsumer(
                    self.topic,
                    **consumer_kwargs
                )
                logger.info("Subscribed to live Kafka broker topic.")
            except Exception as e:
                logger.warning(f"Kafka consumer connection failed ({e}). Running with generator fallback stream.")
                consumer = None
        else:
            logger.warning(f"Kafka broker not reachable at {host}:{port}. Running with resilient generator stream.")

        batches_processed = 0
        last_reconnect_attempt = 0.0
        from generator.order_generator import order_generator

        try:
            while self.running and (max_batches is None or batches_processed < max_batches):
                # Periodically retry Kafka connection if currently in fallback
                if consumer is None and time.time() - last_reconnect_attempt > 5.0:
                    last_reconnect_attempt = time.time()
                    if self._check_kafka_available(host, port, timeout=2.0):
                        try:
                            from kafka import KafkaConsumer
                            consumer = KafkaConsumer(self.topic, **consumer_kwargs)
                            logger.info("Successfully connected and subscribed to live Kafka broker topic.")
                        except Exception:
                            consumer = None

                batch_start = time.time()
                batch_events: List[Dict[str, Any]] = []

                if consumer:
                    records = consumer.poll(timeout_ms=int(self.batch_duration_sec * 1000), max_records=50)
                    for topic_partition, msgs in records.items():
                        for msg in msgs:
                            batch_events.append(msg.value)
                else:
                    # In standalone mode, sample current streaming events
                    sample_count = int(settings.GENERATOR_RATE_PER_SECOND * self.batch_duration_sec)
                    for _ in range(max(1, sample_count)):
                        batch_events.append(order_generator.generate_next_event())

                # Process and commit micro-batch
                if batch_events:
                    stats = postgres_stream_writer.write_micro_batch(batch_events)
                    batches_processed += 1
                    logger.info(
                        f"Stream Micro-Batch #{batches_processed} Summary: "
                        f"Valid={stats['valid']} | Quarantined={stats['quarantined']} | "
                        f"Duplicates Handled={stats['duplicates']}"
                    )

                elapsed = time.time() - batch_start
                sleep_time = max(0.0, self.batch_duration_sec - elapsed)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Streaming processor interrupted by user.")
        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
            logger.info(f"Stream processor stopped. Total micro-batches processed: {batches_processed}")

    def stop(self):
        """Stops streaming consumer loop."""
        self.running = False


def main():
    parser = argparse.ArgumentParser(description="DataPulse Live - Stream Processor")
    parser.add_argument("--mode", choices=["pyspark", "native"], default="native", help="Execution mode")
    parser.add_argument("--max-batches", type=int, default=None, help="Stop after N micro-batches")
    parser.add_argument("--batch-window", type=int, default=3, help="Batch window in seconds")
    args = parser.parse_args()

    processor = StreamProcessor(batch_duration_sec=args.batch_window)

    def sig_handler(sig, frame):
        processor.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    if args.mode == "pyspark":
        processor.run_pyspark_streaming_cluster()
    else:
        processor.run_native_stream_consumer(max_batches=args.max_batches)


if __name__ == "__main__":
    main()
