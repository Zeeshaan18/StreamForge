"""
Continuous Kafka Producer for DataPulse Live.
Streams synthetic e-commerce order events to Apache Kafka with fallback buffer for local testing.
"""
import sys
import json
import time
import argparse
import signal
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from generator.order_generator import ContinuousOrderGenerator
from config.settings import settings
from config.logging_config import setup_logger

logger = setup_logger("kafka_producer")


class IngestionProducer:
    """Manages publishing streaming order events to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
        rate_per_sec: float = None,
        error_rate: float = None,
        duplicate_rate: float = None
    ):
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or settings.KAFKA_RAW_ORDERS_TOPIC
        self.rate_per_sec = rate_per_sec or settings.GENERATOR_RATE_PER_SECOND
        self.generator = ContinuousOrderGenerator(
            error_rate=error_rate,
            duplicate_rate=duplicate_rate
        )
        self.running = False
        self._producer: Optional[KafkaProducer] = None
        self._is_mock_mode = False
        self._mock_buffer = []
        self._last_reconnect_attempt = 0.0
        self._init_producer()

    def _check_kafka_available(self, host: str, port: int, timeout: float = 2.0) -> bool:
        """Tests if Kafka broker host and port are reachable."""
        import socket
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

    def _init_producer(self):
        """Initializes connection to Kafka cluster with fast fallback."""
        first_broker = self.bootstrap_servers.split(",")[0].strip()
        if ":" in first_broker:
            host, port_str = first_broker.split(":", 1)
            port = int(port_str)
        else:
            host, port = first_broker, 9092

        if not self._check_kafka_available(host, port, timeout=2.0):
            logger.warning(f"Kafka broker not listening at {host}:{port}. Running in simulation/fallback mode.")
            self._is_mock_mode = True
            return

        try:
            producer_kwargs = {
                "bootstrap_servers": self.bootstrap_servers.split(","),
                "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
                "key_serializer": lambda k: k.encode("utf-8") if k else None,
                "acks": "all",
                "retries": 3,
                "max_in_flight_requests_per_connection": 1,
                "request_timeout_ms": 5000
            }

            # Optional SASL / SSL support for cloud brokers (e.g. Upstash, Confluent, Aiven)
            if settings.KAFKA_SECURITY_PROTOCOL and settings.KAFKA_SECURITY_PROTOCOL.upper() != "PLAINTEXT":
                producer_kwargs["security_protocol"] = settings.KAFKA_SECURITY_PROTOCOL.upper()
                producer_kwargs["sasl_mechanism"] = settings.KAFKA_SASL_MECHANISM or "PLAIN"
                if settings.KAFKA_SASL_USERNAME and settings.KAFKA_SASL_PASSWORD:
                    producer_kwargs["sasl_plain_username"] = settings.KAFKA_SASL_USERNAME
                    producer_kwargs["sasl_plain_password"] = settings.KAFKA_SASL_PASSWORD

            self._producer = KafkaProducer(**producer_kwargs)
            self._is_mock_mode = False
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}, target topic: '{self.topic}'")
        except Exception as e:
            logger.warning(f"Kafka connection failed ({e}). Running in simulation mode.")
            self._is_mock_mode = True

    def publish_event(self, event: Dict[str, Any]) -> bool:
        """Sends a single event to Kafka or local simulation buffer."""
        event_id = event.get("event_id")
        order_id = event.get("order_id")

        # Periodically retry Kafka connection if currently in fallback
        if self._is_mock_mode:
            now = time.time()
            if now - self._last_reconnect_attempt > 5.0:
                self._last_reconnect_attempt = now
                self._init_producer()

        if self._is_mock_mode or not self._producer:
            self._mock_buffer.append(event)
            if len(self._mock_buffer) > 1000:
                self._mock_buffer.pop(0)
            return True

        try:
            self._producer.send(
                self.topic,
                key=order_id,
                value=event
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to publish event {event_id} to Kafka: {e}. Falling back to simulation mode.")
            self._is_mock_mode = True
            return False

    def run(self, max_events: int = None):
        """Starts continuous stream generation and publishing loop."""
        self.running = True
        logger.info(f"Starting event stream emission (Rate: {self.rate_per_sec} events/sec)...")
        
        events_sent = 0
        interval = 1.0 / max(0.1, self.rate_per_sec)

        try:
            while self.running and (max_events is None or events_sent < max_events):
                start_time = time.time()
                
                event = self.generator.generate_next_event()
                self.publish_event(event)
                events_sent += 1
                
                if events_sent % 25 == 0 or events_sent == 1:
                    logger.info(
                        f"Stream Velocity: {events_sent} events emitted | "
                        f"Order ID: {event.get('order_id')} | "
                        f"Net: ${event.get('net_amount', 0):.2f} | "
                        f"Mode: {'Kafka Live' if not self._is_mock_mode else 'Local Buffer'}"
                    )

                # Maintain target rate
                elapsed = time.time() - start_time
                sleep_time = max(0.0, interval - elapsed)
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Received stop signal (SIGINT).")
        finally:
            self.stop()
            logger.info(f"Producer stopped. Total events published: {events_sent}")

    def stop(self):
        """Flushes and cleanly closes the producer connection."""
        self.running = False
        if self._producer:
            try:
                self._producer.flush(timeout=5)
                self._producer.close(timeout=5)
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="DataPulse Live - Synthetic Kafka Event Producer")
    parser.add_argument("--rate", type=float, default=settings.GENERATOR_RATE_PER_SECOND, help="Events per second")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after N events (default: infinite)")
    parser.add_argument("--error-rate", type=float, default=settings.GENERATOR_ERROR_INJECTION_RATE, help="Anomaly injection rate")
    parser.add_argument("--duplicate-rate", type=float, default=settings.GENERATOR_DUPLICATE_RATE, help="Duplicate event rate")
    args = parser.parse_args()

    producer = IngestionProducer(
        rate_per_sec=args.rate,
        error_rate=args.error_rate,
        duplicate_rate=args.duplicate_rate
    )

    def sig_handler(sig, frame):
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    producer.run(max_events=args.max_events)


if __name__ == "__main__":
    main()
