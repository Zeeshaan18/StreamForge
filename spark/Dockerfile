FROM python:3.11-slim

USER root
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    JAVA_HOME=/usr/lib/jvm/default-java \
    PATH="/usr/lib/jvm/default-java/bin:$PATH"

# Install Java Runtime (headless JRE) required for PySpark Structured Streaming & Spark JVM
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY spark/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ ./config/
COPY database/ ./database/
COPY spark/ ./spark/
COPY generator/ ./generator/

# Create non-root user (1001), checkpoint directory, and ensure proper directory permissions
RUN useradd -u 1001 -m -s /bin/bash sparkuser && \
    mkdir -p /tmp/spark-checkpoints/datapulse /app && \
    chown -R 1001:1001 /app /tmp/spark-checkpoints && \
    chmod -R 775 /tmp/spark-checkpoints

USER 1001

CMD ["python", "-m", "spark.stream_processor", "--mode", "native", "--batch-window", "3"]

