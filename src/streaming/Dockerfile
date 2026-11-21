# Kafka producer image: small Python base + the Kafka client.
# Build context is the repository root (see airflow/docker-compose.yaml).
FROM python:3.15-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY src/streaming/requirements.txt src/streaming/requirements.txt
RUN pip install -r src/streaming/requirements.txt

COPY src/streaming/ src/streaming/

RUN useradd --create-home --uid 10001 producer && mkdir -p /app/cache && chown producer /app/cache
USER producer

CMD ["python", "-m", "src.streaming.producer"]
