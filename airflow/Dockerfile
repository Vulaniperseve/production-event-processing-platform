FROM apache/airflow:3.4.0

# Spark runs on the JVM. default-jre-headless is OpenJDK 17 on Debian
# bookworm, and /usr/lib/jvm/default-java points at it on any CPU arch.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt requirements-spark.txt /tmp/

RUN pip install --no-cache-dir -r /tmp/requirements.txt -r /tmp/requirements-spark.txt
