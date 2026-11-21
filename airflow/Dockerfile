# 3.3.2 (from 3.1.0): fixes critical vulnerabilities in Airflow itself and
# in libraries it ships (PyJWT, Authlib, GitPython...). Python pinned to
# 3.12 to match CI.
FROM apache/airflow:3.4.0-python3.12

# Spark runs on the JVM. default-jre-headless is the distribution's OpenJDK
# (Spark 4 supports Java 17 and 21); /usr/lib/jvm/default-java points at it
# on any CPU architecture.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# PySpark (~450 MB) gets its own layer: it rarely changes, so bumping a pin
# in requirements.txt doesn't download it again.
COPY requirements-spark.txt /tmp/
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-spark.txt

# Naming apache-airflow in the same install makes pip refuse a pin that
# conflicts with Airflow's own dependencies, instead of silently downgrading
# them (a SQLAlchemy 1.4 pin once broke Airflow 3.3's database migration).
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
