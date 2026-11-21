# Dashboard image: small Python base, only the dashboard's own dependencies.
# Build context is the repository root (see airflow/docker-compose.yaml).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

# Dependencies first, so code changes don't reinstall them (layer caching).
COPY dashboard/requirements.txt dashboard/requirements.txt
RUN pip install -r dashboard/requirements.txt

# The same image also runs the metrics exporter: its two extra libraries go
# in their own layer, so changing them doesn't reinstall everything above.
COPY monitoring/requirements.txt monitoring/requirements.txt
RUN pip install -r monitoring/requirements.txt

COPY .streamlit/ .streamlit/
COPY dashboard/ dashboard/
COPY monitoring/ monitoring/

# Never run as root.
RUN useradd --create-home --uid 10001 dashboard
USER dashboard

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health', timeout=4)"

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
