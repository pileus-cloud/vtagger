# VTagger Backend - FastAPI + SQLite API server
#
# Provides REST API for dimension management, sync operations,
# simulation, monitoring, and Umbrella API integration.
#
# Credentials are passed via environment variables (VTAGGER_USERNAME, VTAGGER_PASSWORD).
# Data is persisted in /app/data (SQLite DB) and /app/output (CSV/JSONL files).

FROM python:3.11-slim

LABEL description="VTagger Backend - FastAPI + SQLite API server. \
Provides REST API for dimension management, sync operations, simulation, \
monitoring, and Umbrella API integration."
LABEL env.VTAGGER_USERNAME="Umbrella API username (required)"
LABEL env.VTAGGER_PASSWORD="Umbrella API password (required)"
LABEL env.VTAGGER_UMBRELLA_API_BASE="Umbrella API base URL (default: https://api.umbrellacost.io/api)"
LABEL env.VTAGGER_DATABASE_PATH="SQLite database path (default: /app/data/vtagger.db)"
LABEL env.VTAGGER_OUTPUT_DIR="Output directory for CSV/JSONL files (default: /app/output)"
LABEL env.VTAGGER_BATCH_SIZE="Assets per batch during fetch (default: 1000)"
LABEL env.VTAGGER_RETENTION_DAYS="Days to keep job history (default: 90)"
LABEL env.VTAGGER_MASTER_KEY="Encryption key for credentials (auto-generated if empty)"
LABEL env.VTAGGER_DEV_MODE="Skip API key auth for development (default: false)"

WORKDIR /app
RUN apt-get update && apt-get install -y gcc libffi-dev curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /app/data /app/output
ENV PYTHONUNBUFFERED=1
ENV VTAGGER_DATABASE_PATH=/app/data/vtagger.db
ENV VTAGGER_OUTPUT_DIR=/app/output
EXPOSE 8888
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8888"]
