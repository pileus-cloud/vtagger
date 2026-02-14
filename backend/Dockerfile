# VTagger Backend - FastAPI + SQLite API server
#
# Provides REST API for dimension management, sync operations,
# simulation, monitoring, and Umbrella API integration.
#
# Credentials are passed via environment variables (VTAGGER_USERNAME, VTAGGER_PASSWORD).
# Data is persisted in /app/data (SQLite DB) and /app/output (CSV/JSONL files).

FROM python:3.11-slim
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
