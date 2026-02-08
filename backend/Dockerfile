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
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
