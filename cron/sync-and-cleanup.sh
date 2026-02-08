#!/bin/bash

BACKEND_URL="${BACKEND_URL:-http://backend:8000}"
RETENTION_DAYS="${CLEANUP_RETENTION_DAYS:-30}"

echo "========================================"
echo "[VTagger Cron] $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# Wait for backend to be healthy
echo "[VTagger Cron] Checking backend health..."
for i in $(seq 1 5); do
    if curl -sf "${BACKEND_URL}/status/health" > /dev/null 2>&1; then
        echo "[VTagger Cron] Backend is healthy"
        break
    fi
    if [ "$i" -eq 5 ]; then
        echo "[VTagger Cron] Backend not responding after 5 attempts. Aborting."
        exit 1
    fi
    echo "[VTagger Cron] Waiting for backend... (attempt $i/5)"
    sleep 10
done

# Trigger sync
echo "[VTagger Cron] Starting sync..."
SYNC_RESPONSE=$(curl -sf -X POST "${BACKEND_URL}/status/sync/week" \
    -H "Content-Type: application/json" \
    -d '{}' 2>&1) || true
echo "[VTagger Cron] Sync response: ${SYNC_RESPONSE}"

# Wait for sync to complete
sleep 30

# Trigger soft cleanup
echo "[VTagger Cron] Starting cleanup (retention: ${RETENTION_DAYS} days)..."
CLEANUP_RESPONSE=$(curl -sf -X POST "${BACKEND_URL}/status/cleanup" \
    -H "Content-Type: application/json" \
    -d "{\"delete_files\": true, \"clean_database\": true, \"soft\": true, \"retention_days\": ${RETENTION_DAYS}}" 2>&1) || true
echo "[VTagger Cron] Cleanup response: ${CLEANUP_RESPONSE}"

echo "[VTagger Cron] Done at $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
