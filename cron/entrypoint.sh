#!/bin/bash
set -e

SYNC_SCHEDULE="${SYNC_CRON_SCHEDULE:-0 2 * * *}"

echo "[VTagger Cron] Starting with schedule: ${SYNC_SCHEDULE}"
echo "[VTagger Cron] Backend URL: ${BACKEND_URL:-http://backend:8000}"
echo "[VTagger Cron] Cleanup retention: ${CLEANUP_RETENTION_DAYS:-30} days"

# Write crontab
echo "${SYNC_SCHEDULE} /app/sync-and-cleanup.sh >> /var/log/vtagger-cron.log 2>&1" > /etc/crontabs/root

# Start crond in foreground
echo "[VTagger Cron] Cron daemon starting..."
crond -f -l 2
