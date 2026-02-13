#!/bin/bash
set -e

SCHEDULE="${SYNC_CRON_SCHEDULE:-0 2 * * *}"

echo "Starting VTagger cron with schedule: $SCHEDULE"
echo "Backend URL: ${BACKEND_URL:-http://backend:8888}"
echo "Cleanup retention: ${CLEANUP_RETENTION_DAYS:-30} days"

# Write cron schedule from env var
echo "$SCHEDULE /app/sync-and-cleanup.sh >> /var/log/cron.log 2>&1" > /etc/crontabs/root

# Ensure log file exists
touch /var/log/cron.log

# Start cron in foreground, tail log so docker logs work
crond -f -l 2 &
exec tail -f /var/log/cron.log
