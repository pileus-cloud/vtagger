#!/bin/bash
# VTagger daily sync and cleanup script
# Syncs the current week's assets and uploads vtags to Umbrella.

# Load environment
source /etc/environment 2>/dev/null || true

BACKEND_URL="${BACKEND_URL:-http://backend:8888}"
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

# Calculate current ISO week date range
YEAR=$(date +%G)
WEEK=$(date +%V)
# Monday of current ISO week
MONDAY=$(date -d "${YEAR}-01-04 +$(( (${WEEK} - 1) * 7 )) days -$(date -d "${YEAR}-01-04" +%u) days +1 day" +%Y-%m-%d 2>/dev/null)
# Fallback for systems without GNU date (e.g. Alpine)
if [ -z "$MONDAY" ]; then
    MONDAY=$(date +%Y-%m-%d)
    SUNDAY=$(date +%Y-%m-%d)
else
    SUNDAY=$(date -d "${MONDAY} +6 days" +%Y-%m-%d)
fi

echo "[VTagger Cron] Syncing week ${WEEK}/${YEAR}: ${MONDAY} to ${SUNDAY}"

# Trigger week sync
SYNC_RESPONSE=$(curl -sf -X POST "${BACKEND_URL}/status/sync/week" \
    -H "Content-Type: application/json" \
    -d "{\"account_key\": \"0\", \"start_date\": \"${MONDAY}\", \"end_date\": \"${SUNDAY}\", \"filter_mode\": \"not_vtagged\"}" 2>&1) || true
echo "[VTagger Cron] Sync response: ${SYNC_RESPONSE}"

# Poll for sync completion (max 30 minutes)
MAX_WAIT=1800
ELAPSED=0
POLL_INTERVAL=30

echo "[VTagger Cron] Waiting for sync to complete..."
while [ $ELAPSED -lt $MAX_WAIT ]; do
    sleep $POLL_INTERVAL
    ELAPSED=$((ELAPSED + POLL_INTERVAL))

    STATUS=$(curl -sf "${BACKEND_URL}/status/sync/progress" 2>/dev/null)
    STATE=$(echo "$STATUS" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)

    if [ "$STATE" = "idle" ] || [ "$STATE" = "completed" ] || [ "$STATE" = "error" ] || [ "$STATE" = "cancelled" ]; then
        echo "[VTagger Cron] Sync finished with status: ${STATE} (${ELAPSED}s)"
        break
    fi

    echo "[VTagger Cron] Sync in progress... (${ELAPSED}s elapsed, status: ${STATE})"
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "[VTagger Cron] WARNING: Sync did not complete within ${MAX_WAIT}s"
fi

# Trigger soft cleanup
echo "[VTagger Cron] Starting cleanup (retention: ${RETENTION_DAYS} days)..."
CLEANUP_RESPONSE=$(curl -sf -X POST "${BACKEND_URL}/status/cleanup" \
    -H "Content-Type: application/json" \
    -d "{\"cleanup_type\": \"soft\", \"older_than_days\": ${RETENTION_DAYS}}" 2>&1) || true
echo "[VTagger Cron] Cleanup response: ${CLEANUP_RESPONSE}"

echo "[VTagger Cron] Done at $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
