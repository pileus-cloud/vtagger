#!/bin/bash
set -e

# Determine current ISO week and year
WEEK=$(date +%V)
YEAR=$(date +%G)
BACKEND_URL="${BACKEND_URL:-http://backend:8888}"
RETENTION_DAYS="${CLEANUP_RETENTION_DAYS:-30}"

echo "[$(date)] Starting sync for week $WEEK/$YEAR"

# Step 1: Start week sync
RESPONSE=$(curl -s -X POST "$BACKEND_URL/status/sync/week" \
  -H "Content-Type: application/json" \
  -d "{\"week_number\": $WEEK, \"year\": $YEAR, \"filter_mode\": \"not_vtagged\"}")

echo "[$(date)] Sync response: $RESPONSE"

# Check for error
if echo "$RESPONSE" | grep -q '"detail"'; then
  echo "[$(date)] ERROR: Failed to start sync: $RESPONSE"
  exit 1
fi

echo "[$(date)] Sync started"

# Step 2: Poll until sync completes (timeout 2 hours)
TIMEOUT=7200
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  sleep 30
  ELAPSED=$((ELAPSED + 30))
  STATUS=$(curl -s "$BACKEND_URL/status/sync/progress" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)

  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "error" ] || [ "$STATUS" = "cancelled" ]; then
    echo "[$(date)] Sync finished: $STATUS ($ELAPSED seconds)"
    break
  fi

  # Also treat idle as done (sync may have completed between polls)
  if [ "$STATUS" = "idle" ] && [ $ELAPSED -gt 60 ]; then
    echo "[$(date)] Sync finished (idle) ($ELAPSED seconds)"
    break
  fi

  # Log progress every 5 minutes
  if [ $((ELAPSED % 300)) -eq 0 ]; then
    echo "[$(date)] Sync still running ($ELAPSED seconds elapsed)..."
  fi
done

if [ $ELAPSED -ge $TIMEOUT ]; then
  echo "[$(date)] WARNING: Sync timed out after $TIMEOUT seconds"
fi

# Step 3: Soft cleanup (delete old files, keep DB history)
echo "[$(date)] Running soft cleanup (retention: $RETENTION_DAYS days)"
curl -s -X POST "$BACKEND_URL/status/cleanup" \
  -H "Content-Type: application/json" \
  -d "{\"cleanup_type\": \"soft\", \"older_than_days\": $RETENTION_DAYS}" || true

echo ""
echo "[$(date)] Done"
