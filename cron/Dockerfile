# VTagger Cron - Daily sync scheduler
#
# Runs a cron job that:
#   1. Starts a week sync (current ISO week) via the backend API
#   2. Polls /status/sync/progress until complete (2-hour timeout)
#   3. Runs soft cleanup of output files older than CLEANUP_RETENTION_DAYS
#
# Environment variables:
#   SYNC_CRON_SCHEDULE     - Cron expression (default: "0 2 * * *" = daily 2 AM UTC)
#   BACKEND_URL            - Backend API URL (default: http://backend:8888)
#   CLEANUP_RETENTION_DAYS - Days to keep output files (default: 30)

FROM alpine:3.19

LABEL description="VTagger Cron - Daily sync scheduler. \
Runs a cron job that: (1) Starts a week sync for the current ISO week via the backend API, \
(2) Polls /status/sync/progress until complete (2-hour timeout), \
(3) Runs soft cleanup of output files older than CLEANUP_RETENTION_DAYS."
LABEL env.SYNC_CRON_SCHEDULE="Cron expression (default: 0 2 * * * = daily 2 AM UTC)"
LABEL env.BACKEND_URL="Backend API URL (default: http://backend:8888)"
LABEL env.CLEANUP_RETENTION_DAYS="Days to keep output files (default: 30)"

RUN apk add --no-cache curl bash

WORKDIR /app

COPY entrypoint.sh /app/
COPY sync-and-cleanup.sh /app/

RUN chmod +x /app/entrypoint.sh /app/sync-and-cleanup.sh

ENTRYPOINT ["/app/entrypoint.sh"]
