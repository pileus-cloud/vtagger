FROM alpine:3.19

RUN apk add --no-cache curl bash

WORKDIR /app

COPY entrypoint.sh /app/
COPY sync-and-cleanup.sh /app/

RUN chmod +x /app/entrypoint.sh /app/sync-and-cleanup.sh

ENTRYPOINT ["/app/entrypoint.sh"]
