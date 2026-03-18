# Auto-detect docker compose v2 plugin vs docker-compose v1 standalone
DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

.PHONY: help build up down logs shell backend-shell frontend-shell clean dev cron-logs cron-run-now

help:
	@echo "VTagger - Available commands:"
	@echo ""
	@echo "  make build          - Build Docker images"
	@echo "  make up             - Start all services"
	@echo "  make down           - Stop all services"
	@echo "  make logs           - View logs"
	@echo "  make shell          - Open shell in backend container"
	@echo "  make clean          - Remove containers and volumes"
	@echo ""
	@echo "  make dev-backend    - Run backend in development mode"
	@echo "  make dev-frontend   - Run frontend in development mode"
	@echo ""
	@echo "  make cron-logs      - View cron container logs"
	@echo "  make cron-run-now   - Trigger sync+cleanup manually"

build:
	$(DOCKER_COMPOSE) build

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

logs:
	$(DOCKER_COMPOSE) logs -f

shell:
	$(DOCKER_COMPOSE) exec backend /bin/bash

backend-shell:
	$(DOCKER_COMPOSE) exec backend /bin/bash

frontend-shell:
	$(DOCKER_COMPOSE) exec frontend /bin/sh

clean:
	$(DOCKER_COMPOSE) down -v
	docker system prune -f

dev-backend:
	cd backend && pip install -r requirements.txt && uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

dev-frontend:
	cd frontend && npm install && npm run dev

# CLI commands (run from backend directory)
cli-sync:
	cd backend && python -m cli.main sync

cli-credentials:
	cd backend && python -m cli.main credentials set

cli-status:
	cd backend && python -m cli.main status

cli-dimensions:
	cd backend && python -m cli.main dimensions list

# Cron commands
cron-logs:
	$(DOCKER_COMPOSE) logs -f cron

cron-run-now:
	$(DOCKER_COMPOSE) exec cron /app/sync-and-cleanup.sh
