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
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

shell:
	docker-compose exec backend /bin/bash

backend-shell:
	docker-compose exec backend /bin/bash

frontend-shell:
	docker-compose exec frontend /bin/sh

clean:
	docker-compose down -v
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
	docker-compose logs -f cron

cron-run-now:
	docker-compose exec cron /app/sync-and-cleanup.sh
