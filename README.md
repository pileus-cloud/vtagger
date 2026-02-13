# VTagger

Virtual Tag management system for [Umbrella Cost](https://umbrellacost.io). Automatically maps cloud resource tags to standardized virtual tags (vtags) and uploads them to Umbrella for cost allocation and governance.

## How It Works

1. **Define Dimensions** -- Create tag mapping rules using a simple DSL (e.g., `TAG['Environment'] == 'production'` maps to vtag value `prod`)
2. **Simulate** -- Test your mappings against live cloud data without making changes
3. **Sync & Upload** -- Fetch assets from Umbrella, apply dimension mappings, and upload vtags back via the governance API
4. **Monitor** -- Track upload status, view match rates, and see operation counts (inserted/updated/deleted)

## Architecture

```
Frontend (React + Vite)  -->  Backend (FastAPI + SQLite)  -->  Umbrella API
       :8889                         :8888                    (v2 governance-tags)
```

- **Backend**: FastAPI with SQLite for dimension storage, async sync engine
- **Frontend**: React 18 with TanStack Query, Tailwind CSS, shadcn/ui components
- **Cron**: Daily sync job (configurable schedule)

---

## Quick Start (Docker)

### 1. Clone and configure

```bash
git clone https://github.com/pileus-cloud/vtagger.git
cd vtagger
cp .env.example .env
```

Edit `.env` with your Umbrella credentials:

```env
VTAGGER_USERNAME=your-umbrella-email@company.com
VTAGGER_PASSWORD=your-umbrella-password
```

### 2. Start all services

```bash
docker compose up -d
```

This starts three containers:
- **vtagger-backend** -- API server on port 8888 (internal)
- **vtagger-frontend** -- Web UI on port 8889 (exposed)
- **vtagger-cron** -- Daily sync job (runs at 2 AM UTC by default)

### 3. Open the UI

Navigate to **http://localhost:8889**

### 4. Create an API key

On first visit, you'll be prompted to create an API key. Save it -- you'll need it for authentication.

### Docker Commands

```bash
# Start
docker compose up -d

# View logs
docker compose logs -f

# Stop
docker compose down

# Rebuild after code changes
docker compose up -d --build

# Remove everything including data volumes
docker compose down -v

# Manually trigger a sync
docker compose exec cron /app/sync-and-cleanup.sh
```

### Configuration

All settings are configured via environment variables in `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `VTAGGER_USERNAME` | *(required)* | Umbrella API username |
| `VTAGGER_PASSWORD` | *(required)* | Umbrella API password |
| `VTAGGER_UMBRELLA_API_BASE` | `https://api.umbrellacost.io/api` | Umbrella API base URL |
| `VTAGGER_FRONTEND_PORT` | `8889` | Port for the web UI |
| `VTAGGER_BATCH_SIZE` | `1000` | Assets per batch during fetch |
| `VTAGGER_RETENTION_DAYS` | `90` | Days to keep job history |
| `SYNC_CRON_SCHEDULE` | `0 2 * * *` | Cron schedule for daily sync |
| `CLEANUP_RETENTION_DAYS` | `30` | Days to keep output files |
| `VTAGGER_DEV_MODE` | `false` | Skip API key auth (dev only) |
| `VTAGGER_MASTER_KEY` | *(auto)* | Encryption key for credentials |

---

## Local Development

### Prerequisites

- Python 3.11+
- Node.js 18+
- npm

### Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Create `.env` in the project root (not in `backend/`):

```bash
cp .env.example .env
# Edit .env with your credentials
```

Start the backend:

```bash
cd backend
source venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8888 --reload
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

The frontend runs on **http://localhost:8889** and proxies `/api/*` requests to the backend on port 8888.

---

## Dimension Configuration

Dimensions define how cloud resource tags map to virtual tags. Each dimension has:
- **vtag_name**: The virtual tag name in Umbrella
- **statements**: Ordered list of match rules
- **defaultValue**: Value when no rule matches

### Example: Environment Dimension

```json
{
  "vtag_name": "environment",
  "index": 1,
  "kind": "tag",
  "defaultValue": "Unallocated",
  "source": "TAG:Environment",
  "statements": [
    {
      "matchExpression": "TAG['Environment'] == 'production' || TAG['Environment'] == 'prod'",
      "valueExpression": "'production'"
    },
    {
      "matchExpression": "TAG['Environment'] CONTAINS 'stag' || TAG['Environment'] == 'uat'",
      "valueExpression": "'staging'"
    },
    {
      "matchExpression": "TAG['Environment'] == 'dev' || TAG['Environment'] == 'development'",
      "valueExpression": "'development'"
    }
  ]
}
```

### DSL Syntax

**Match Expressions:**
```
TAG['TagKey'] == 'value'              # Exact match
TAG['TagKey'] CONTAINS 'partial'      # Substring match
DIMENSION['other_dim'] == 'value'     # Reference another dimension
expr1 || expr2                        # OR (first match wins)
```

**Value Expressions:**
```
'literal value'                        # Static string
```

### Import/Export

Dimensions can be imported and exported as JSON files via the web UI (Dimensions page) or the API:

```bash
# Create dimension from JSON file
curl -X POST http://localhost:8888/dimensions/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_KEY" \
  -d @environment_dimension.json

# Update existing dimension
curl -X PUT http://localhost:8888/dimensions/environment \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_KEY" \
  -d @environment_dimension.json

# Export dimension
curl http://localhost:8888/dimensions/environment \
  -H "X-API-Key: YOUR_KEY" > environment_dimension.json
```

### Tag Key Case Sensitivity

Tag keys in Umbrella are **case-sensitive**. If the tag in Umbrella is `Environment`, the dimension must use `TAG['Environment']` (not `TAG['environment']`). Check the discovered tags in the UI after running a simulation to see the exact tag key names.

---

## CLI Reference

VTagger includes a CLI for running syncs, managing dimensions, and managing credentials without the web UI.

### Sync

```bash
vtagger sync [OPTIONS]
```

Downloads assets from Umbrella, applies dimension mappings, and uploads virtual tags. Defaults to the current ISO week if no options are given.

| Option | Short | Description |
|--------|-------|-------------|
| `--week` | `-w` | Week number (1-53). Defaults to current week |
| `--year` | `-y` | Year. Defaults to current year |
| `--from-month` | | Start month (1-12) for multi-month sync |
| `--from-year` | | Start year (defaults to current year) |
| `--to-month` | | End month (1-12) for multi-month sync |
| `--to-year` | | End year (defaults to current year) |
| `--dry-run` | | Simulate only -- fetch and map without uploading |
| `--filter-mode` | | `not_vtagged` (default) or `all` |
| `--vtag-filter` | | Filter to specific dimension names (repeatable) |

**Examples:**

```bash
# Sync current week (most common daily usage)
vtagger sync

# Sync a specific week
vtagger sync --week 5 --year 2026

# Dry run -- see match results without uploading
vtagger sync --dry-run

# Sync a range of months (e.g., backfill Nov 2025 through Feb 2026)
vtagger sync --from-month 11 --from-year 2025 --to-month 2 --to-year 2026

# Sync only specific dimensions
vtagger sync --vtag-filter environment --vtag-filter cost_center

# Sync all assets (including already-vtagged ones)
vtagger sync --filter-mode all
```

### Dimensions

```bash
vtagger dimensions list                # List all loaded dimensions
vtagger dimensions validate file.json  # Validate a dimension JSON file
vtagger dimensions import file.json    # Import dimensions (--replace to overwrite)
vtagger dimensions export output.json  # Export dimensions to file
vtagger dimensions resolve '{"Environment": "prod", "Team": "backend"}'
```

### Credentials

```bash
vtagger credentials set       # Store Umbrella username/password
vtagger credentials verify    # Test authentication
vtagger credentials status    # Check if credentials are configured
vtagger credentials delete    # Remove stored credentials
```

### Other Commands

```bash
vtagger info                  # Show configuration and status
vtagger serve                 # Start the API server
vtagger --version             # Show version
```

---

## Daily Sync (Cron Job)

The cron container runs a daily sync that:

1. Determines the current ISO week number and year
2. Calls the backend API to start a week sync
3. Polls until sync completes (2-hour timeout)
4. Runs a soft cleanup of old output files

### Customize Schedule

In `.env`:
```env
# Run at 3 AM UTC on weekdays only
SYNC_CRON_SCHEDULE=0 3 * * 1-5
```

### Manual Sync (CLI)

The simplest way to run a manual sync:

```bash
# Sync current week
vtagger sync

# Dry run first, then sync
vtagger sync --dry-run
vtagger sync

# Backfill a month range
vtagger sync --from-month 1 --to-month 3 --year 2026
```

### Manual Sync (API)

You can also trigger syncs via the web UI (Tools page) or the REST API:

```bash
# Sync a specific week
curl -X POST http://localhost:8888/status/sync/week \
  -H "Content-Type: application/json" \
  -d '{"week_number": 2, "year": 2026}'

# Sync a full month
curl -X POST http://localhost:8888/status/sync/month \
  -H "Content-Type: application/json" \
  -d '{"account_key": "0", "month": "2026-01"}'

# Sync a date range
curl -X POST http://localhost:8888/status/sync/range \
  -H "Content-Type: application/json" \
  -d '{"account_key": "0", "start_date": "2026-01-01", "end_date": "2026-03-31"}'

# Sync only specific payer accounts
curl -X POST http://localhost:8888/status/sync/week \
  -H "Content-Type: application/json" \
  -d '{"week_number": 2, "year": 2026, "account_keys": ["12345", "67890"]}'
```

---

## Monitoring

The Monitor page shows:
- **Live agent status** with progress during sync (polling every 2s)
- **Upload history** with per-upload details:
  - Sync type (week/month/range) and date range
  - Payer account
  - Row counts (processed/total)
  - Operations breakdown (inserted/updated/deleted)
  - Processing phase and status

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/status/health` | Health check |
| GET | `/status/progress` | Current operation progress |
| GET | `/status/events` | SSE stream for live updates |
| POST | `/status/simulate` | Run simulation |
| GET | `/status/simulate/results` | Get simulation results |
| POST | `/status/sync/week` | Start week sync |
| POST | `/status/sync/month` | Start month sync |
| POST | `/status/sync/range` | Start range sync |
| GET | `/status/sync/progress` | Sync progress |
| POST | `/status/sync/cancel` | Cancel running sync |
| GET | `/status/sync/import-status` | Upload processing status |
| POST | `/status/reset` | Force reset agent state |
| GET | `/dimensions/` | List dimensions |
| POST | `/dimensions/` | Create dimension |
| GET | `/dimensions/{name}` | Get dimension details |
| PUT | `/dimensions/{name}` | Update dimension |
| DELETE | `/dimensions/{name}` | Delete dimension |
| POST | `/dimensions/validate` | Validate dimension JSON |
| GET | `/dimensions/discovered-tags` | Tags found during last run |
| GET | `/auth/validate` | Validate API key |
| POST | `/auth/keys` | Create API key |
| GET | `/auth/accounts` | List cloud accounts |
| GET | `/stats/daily` | Daily statistics |
| GET | `/stats/summary` | Summary statistics |
| GET | `/stats/weekly-trends` | Weekly trend data |

---

## License

Copyright (c) Pileus Cloud. All rights reserved.
