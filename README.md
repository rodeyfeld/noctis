# Noctis

Prefect workflow orchestration for the Universe ecosystem.

## Flows

### imagery-finder-study

Processes ImageryFinder requests by matching archive items using PostGIS spatial queries.

- **Input**: `imagery_finder_pk` and `dream_pk` parameters
- **Process**: 
  1. Spatial intersection query (ST_Intersects) between archive items and finder location
  2. Temporal filtering based on finder date range
  3. Creates `ArchiveLookupItem` records linking matches to the study
  4. Notifies Augur API to continue divination process

```bash
# Run via CLI
prefect deployment run 'imagery-finder-study/default' \
  --param imagery_finder_pk=2 \
  --param dream_pk=1
```

## Architecture

```
noctis/
├── flows/
│   ├── __init__.py
│   └── archive_finder_study.py   # Spatial archive matching
├── utils/
│   ├── __init__.py
│   └── atlas_connector.py        # asyncpg + connectorx utilities
├── main.py                       # Flow deployment entry point
├── compose.yml                   # Prefect infrastructure
└── pyproject.toml                # Dependencies (uv)
```

### Database Connectivity

- **asyncpg**: Async PostgreSQL driver for high-performance writes and spatial queries
- **connectorx**: Fast bulk reads into Polars DataFrames (when needed)

## Development

```bash
# Install dependencies
uv sync

# Start Prefect infrastructure
cp example.env .env
docker compose up -d

# Run flows locally
uv run python -m flows.archive_finder_study

# Deploy and serve flows
uv run python main.py
```

## Configuration

Copy `example.env` to `.env` and configure:

| Variable | Description | Default |
|----------|-------------|---------|
| `PREFECT_DB_USER` | Prefect metadata DB user | `prefect` |
| `PREFECT_DB_PASSWORD` | Prefect metadata DB password | `prefect` |
| `PREFECT_DB_NAME` | Prefect metadata DB name | `prefect` |
| `ATLAS_DB_HOST` | PostgreSQL host | `atlas-postgres` |
| `ATLAS_DB_PORT` | PostgreSQL port | `5432` |
| `ATLAS_DB_NAME` | Database name | `augur` |
| `ATLAS_DB_USER` | Database user | `atlas_user` |
| `ATLAS_DB_PASSWORD` | Database password | `atlas_password` |
| `NOCTIS_AUGUR_API_URL` | Augur API base URL | `http://augur:8000` |

## Docker Compose Services

| Service | Purpose |
|---------|---------|
| `postgres` | Prefect metadata storage |
| `prefect-server` | Prefect API + UI (port 4200) |
| `prefect-worker` | Executes flows from noctis-pool |
| `noctis` | Registers and serves flows |
