# Noctis

Prefect workflow orchestration for the Universe ecosystem.

## Flows

### imagery-finder-study

Processes ImageryFinder requests by matching archive items using PostGIS spatial queries.

- **Input**: `imagery_finder_pk` and `dream_pk` parameters
- **Process**:
  1. Reads the finder's location and date range from Atlas
  2. Finds archive items in ephemeris that intersect the location (ST_Intersects) within the dates, at most 50,000, most recent first
  3. Writes `ArchiveLookupItem` and `ArchiveLookupResult` records to Atlas in one transaction, replacing any the study already had
  4. Marks the Dream `COMPLETE`

```bash
# Run via CLI
prefect deployment run 'imagery-finder-study/default' \
  --param imagery_finder_pk=2 \
  --param dream_pk=1
```

### sentinel-sync

Daily pull of new Sentinel-1/2/3/6 scenes from the [Copernicus STAC API](https://stac.dataspace.copernicus.eu/v1) into `imagery_finder_archiveitem` in ephemeris. `sentinel-backfill` fills in the past; this keeps it current.

- **Schedule**: `sentinel-sync/daily` at 02:00 UTC
- **Window**: scenes acquired in the `lookback_days` (default 3) days before `target_date` (default: the scheduled run date). The lookback catches products Copernicus publishes late (Sentinel-3 NTC lands a day or more after acquisition).
- **Idempotent**: scenes whose `external_id` already exists for the Copernicus provider are skipped, so re-runs and overlapping windows never duplicate rows. Each page commits on its own, so a retried task picks up where it left off.

```bash
# Backfill a specific day
prefect deployment run 'sentinel-sync/daily' \
  --param target_date=2026-10-01 \
  --param lookback_days=1

# Just one collection
prefect deployment run 'sentinel-sync/daily' \
  --param collections='["sentinel-2-l2a"]'
```

### sentinel-backfill

Fills ephemeris with past scenes, newest first, using sentinel-sync's task one window at a time. Not scheduled; start it from the Prefect UI or CLI.

- **Range**: from `before` (default today) back to `until` (default 2014-04-01, Sentinel-1A) in `window_days` (default 7) windows
- **Size cap**: stops before ephemeris passes `max_database_gb` (default 40, on a 48Gi volume), so the oldest scenes are what's left out
- **Rerunnable**: inserts skip stored scenes; a failed window is skipped and listed when the run ends, and a rerun fills it. To resume a stopped run, pass the `reached` date from its summary as `before`.

```bash
prefect deployment run 'sentinel-backfill/manual'

# Only Sentinel-1 and Sentinel-2 L2A, back to 2020
prefect deployment run 'sentinel-backfill/manual' \
  --param collections='["sentinel-1-grd", "sentinel-1-slc", "sentinel-2-l2a"]' \
  --param until=2020-01-01
```

## Architecture

```
noctis/
├── flows/
│   ├── __init__.py
│   ├── archive_finder_study.py   # Spatial archive matching
│   ├── sentinel_backfill.py      # Past Copernicus scenes, newest first
│   └── sentinel_sync.py          # Daily Copernicus STAC pull
├── utils/
│   ├── __init__.py
│   └── atlas_connector.py        # asyncpg + connectorx utilities
├── main.py                       # Flow deployment entry point
├── compose.yml                   # Prefect infrastructure
└── pyproject.toml                # Dependencies (uv)
```

### Database Connectivity

Two PostGIS databases: **Atlas** (augur's tables: finders, studies, results) and **ephemeris** (the sentinel archive, `imagery_finder_archiveitem`). The archive is kept apart so scrapes and archive scans can't slow augur down. Flows never join across them: the study flow reads matches out of ephemeris and writes them into Atlas.

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
| `EPHEMERIS_DB_HOST` | Sentinel archive host; unset keeps the archive in Atlas | (unset) |
| `EPHEMERIS_DB_PORT` | Sentinel archive port | `ATLAS_DB_PORT` |
| `EPHEMERIS_DB_NAME` | Sentinel archive database | `ephemeris` |
| `EPHEMERIS_DB_USER` | Sentinel archive user | `ATLAS_DB_USER` |
| `EPHEMERIS_DB_PASSWORD` | Sentinel archive password | `ATLAS_DB_PASSWORD` |
| `NOCTIS_AUGUR_API_URL` | Augur API base URL | `http://augur:8000` |

## Docker Compose Services

| Service | Purpose |
|---------|---------|
| `postgres` | Prefect metadata storage |
| `prefect-server` | Prefect API + UI (port 4200) |
| `prefect-worker` | Executes flows from noctis-pool |
| `noctis` | Registers and serves flows |
