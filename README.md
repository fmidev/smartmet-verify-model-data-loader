# smartmet-verify-model-data-loader

Daemon that fetches forecast data from a SmartMet Server OGC EDR API and loads it into a SmartMet Verify verification database. Runs continuously, repeating at a configurable interval.

Each iteration queries the EDR instances endpoint to discover available model runs, compares them against the database, and loads any that have not yet been ingested — ensuring all instances are eventually loaded even after downtime.

## Configuration

All configuration is via environment variables. Copy `.env.template` to `.env` and fill in the values.

### Required

| Variable | Description |
|---|---|
| `SMARTMET_SERVER_URL` | SmartMet Server base URL including scheme, e.g. `https://smartmet.example.com` |
| `EDR_COLLECTION` | EDR collection name, e.g. `gfs_surface` |
| `VERIF_PRODUCER` | Verification database producer name |
| `SMARTMET_PARAMETERS` | Comma-separated parameter list (newbase names), e.g. `Temperature,WindSpeedMS` |
| `SMARTMET_STATIONGROUP` | Station group name(s), comma-separated *(mutually exclusive with `SMARTMET_STATION`)* |
| `SMARTMET_STATION` | Station FMISID(s), comma-separated *(mutually exclusive with `SMARTMET_STATIONGROUP`)* |
| `VERIFIMPORT_USER` | Database user |
| `VERIFIMPORT_PASSWORD` | Database password |
| `VERIFIMPORT_HOST` | Database host |
| `VERIFIMPORT_DBNAME` | Database name |
| `VERIFIMPORT_PORT` | Database port |

### Optional

| Variable | Default | Description |
|---|---|---|
| `RUN_INTERVAL` | `600` | Seconds between polls for new instances |
| `RETRY_COUNT` | `3` | Number of retries per instance on API failure or incomplete data |
| `RETRY_DELAY` | `60` | Seconds to wait between retries |
| `VERBOSE` | _(unset)_ | Set to `1`, `true`, or `yes` for verbose logging |
| `DRY_RUN` | _(unset)_ | Set to `1`, `true`, or `yes` to log queries/URLs without writing to the database |
| `HTTP_PROXY` / `HTTPS_PROXY` | _(unset)_ | Standard proxy env vars, forwarded to outbound HTTP requests |

## Container usage

```bash
docker build -t smartmet-verify-model-data-loader .

docker run \
  -e SMARTMET_SERVER_URL=https://smartmet.example.com \
  -e EDR_COLLECTION=gfs_surface \
  -e VERIF_PRODUCER=gfs \
  -e SMARTMET_PARAMETERS=Temperature,Pressure,Humidity,WindSpeedMS,WindDirection \
  -e SMARTMET_STATIONGROUP=synop_europe \
  -e RUN_INTERVAL=3600 \
  -e VERIFIMPORT_USER=verifuser \
  -e VERIFIMPORT_PASSWORD=secret \
  -e VERIFIMPORT_HOST=localhost \
  -e VERIFIMPORT_DBNAME=verifdb \
  -e VERIFIMPORT_PORT=5432 \
  smartmet-verify-model-data-loader
```

The container responds to `SIGTERM` and `SIGINT` for clean shutdown.

## Development

### Prerequisites

- Docker (recommended) or Python 3.14+

### Running tests and linters via Docker Compose

```bash
make test    # pytest with coverage (≥80% required)
make lint    # ruff + mypy + pylint
make fix     # auto-fix ruff formatting and safe lint fixes
make audit   # pip-audit vulnerability scan
```

Or directly via Docker Compose:

```bash
docker compose run --rm test
docker compose run --rm lint
```

### Local setup (without Docker)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

pytest
ruff check src tests
mypy src
pylint src
bandit -c bandit.yaml -r src
```

### Pre-commit hooks

```bash
pip install pre-commit
pre-commit install
```

Hooks run ruff, bandit, mypy, and pylint on every commit.

### Running locally

```bash
cp .env.template .env
# edit .env

make run          # via Docker Compose (uses .env)
# or
python -m smartmet_verify_model_data_loader
```

## CI/CD

| Workflow | Trigger | Purpose |
|---|---|---|
| `test` | push / PR | pytest + coverage |
| `lint` | push / PR | ruff, mypy, pylint, bandit |
| `audit` | push / PR / weekly | pip-audit vulnerability scan |
| `publish` | version tag `X.Y.Z` | build and push OCI image |
| `update-python-version` | weekly / manual | open PR when a new CPython patch is released |

### Publishing a release

1. Update `version` in `pyproject.toml`, commit, push.
2. `git tag X.Y.Z && git push --tags`

GitHub Actions validates the tag matches the `pyproject.toml` version, then builds and pushes the OCI image to `$IMAGE_REGISTRY/$IMAGE_REGISTRY_NAMESPACE/smartmet-verify-model-data-loader`.
