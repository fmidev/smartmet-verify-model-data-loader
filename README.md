# smartmet-verify-model-data-loader

Daemon that fetches forecast data from a SmartMet Server OGC EDR API and loads it into a SmartMet Verify verification database. Runs continuously, repeating at a configurable interval.

Each iteration queries the EDR instances endpoint to discover available model runs, compares them against the database, and loads any that have not yet been ingested — ensuring all instances are eventually loaded even after downtime.

## Configuration

All configuration is via environment variables.

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
| `RUN_INTERVAL` | `3600` | Seconds between runs |
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

## Local development

```bash
pip install -r requirements.txt

export SMARTMET_SERVER_URL=https://smartmet.example.com
export EDR_COLLECTION=gfs_surface
export VERIF_PRODUCER=gfs
export SMARTMET_PARAMETERS=Temperature,WindSpeedMS
export SMARTMET_STATIONGROUP=synop_finland
export VERIFIMPORT_USER=verifuser
export VERIFIMPORT_PASSWORD=secret
export VERIFIMPORT_HOST=localhost
export VERIFIMPORT_DBNAME=verifdb
export VERIFIMPORT_PORT=5432

python3 load_model_data.py
```
