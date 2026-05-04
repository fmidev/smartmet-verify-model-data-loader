# smartmet-data-verif

Tools for fetching forecast data from SmartMet Server and loading it into a verification database.

## Prerequisites

Both scripts require a PostgreSQL verification database. Connection is configured via environment variables:

```
export VERIFIMPORT_USER=...
export VERIFIMPORT_PASSWORD=...
export VERIFIMPORT_HOST=...
export VERIFIMPORT_DBNAME=...
export VERIFIMPORT_PORT=...
```

Python dependencies: `requests`, `psycopg2`.

## fetch_to_verif.py

Fetches forecast data from a SmartMet Server timeseries API and writes a CSV file (`out.csv`).

```
fetch_to_verif.py -q SERVER -b SMARTMET_PRODUCER -r VERIF_PRODUCER \
    -p PARAMS (-s STATIONGROUP | -S STATION) [OPTIONS]
```

| Option | Description |
|---|---|
| `-q`, `--server` | SmartMet Server hostname (required) |
| `-p`, `--parameters` | Parameter(s), comma-separated newbase names (required) |
| `-b`, `--smartmet-producer` | SmartMet Server producer name (required) |
| `-r`, `--verif-producer` | Verif database producer name (required) |
| `-s`, `--stationgroup` | Station group name(s), comma-separated |
| `-S`, `--station` | Station ID(s), comma-separated |
| `-t`, `--timestep` | Timestep for timeseries query (default: `data`) |
| `--proxy` | HTTP(S) proxy URL |
| `-v`, `--verbose` | Verbose output |
| `--dry-run` | Print queries and URLs without making changes |

Example:

```
./fetch_to_verif.py -q smartmet.example.com -b ecmwf -r ecmwf_verif \
    -p Temperature,Pressure -s synop_finland -t 60
```

## verif_loader.py

Loads a CSV file (produced by `fetch_to_verif.py`) into the verification database.

```
verif_loader.py -r PRODUCER FILE
```

| Option | Description |
|---|---|
| `-r`, `--producer` | Producer name (required) |
| `FILE` | Input CSV file |

Example:

```
./verif_loader.py -r ecmwf_verif out.csv
```

## Container Usage

For Kubernetes / container deployments, copy the scripts directly into the image:

```dockerfile
FROM python:3.11-slim

RUN pip install --no-cache-dir requests psycopg2-binary

COPY fetch_to_verif.py verif_loader.py /opt/smartmet-data-verif/

ENTRYPOINT ["python3"]
```

Pass environment variables and arguments via your pod spec or job template.
