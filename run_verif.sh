#!/bin/bash
#
# Example script to fetch forecast data and load it into the verification database.
#
# Usage: ./run_verif.sh
#

set -euo pipefail

# Database connection settings
export VERIFIMPORT_USER="verifuser"
export VERIFIMPORT_PASSWORD="secret"
export VERIFIMPORT_HOST="localhost"
export VERIFIMPORT_DBNAME="verifdb"
export VERIFIMPORT_PORT="5432"

# Configuration
SERVER="smartmet.example.com"
SMARTMET_PRODUCER="ecmwf_world_surface"
VERIF_PRODUCER="ecmwf"
PARAMETERS="Temperature,Pressure,Humidity,WindSpeed,WindDirection"
STATIONGROUP="synop_{area}"
TIMESTEP="60"
OUTFILE="out.csv"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Fetching forecast data from ${SERVER}..."

python3 "${SCRIPT_DIR}/fetch_to_verif.py" \
    -q "${SERVER}" \
    -b "${SMARTMET_PRODUCER}" \
    -r "${VERIF_PRODUCER}" \
    -p "${PARAMETERS}" \
    -s "${STATIONGROUP}" \
    -t "${TIMESTEP}" \
    -v

if [ ! -f "${OUTFILE}" ]; then
    echo "ERROR: Fetch did not produce ${OUTFILE}"
    exit 1
fi

echo "Loading data into verification database..."

python3 "${SCRIPT_DIR}/verif_loader.py" \
    -r "${VERIF_PRODUCER}" \
    "${OUTFILE}"

echo "Done."
