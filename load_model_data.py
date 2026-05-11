#!/usr/bin/env python3

import io
import logging
import math
import os
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

_stop = threading.Event()
signal.signal(signal.SIGTERM, lambda s, f: _stop.set())
signal.signal(signal.SIGINT, lambda s, f: _stop.set())


@dataclass(frozen=True)
class Config:
    server_url: str
    smartmet_producer: str
    verif_producer: str
    parameters: str
    stationgroup: Optional[str]
    station: Optional[str]
    timestep: str
    run_interval: int
    verbose: bool
    dry_run: bool
    db_user: str
    db_password: str
    db_host: str
    db_name: str
    db_port: str


def load_config() -> Config:
    errors = []

    def require(name: str) -> str:
        val = os.environ.get(name, "").strip()
        if not val:
            errors.append(f"  {name} is required")
        return val

    def optional(name: str, default: str = "") -> str:
        return os.environ.get(name, default).strip()

    server_url = require("SMARTMET_SERVER_URL").rstrip("/")
    smartmet_producer = require("SMARTMET_PRODUCER")
    verif_producer = require("VERIF_PRODUCER")
    parameters = require("SMARTMET_PARAMETERS")
    db_user = require("VERIFIMPORT_USER")
    db_password = require("VERIFIMPORT_PASSWORD")
    db_host = require("VERIFIMPORT_HOST")
    db_name = require("VERIFIMPORT_DBNAME")
    db_port = require("VERIFIMPORT_PORT")

    stationgroup = optional("SMARTMET_STATIONGROUP") or None
    station = optional("SMARTMET_STATION") or None

    if not stationgroup and not station:
        errors.append("  One of SMARTMET_STATIONGROUP or SMARTMET_STATION is required")
    if stationgroup and station:
        errors.append("  SMARTMET_STATIONGROUP and SMARTMET_STATION are mutually exclusive")

    run_interval_str = optional("RUN_INTERVAL", "3600")
    try:
        run_interval = int(run_interval_str)
        if run_interval <= 0:
            raise ValueError
    except ValueError:
        errors.append(f"  RUN_INTERVAL must be a positive integer (seconds), got: {run_interval_str!r}")
        run_interval = 3600

    if errors:
        log.error("Configuration errors:\n%s", "\n".join(errors))
        sys.exit(1)

    return Config(
        server_url=server_url,
        smartmet_producer=smartmet_producer,
        verif_producer=verif_producer,
        parameters=parameters,
        stationgroup=stationgroup,
        station=station,
        timestep=optional("SMARTMET_TIMESTEP", "data"),
        run_interval=run_interval,
        verbose=optional("VERBOSE").lower() in ("1", "true", "yes"),
        dry_run=optional("DRY_RUN").lower() in ("1", "true", "yes"),
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_name=db_name,
        db_port=db_port,
    )


def connect_db(cfg: Config):
    conn = psycopg2.connect(
        user=cfg.db_user,
        password=cfg.db_password,
        host=cfg.db_host,
        dbname=cfg.db_name,
        port=cfg.db_port,
    )
    conn.autocommit = True
    return conn


def validate_params(cfg: Config, cur):
    params = []
    for orig in cfg.parameters.split(","):
        name = orig[:-4] if orig.endswith(".raw") else orig
        query = "SELECT parameter_id FROM parameter_map WHERE category = 'newbase' AND alternative_name = %s"
        if cfg.dry_run:
            log.info("QUERY: %s", cur.mogrify(query, (name,)).decode())
        cur.execute(query, (name,))
        row = cur.fetchone()
        if row is None:
            log.warning("Parameter '%s' not recognized by verif db", name)
        else:
            if cfg.verbose:
                log.info("%s -> id %d", name, row[0])
            params.append({"verif_name": name, "verif_id": row[0], "smartmet_name": orig})
    if not params:
        raise RuntimeError("No valid parameters found")
    return params


def get_stations(cfg: Config, cur):
    if cfg.stationgroup:
        arglist = tuple(cfg.stationgroup.split(","))
        query = """SELECT l.fmisid, l.name, st_x(l.geom), st_y(l.geom)
FROM locations_v l, targetgroup_map m, targetgroups_v g
WHERE m.target_id = l.fmisid AND m.group_id = g.id AND g.name IN %s
GROUP BY 1,2,3,4"""
    else:
        arglist = tuple(cfg.station.split(","))
        query = "SELECT fmisid, name, st_x(geom), st_y(geom) FROM locations_v WHERE fmisid IN %s"

    if cfg.dry_run:
        log.info("QUERY: %s", cur.mogrify(query, (arglist,)).decode())
    cur.execute(query, (arglist,))
    stations = cur.fetchall()

    if not stations:
        raise RuntimeError("No stations found with given arguments")
    return stations


def get_producer_id(cur, name: str) -> int:
    cur.execute("SELECT id FROM producers WHERE name = %s", (name,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Producer '{name}' not found in database")
    return row[0]


def fetch_forecasts(cfg: Config, stations, params):
    param_str = ",".join(p["smartmet_name"] for p in params)
    base_url = (
        f"{cfg.server_url}/timeseries"
        f"?format=json&timeformat=timestamp&precision=double"
        f"&tz=utc&starttime=data&endtime=data&timestep={cfg.timestep}"
        f"&who=smartmet-verif"
    )
    data = {}
    for fmisid, name, lon, lat in stations:
        if cfg.verbose:
            log.info("Station %d %s (%f,%f)", fmisid, name, lon, lat)
        url = (
            f"{base_url}&lonlat={lon},{lat}"
            f"&param=origintime,time,{param_str}"
            f"&producer={cfg.smartmet_producer}&origintime=latest"
        )
        if cfg.dry_run:
            log.info("URL: %s", url)
            continue
        r = requests.get(url)
        if r.status_code != requests.codes.ok:
            log.warning("HTTP %d for station %d", r.status_code, fmisid)
            if r.status_code == 400:
                continue
            raise RuntimeError(f"HTTP {r.status_code} fetching station {fmisid}")
        data[fmisid] = r.json()
    return data


_COPY_COLUMNS = ["producer_id", "target_id", "analysis_time", "parameter_id", "forecaster_id", "leadtime", "value"]


def build_copy_buffer(producer_id: int, params, data):
    rows = []
    analysis_time = None

    for fmisid, forecasts in data.items():
        origin_dt = None
        for forecast in forecasts:
            if origin_dt is None:
                origin_dt = datetime.strptime(str(forecast["origintime"]), "%Y%m%d%H%M")
                if analysis_time is None:
                    analysis_time = origin_dt
            valid_dt = datetime.strptime(str(forecast["time"]), "%Y%m%d%H%M")
            leadtime = math.floor((valid_dt - origin_dt).total_seconds() / 3600)
            for p in params:
                value = forecast.get(p["smartmet_name"])
                if value in (None, "None", ""):
                    continue
                rows.append(
                    f"{producer_id}\t{fmisid}\t{origin_dt}\t{p['verif_id']}\t\\N\t{leadtime}\t{value}"
                )

    return rows, analysis_time


def load_to_db(cur, producer_id: int, producer_name: str, analysis_time, rows):
    tablename = f"{producer_name}_forecasts"

    cur.execute(
        "INSERT INTO forecasts(producer_id, analysis_time, arrive_time) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (producer_id, analysis_time, analysis_time),
    )

    buf = io.StringIO("\n".join(rows))

    try:
        log.info("Loading %d rows into %s via COPY", len(rows), tablename)
        cur.copy_from(buf, tablename, columns=_COPY_COLUMNS)
    except psycopg2.IntegrityError as e:
        if e.pgcode != "23505":
            raise
        log.warning("COPY failed (duplicate key), switching to INSERT/UPDATE")
        cur.execute(
            "CREATE TEMP TABLE temp_load (producer_id int, target_id int, analysis_time timestamptz, "
            "parameter_id int, forecaster_id int, leadtime int, value numeric)"
        )
        buf.seek(0)
        cur.copy_from(buf, "temp_load", columns=_COPY_COLUMNS)
        cur.execute(
            f"""INSERT INTO {tablename}
                (producer_id, analysis_time, target_id, parameter_id, forecaster_id, leadtime, value)
            SELECT producer_id, analysis_time, target_id, parameter_id, forecaster_id, leadtime, value
            FROM temp_load
            ON CONFLICT (producer_id, analysis_time, target_id, parameter_id, leadtime)
            DO UPDATE SET forecaster_id = EXCLUDED.forecaster_id, value = EXCLUDED.value"""
        )

    log.info("Loaded %d rows into %s", len(rows), tablename)


def run_once(cfg: Config):
    conn = connect_db(cfg)
    try:
        cur = conn.cursor()
        params = validate_params(cfg, cur)
        stations = get_stations(cfg, cur)
        producer_id = get_producer_id(cur, cfg.verif_producer)

        log.info("Fetching forecasts from %s", cfg.server_url)
        data = fetch_forecasts(cfg, stations, params)

        if not data:
            log.warning("No data retrieved from SmartMet Server")
            return

        rows, analysis_time = build_copy_buffer(producer_id, params, data)

        if not rows:
            log.warning("No valid data rows to load")
            return

        if cfg.dry_run:
            log.info("Dry run: would load %d rows for analysis time %s", len(rows), analysis_time)
            return

        load_to_db(cur, producer_id, cfg.verif_producer, analysis_time, rows)
    finally:
        conn.close()


def main():
    cfg = load_config()
    log.info(
        "Starting: server=%s producer=%s interval=%ds",
        cfg.server_url, cfg.verif_producer, cfg.run_interval,
    )

    while not _stop.is_set():
        try:
            run_once(cfg)
        except Exception:
            log.exception("Run failed")
        _stop.wait(timeout=cfg.run_interval)

    log.info("Shutting down")


if __name__ == "__main__":
    main()
