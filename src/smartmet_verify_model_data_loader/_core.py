#!/usr/bin/env python3

import io
import logging
import math
import os
import re
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import datetime

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
class Config:  # pylint: disable=too-many-instance-attributes
    server_url: str
    edr_collection: str
    verif_producer: str
    parameters: str
    stationgroup: str | None
    station: str | None
    run_interval: int
    retry_count: int
    retry_delay: int
    verbose: bool
    dry_run: bool
    db_user: str
    db_password: str
    db_host: str
    db_name: str
    db_port: str


def load_config() -> Config:
    errors: list[str] = []

    def require(name: str) -> str:
        val = os.environ.get(name, "").strip()
        if not val:
            errors.append(f"  {name} is required")
        return val

    def optional(name: str, default: str = "") -> str:
        return os.environ.get(name, default).strip()

    server_url = require("SMARTMET_SERVER_URL").rstrip("/")
    edr_collection = require("EDR_COLLECTION")
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

    def parse_positive_int(name: str, default: int) -> int:
        raw = optional(name, str(default))
        try:
            v = int(raw)
            if v <= 0:
                raise ValueError
            return v
        except ValueError:
            errors.append(f"  {name} must be a positive integer, got: {raw!r}")
            return default

    run_interval = parse_positive_int("RUN_INTERVAL", 600)
    retry_count = parse_positive_int("RETRY_COUNT", 3)
    retry_delay = parse_positive_int("RETRY_DELAY", 60)

    if errors:
        log.error("Configuration errors:\n%s", "\n".join(errors))
        sys.exit(1)

    return Config(
        server_url=server_url,
        edr_collection=edr_collection,
        verif_producer=verif_producer,
        parameters=parameters,
        stationgroup=stationgroup,
        station=station,
        run_interval=run_interval,
        retry_count=retry_count,
        retry_delay=retry_delay,
        verbose=optional("VERBOSE").lower() in ("1", "true", "yes"),
        dry_run=optional("DRY_RUN").lower() in ("1", "true", "yes"),
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_name=db_name,
        db_port=db_port,
    )


def connect_db(cfg: Config) -> "psycopg2.connection":
    conn = psycopg2.connect(
        user=cfg.db_user,
        password=cfg.db_password,
        host=cfg.db_host,
        dbname=cfg.db_name,
        port=cfg.db_port,
    )
    conn.autocommit = True
    return conn


def validate_params(cfg: Config, cur: "psycopg2.cursor") -> list[dict[str, object]]:
    params: list[dict[str, object]] = []
    for orig in cfg.parameters.split(","):
        name = orig[:-4] if orig.endswith(".raw") else orig
        query = (
            "SELECT parameter_id FROM parameter_map "
            "WHERE category = 'newbase' AND alternative_name = %s"
        )
        if cfg.dry_run:
            log.info("QUERY: %s", cur.mogrify(query, (name,)).decode())
        cur.execute(query, (name,))
        row = cur.fetchone()
        if row is None:
            log.warning("Parameter '%s' not recognized by verif db", name)
        else:
            if cfg.verbose:
                log.info("%s -> id %d", name, row[0])
            params.append({"verif_name": name, "verif_id": row[0], "edr_name": orig})
    if not params:
        raise RuntimeError("No valid parameters found")
    return params


def get_stations(cfg: Config, cur: "psycopg2.cursor") -> list[tuple[object, ...]]:
    if cfg.stationgroup:
        arglist = tuple(cfg.stationgroup.split(","))
        query = """SELECT l.fmisid, l.name, st_x(l.geom), st_y(l.geom)
FROM locations_v l, targetgroup_map m, targetgroups_v g
WHERE m.target_id = l.fmisid AND m.group_id = g.id AND g.name IN %s
GROUP BY 1,2,3,4"""
    else:
        arglist = tuple(cfg.station.split(","))  # type: ignore[union-attr]
        query = "SELECT fmisid, name, st_x(geom), st_y(geom) FROM locations_v WHERE fmisid IN %s"

    if cfg.dry_run:
        log.info("QUERY: %s", cur.mogrify(query, (arglist,)).decode())
    cur.execute(query, (arglist,))
    stations: list[tuple[object, ...]] = cur.fetchall()

    if not stations:
        raise RuntimeError("No stations found with given arguments")
    return stations


def get_producer_id(cur: "psycopg2.cursor", name: str) -> int:
    cur.execute("SELECT id FROM producers WHERE name = %s", (name,))
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Producer '{name}' not found in database")
    return int(row[0])


def get_loaded_analysis_times(cur: "psycopg2.cursor", producer_id: int) -> set[datetime]:
    cur.execute("SELECT analysis_time FROM forecasts WHERE producer_id = %s", (producer_id,))
    # Strip timezone from timestamptz results; all times are UTC throughout
    return {row[0].replace(tzinfo=None) for row in cur.fetchall()}


def _parse_expected_steps(title: str) -> int | None:
    """Extract expected timestep count from the SmartMet instance title string."""
    m = re.search(r"Starttime:\s*(\S+)\s+Endtime:\s*(\S+)\s+Timestep:\s*(\d+)", title)
    if not m:
        return None
    try:
        start = datetime.strptime(m.group(1).rstrip("Z"), "%Y-%m-%dT%H:%M:%S")
        end = datetime.strptime(m.group(2).rstrip("Z"), "%Y-%m-%dT%H:%M:%S")
        step_min = int(m.group(3))
        if step_min <= 0:
            return None
        return int((end - start).total_seconds() / (step_min * 60)) + 1
    except ValueError:
        return None


def get_instances(cfg: Config, session: requests.Session) -> list[dict[str, object]]:
    url = f"{cfg.server_url}/edr/collections/{cfg.edr_collection}/instances"
    r = session.get(url)
    r.raise_for_status()
    instances: list[dict[str, object]] = []
    for inst in r.json().get("instances", []):
        interval = inst["extent"]["temporal"]["interval"][0]
        instances.append({
            "id": inst["id"],
            "start": interval[0],
            "end": interval[1],
            "expected_steps": _parse_expected_steps(inst.get("title", "")),
        })
    instances.sort(key=lambda x: str(x["id"]))  # oldest first → chronological backfill
    return instances


def parse_instance_id(instance_id: str) -> datetime:
    return datetime.strptime(instance_id, "%Y%m%dT%H%M%S")


def fetch_instance_data(
    cfg: Config,
    session: requests.Session,
    instance: dict[str, object],
    stations: list[tuple[object, ...]],
    params: list[dict[str, object]],
) -> dict[object, object]:
    """Fetch CoverageJSON for every station for one instance. Returns {fmisid: covjson}."""
    param_str = ",".join(str(p["edr_name"]).lower() for p in params)
    url = (
        f"{cfg.server_url}/edr/collections/{cfg.edr_collection}"
        f"/instances/{instance['id']}/position"
    )
    data: dict[object, object] = {}
    for fmisid, name, lon, lat in stations:
        if cfg.verbose:
            log.info("  Station %s %s (%f,%f)", fmisid, name, lon, lat)
        query_params = {
            "coords": f"POINT({lon} {lat})",
            "parameter-name": param_str,
            "datetime": f"{instance['start']}/{instance['end']}",
            "who": "smartmet-verify-model-data-loader",
        }
        if cfg.dry_run:
            log.info("  GET %s params=%s", url, query_params)
            continue
        r = session.get(url, params=query_params)
        if not r.ok:
            log.warning(
                "HTTP %d for station %s (instance %s)",
                r.status_code, fmisid, instance["id"],
            )
            if r.status_code == 400:
                continue
            raise RuntimeError(f"HTTP {r.status_code} fetching station {fmisid}")
        data[fmisid] = r.json()
    return data


def _check_completeness(
    instance: dict[str, object], data: dict[object, object]
) -> str | None:
    """Return a description of the problem if data is incomplete, else None.

    Completeness is checked two ways:
    - If the instance title carried an expected step count, compare against it.
    - Otherwise detect gaps by looking for uneven intervals between consecutive timestamps.
    """
    expected = instance.get("expected_steps")

    for fmisid, covjson in data.items():
        assert isinstance(covjson, dict)
        t_values: list[str] = covjson["domain"]["axes"]["t"]["values"]
        actual = len(t_values)

        if isinstance(expected, int):
            if actual < expected:
                return f"station {fmisid}: {actual}/{expected} timesteps"
        elif actual >= 2:
            times = [datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ") for t in t_values]
            deltas = [(times[i + 1] - times[i]).total_seconds() for i in range(len(times) - 1)]
            min_delta = min(deltas)
            if any(d > min_delta + 1 for d in deltas):
                return f"station {fmisid}: gaps detected in {actual} timesteps"

    return None


def fetch_with_retry(
    cfg: Config,
    session: requests.Session,
    instance: dict[str, object],
    stations: list[tuple[object, ...]],
    params: list[dict[str, object]],
) -> dict[object, object]:
    """Fetch instance data, retrying on failure or incomplete timesteps."""
    last_exc: Exception | None = None

    for attempt in range(cfg.retry_count + 1):
        if attempt > 0:
            log.info(
                "Retry %d/%d for instance %s in %ds",
                attempt, cfg.retry_count, instance["id"], cfg.retry_delay,
            )
            if _stop.wait(timeout=cfg.retry_delay):
                break  # shutdown requested during wait

        try:
            data = fetch_instance_data(cfg, session, instance, stations, params)
        except Exception as e:  # pylint: disable=broad-exception-caught
            last_exc = e
            log.warning(
                "Fetch error for instance %s (attempt %d/%d): %s",
                instance["id"], attempt + 1, cfg.retry_count + 1, e,
            )
            continue

        reason = _check_completeness(instance, data)
        if reason is None:
            return data

        last_exc = RuntimeError(f"Incomplete data: {reason}")
        log.warning(
            "Incomplete data for instance %s (attempt %d/%d): %s",
            instance["id"], attempt + 1, cfg.retry_count + 1, reason,
        )

    raise last_exc or RuntimeError(f"Failed to fetch instance {instance['id']}")


_COPY_COLUMNS = [
    "producer_id", "target_id", "analysis_time",
    "parameter_id", "forecaster_id", "leadtime", "value",
]


def build_copy_buffer(
    producer_id: int,
    params: list[dict[str, object]],
    instance: dict[str, object],
    data: dict[object, object],
) -> tuple[list[str], datetime]:
    analysis_time = parse_instance_id(str(instance["id"]))
    rows: list[str] = []

    for fmisid, covjson in data.items():
        assert isinstance(covjson, dict)
        times = [
            datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
            for t in covjson["domain"]["axes"]["t"]["values"]
        ]
        for p in params:
            key = str(p["edr_name"]).lower()
            if key not in covjson.get("ranges", {}):
                continue
            values: list[object] = covjson["ranges"][key]["values"]
            for valid_dt, value in zip(times, values, strict=False):
                if value is None:
                    continue
                leadtime = math.floor((valid_dt - analysis_time).total_seconds() / 3600)
                rows.append(
                    f"{producer_id}\t{fmisid}\t{analysis_time}\t"
                    f"{p['verif_id']}\t\\N\t{leadtime}\t{value}"
                )

    return rows, analysis_time


def load_to_db(
    cur: "psycopg2.cursor",
    producer_id: int,
    producer_name: str,
    analysis_time: datetime,
    rows: list[str],
) -> None:
    tablename = f"{producer_name}_forecasts"

    cur.execute(
        "INSERT INTO forecasts(producer_id, analysis_time, arrive_time) "
        "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
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
            "CREATE TEMP TABLE temp_load "
            "(producer_id int, target_id int, analysis_time timestamptz, "
            "parameter_id int, forecaster_id int, leadtime int, value numeric)"
        )
        buf.seek(0)
        cur.copy_from(buf, "temp_load", columns=_COPY_COLUMNS)
        cur.execute(
            f"INSERT INTO {tablename} "
            "(producer_id, analysis_time, target_id, parameter_id, "
            "forecaster_id, leadtime, value) "
            "SELECT producer_id, analysis_time, target_id, parameter_id, "
            "forecaster_id, leadtime, value FROM temp_load "
            "ON CONFLICT (producer_id, analysis_time, target_id, parameter_id, leadtime) "
            "DO UPDATE SET forecaster_id = EXCLUDED.forecaster_id, value = EXCLUDED.value"
        )

    log.info("Loaded %d rows for instance %s", len(rows), analysis_time)


def run_once(cfg: Config) -> None:
    conn = connect_db(cfg)
    try:
        cur = conn.cursor()
        params = validate_params(cfg, cur)
        stations = get_stations(cfg, cur)
        producer_id = get_producer_id(cur, cfg.verif_producer)
        loaded = get_loaded_analysis_times(cur, producer_id)

        with requests.Session() as session:
            instances = get_instances(cfg, session)
            new_instances = [i for i in instances if parse_instance_id(str(i["id"])) not in loaded]
            log.info(
                "Instances: %d total, %d already loaded, %d to process",
                len(instances), len(loaded), len(new_instances),
            )

            for instance in new_instances:
                log.info("Processing instance %s", instance["id"])
                try:
                    data = fetch_with_retry(cfg, session, instance, stations, params)
                except Exception:  # pylint: disable=broad-exception-caught
                    log.exception("Giving up on instance %s", instance["id"])
                    continue

                if not data:
                    log.warning("No data returned for instance %s", instance["id"])
                    continue

                rows, analysis_time = build_copy_buffer(producer_id, params, instance, data)

                if not rows:
                    log.warning("No valid rows for instance %s", instance["id"])
                    continue

                if cfg.dry_run:
                    log.info(
                        "Dry run: would load %d rows for instance %s",
                        len(rows), instance["id"],
                    )
                    continue

                try:
                    load_to_db(cur, producer_id, cfg.verif_producer, analysis_time, rows)
                except Exception:  # pylint: disable=broad-exception-caught
                    log.exception("Failed to load instance %s", instance["id"])
    finally:
        conn.close()


def main() -> None:
    cfg = load_config()
    log.info(
        "Starting: server=%s collection=%s producer=%s interval=%ds",
        cfg.server_url, cfg.edr_collection, cfg.verif_producer, cfg.run_interval,
    )

    while not _stop.is_set():
        try:
            run_once(cfg)
        except Exception:  # pylint: disable=broad-exception-caught
            log.exception("Run failed")
        _stop.wait(timeout=cfg.run_interval)

    log.info("Shutting down")
