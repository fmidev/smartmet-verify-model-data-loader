#!/usr/bin/env python3

import argparse
import io
import math
import os
import sys
from datetime import datetime

import psycopg2
import requests


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch forecast data from SmartMet Server and load into the verification database"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print DB queries and fetch URLs, make no changes")
    parser.add_argument("-q", "--server", required=True,
                        help="SmartMet Server hostname")
    parser.add_argument("-t", "--timestep", default="data",
                        help="Timeseries timestep (default: 'data')")
    parser.add_argument("--proxy",
                        help="HTTP(S) proxy URL; also settable via HTTP_PROXY/HTTPS_PROXY env vars")
    parser.add_argument("-p", "--parameters", required=True,
                        help="Parameter(s), comma-separated newbase names")
    parser.add_argument("-b", "--smartmet-producer", required=True,
                        help="SmartMet Server producer name")
    parser.add_argument("-r", "--verif-producer", required=True,
                        help="Verification database producer name")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-s", "--stationgroup",
                       help="Station group name(s), comma-separated")
    group.add_argument("-S", "--station",
                       help="Station FMISID(s), comma-separated")

    return parser.parse_args()


def connect_db():
    try:
        dsn = "user={} password={} host={} dbname={} port={}".format(
            os.environ["VERIFIMPORT_USER"],
            os.environ["VERIFIMPORT_PASSWORD"],
            os.environ["VERIFIMPORT_HOST"],
            os.environ["VERIFIMPORT_DBNAME"],
            os.environ["VERIFIMPORT_PORT"],
        )
    except KeyError as e:
        print(f"Missing env var {e}. Required: VERIFIMPORT_USER, VERIFIMPORT_PASSWORD, "
              "VERIFIMPORT_HOST, VERIFIMPORT_DBNAME, VERIFIMPORT_PORT")
        sys.exit(1)
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def validate_params(args, cur):
    params = []
    for orig in args.parameters.split(","):
        name = orig[:-4] if orig.endswith(".raw") else orig
        query = "SELECT parameter_id FROM parameter_map WHERE category = 'newbase' AND alternative_name = %s"
        if args.dry_run:
            print("QUERY:", cur.mogrify(query, (name,)).decode())
        cur.execute(query, (name,))
        row = cur.fetchone()
        if row is None:
            print(f"Parameter '{name}' not recognized by verif db")
        else:
            if args.verbose:
                print(f"{name} -> id {row[0]}")
            params.append({"verif_name": name, "verif_id": row[0], "smartmet_name": orig})
    if not params:
        print("No valid parameters found")
        sys.exit(1)
    return params


def get_stations(args, cur):
    if args.stationgroup:
        arglist = tuple(args.stationgroup.split(","))
        query = """SELECT l.fmisid, l.name, st_x(l.geom), st_y(l.geom)
FROM locations_v l, targetgroup_map m, targetgroups_v g
WHERE m.target_id = l.fmisid AND m.group_id = g.id AND g.name IN %s
GROUP BY 1,2,3,4"""
    else:
        arglist = tuple(args.station.split(","))
        query = "SELECT fmisid, name, st_x(geom), st_y(geom) FROM locations_v WHERE fmisid IN %s"

    if args.dry_run:
        print("QUERY:", cur.mogrify(query, (arglist,)).decode())
    cur.execute(query, (arglist,))
    stations = cur.fetchall()

    if not stations:
        print("No stations found with given arguments")
        sys.exit(1)
    return stations


def get_producer_id(cur, name):
    cur.execute("SELECT id FROM producers WHERE name = %s", (name,))
    row = cur.fetchone()
    if row is None:
        print(f"Producer '{name}' not found in database")
        sys.exit(1)
    return row[0]


def fetch_forecasts(args, stations, params):
    proxies = {"http": args.proxy, "https": args.proxy} if args.proxy else None
    param_str = ",".join(p["smartmet_name"] for p in params)
    base_url = (
        f"http://{args.server}/timeseries"
        f"?format=json&timeformat=timestamp&precision=double"
        f"&tz=utc&starttime=data&endtime=data&timestep={args.timestep}"
        f"&who=smartmet-verif"
    )
    data = {}
    for fmisid, name, lon, lat in stations:
        if args.verbose:
            print(f"Station {fmisid} {name} ({lon},{lat})")
        url = (
            f"{base_url}&lonlat={lon},{lat}"
            f"&param=origintime,time,{param_str}"
            f"&producer={args.smartmet_producer}&origintime=latest"
        )
        if args.dry_run:
            print("URL:", url)
            continue
        r = requests.get(url, proxies=proxies)
        if r.status_code != requests.codes.ok:
            print(f"HTTP {r.status_code} for station {fmisid}")
            if r.status_code == 400:
                continue
            sys.exit(1)
        data[fmisid] = r.json()
    return data


_COPY_COLUMNS = ["producer_id", "target_id", "analysis_time", "parameter_id", "forecaster_id", "leadtime", "value"]


def build_copy_buffer(producer_id, params, data):
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


def load_to_db(cur, producer_id, producer_name, analysis_time, rows):
    tablename = f"{producer_name}_forecasts"

    cur.execute(
        "INSERT INTO forecasts(producer_id, analysis_time, arrive_time) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
        (producer_id, analysis_time, analysis_time),
    )

    buf = io.StringIO("\n".join(rows))

    try:
        print(f"Loading {len(rows)} rows into {tablename} via COPY")
        cur.copy_from(buf, tablename, columns=_COPY_COLUMNS)
    except psycopg2.IntegrityError as e:
        if e.pgcode != "23505":
            raise
        print("COPY failed (duplicate key), switching to INSERT/UPDATE")
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

    print(f"Done: {len(rows)} rows loaded into {tablename}")


def main():
    args = parse_args()
    conn = connect_db()
    cur = conn.cursor()

    params = validate_params(args, cur)
    stations = get_stations(args, cur)
    producer_id = get_producer_id(cur, args.verif_producer)

    print(f"Fetching forecasts from {args.server}...")
    data = fetch_forecasts(args, stations, params)

    if not data:
        print("No data retrieved from SmartMet Server")
        sys.exit(0)

    rows, analysis_time = build_copy_buffer(producer_id, params, data)

    if not rows:
        print("No valid data rows to load")
        sys.exit(0)

    if args.dry_run:
        print(f"Dry run: would load {len(rows)} rows for analysis time {analysis_time}")
        sys.exit(0)

    load_to_db(cur, producer_id, args.verif_producer, analysis_time, rows)


if __name__ == "__main__":
    main()
