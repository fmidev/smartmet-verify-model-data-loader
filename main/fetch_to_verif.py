#!/usr/bin/env python3

import argparse
import sys
import os
import requests
import psycopg2
import math
from datetime import datetime

def parse_command_line():
    parser = argparse.ArgumentParser()

    parser.add_argument("-v", '--verbose', help="verbose output", action='store_true')
    parser.add_argument("--dry-run", help="make no changes anywhere", action='store_true')
    parser.add_argument("-q", '--server', help="Retrieve data from http://SERVER/. Default is smartmet.fmi.fi", default="smartmet.fmi.fi")
    requiredOpts = parser.add_argument_group('Required arguments')

    requiredOpts.add_argument("--parameters",'-p', help="select parameter(s), comma separated list (newbase name)", required=True)
    requiredOpts.add_argument("--smartmet-producer", '-b', help="smartmet server producer name ", required=True)
    requiredOpts.add_argument("--verif-producer",'-r', help="verif producer name ", required=True)

    mutexOpts = parser.add_mutually_exclusive_group(required=True)

    mutexOpts.add_argument("--stationgroup",'-s', help="stationgroup name, comma separated list" )
    mutexOpts.add_argument("--station",'-S', help="station id, comma separated list")

    args = parser.parse_args()

    return args

def validate_params(args, cur):

    ret = []

    for origparam in args.parameters.split(','):

        # When validating verif-parameters, strip possible .raw extension
        # (this extension is used to fetch non-landscape interpolated data from
        # smartmet server)

        param = origparam

        if origparam[-4:] == ".raw":
            param = '.'.join(param.split('.')[:-1])

        query = "SELECT parameter_id FROM parameter_map WHERE category = 'newbase' AND alternative_name = %s"

        if args.dry_run:
            print("QUERY:", cur.mogrify(query, (param,)))

        cur.execute(query, (param,))

        result = cur.fetchone()

        if result is None:
            print("Parameter '%s' is not recognized by verif db" % (param))
        else:
            if args.verbose:
                print("%s has id %d" % (param, result[0]))

            ret.append({"verif_name": param, "verif_id": result[0], "smartmet_server_name" : origparam})

    if len(ret) == 0:
        print("No valid parameters found")
        sys.exit(1)

    return ret

def get_stations(args, cur):

    if args.stationgroup is not None:
        arglist = tuple(args.stationgroup.split(','))

        query = """SELECT 
  l.fmisid, l.name, st_x(l.geom), st_y(l.geom)
FROM 
  locations_v l, targetgroup_map m, targetgroups_v g
WHERE 
  m.target_id = l.fmisid AND m.group_id = g.id AND g.name IN %s
GROUP BY 1,2,3,4"""

        if args.dry_run:
            print("QUERY:", cur.mogrify(query, (arglist,)))

        cur.execute(query, (arglist,))

        ret = cur.fetchall()

    else:
        arglist = tuple(args.station.split(','))

        query = "SELECT fmisid, name, st_x(geom), st_y(geom) FROM locations_v WHERE fmisid IN %s"

        if args.dry_run:
            print("QUERY:", cur.mogrify(query, (arglist,)))

        cur.execute(query, (arglist,))

        ret = cur.fetchall()

    if len(ret) == 0:
        print("No stations fround from database with given station/stationgroup arguments")
        sys.exit(1)

    return ret

def read_from_smartmet_server(args, stations, params):

    proxies = {
        'http' : 'http://wwwcache.fmi.fi:8080',
        'https' : 'http://wwwcache.fmi.fi:8080'
    }

    step = "data";

    if args.smartmet_producer == "harmonie_skandinavia_pinta":
        step = "60"

    BASE = "http://%s/timeseries?format=json&timeformat=timestamp&precision=double&tz=utc&starttime=data&endtime=data&timestep=%s&who=smartmet-verif" % (args.server, step)

    ret = {}

    for station in stations:
        if args.verbose:
            print("Station %d %s (%f,%f)" % (station[0], station[1], station[2], station[3]))

        url = "%s&lonlat=%s,%s&param=origintime,time,%s&producer=%s&origintime=latest" % (BASE, station[2], station[3], args.parameters, args.smartmet_producer);

        if args.dry_run:
            print("Used url: %s" % (url))

        if args.server == "smartmet.fmi.fi":
            r = requests.get(url, proxies=proxies)
        else:
            r = requests.get(url)


        if r.status_code != requests.codes.ok:
            print("Failed to read data from smartmet server for station %s: got http error code %s" % (station[0], r.status_code))

            if r.status_code == 400:
                # allow bad request -- this happens when for example requesting a location that is not found
                # in the data.
                continue
            else:
                sys.exit(1)

        ret[station[0]] = r.json()

    return ret

def parse_to_csv(verifparams, data):

    ahour = None
    origintime = None

    filename = "out.csv"

    if not args.dry_run:
        f = open (filename, "w")

    for station,forecasts in data.items():
        for forecast in forecasts:

            if ahour is None:
                origintime = datetime.strptime(str(forecast['origintime']), "%Y%m%d%H%M")
                ahour = int(origintime.strftime("%H"))

            validtime = datetime.strptime(str(forecast['time']), "%Y%m%d%H%M")

            for param in verifparams:
                if not args.dry_run:
                    f.write("%s,%s,%s,%s,%s,%s,%s,\n" % (
                        validtime.strftime("%Y-%m-%d %H:%M:00"),
                        origintime.strftime("%Y-%m-%d %H:%M:00"),
                        math.floor((validtime - origintime).total_seconds() / 3600),
                        param['verif_id'],
                        forecast[param['smartmet_server_name']],
                        ahour,
                        station))

    if not args.dry_run:
        f.close()
        print("Wrote file '%s'" % (filename))


def get_and_load(args):
    password = None

    try:
        user = os.environ["VERIFIMPORT_USER"]
        password = os.environ["VERIFIMPORT_PASSWORD"]
        host = os.environ["VERIFIMPORT_HOST"]
        dbname = os.environ["VERIFIMPORT_DBNAME"]
        port = os.environ["VERIFIMPORT_PORT"]
    except KeyError as e:
        print("User, password, host, dbname and port should be given as env variable: 'VERIFIMPORT_USER', 'VERIFIMPORT_PASSWORD', 'VERIFIMPORT_HOST', 'VERIFIMPORT_DBNAME' and 'VERIFIMPORT_PORT'")
        sys.exit(1)

    dsn = "user=%s password=%s host=%s dbname=%s port=%s" % (user, password, host, dbname, port)

    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    cur = conn.cursor()

    verifparams = validate_params(args, cur)
    stations = get_stations(args, cur)

    data = read_from_smartmet_server(args, stations, verifparams)

    if len(data) == 0:
        print("No data found")
    else:
        parse_to_csv(verifparams, data)

if __name__ == "__main__":

    args = parse_command_line()

    get_and_load(args)
