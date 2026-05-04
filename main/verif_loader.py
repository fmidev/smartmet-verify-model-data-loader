#!/usr/bin/env python3

import sys
import os
import psycopg2
import argparse
import math
import io

cur = None

def ParseCommandLine():
    parser = argparse.ArgumentParser()

    requiredOpts = parser.add_argument_group('required arguments')
    requiredOpts.add_argument('-r', '--producer', help='Producer name', required=True)

    parser.add_argument('file', nargs=1, help='Input file (csv)')
    args = parser.parse_args()

    return args

def LoadToDatabase(cur, tablename, buff):
        
    query = "INSERT INTO " + tablename + " (producer_id, analysis_time, target_id, parameter_id, forecaster_id, leadtime, value) "
    query += "SELECT producer_id, analysis_time, target_id, parameter_id, forecaster_id, leadtime, value FROM temp_load "
    query += "ON CONFLICT (producer_id, analysis_time, target_id, parameter_id, leadtime) DO UPDATE SET forecaster_id = EXCLUDED.forecaster_id, value = EXCLUDED.value"

    f = io.StringIO("\n".join(buff))
    f.seek(0)

    try:
        print("Trying COPY")
        cur.copy_from(f, tablename, columns=['producer_id', 'target_id', 'analysis_time', 'parameter_id', 'forecaster_id', 'leadtime', 'value'])

    except psycopg2.IntegrityError as e:
        if e.pgcode == "23505":

            print("COPY failed, switch to UPDATE")

            cur.execute ("CREATE TEMP TABLE temp_load (producer_id int, analysis_time timestamptz, target_id int, parameter_id int, forecaster_id int, leadtime int, value numeric)");
            f.seek(0)
            cur.copy_from(f, "temp_load", columns=['producer_id', 'target_id', 'analysis_time', 'parameter_id', 'forecaster_id', 'leadtime', 'value'])
            cur.execute(query)

    return len(buff)

def GetProducerId(name):
    global cur
    query = "SELECT id FROM producers WHERE name = %s"

    cur.execute(query, (name,))
    result = cur.fetchone()

    if result == None:
        print("Producer id %s not found from database" % (elems[0]))
        sys.exit(1)

    return int(result[0])

def LoadHeader(opts, infile):
    header = open (infile)

    elems = header.readline().strip().split(',')

    producer_id = GetProducerId(opts.producer)

    query = "INSERT INTO forecasts(producer_id, analysis_time, arrive_time) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING"
    cur.execute(query, (producer_id, elems[1], elems[1]))

    return (producer_id)

def Load(opts, infile):

    print("Loading file %s" % (infile))
    body = open (infile)

    (producer_id) = LoadHeader(opts, infile)

    lines = 0
    totlines = 0
    totrows = 0

    buff = [] # data to be uploaded with COPY
    colbuff = [] # data to be uploaded with INSERT if COPY fails
        
    for line in body:
        elems = line.strip().split(',')
                
        validtime = elems[0]
        analysis_time = elems[1]
        leadtime = elems[2]
        parameter_id = elems[3]
        value = elems[4]
        ahour = elems[5]
        target_id = elems[6]
        forecaster_id = elems[7]

        if (value == "") or (value == "None"):
            continue

        # 'producer_id', 'target_id', 'analysis_time', 'parameter_id', 'forecaster_id', 'leadtime', 'value'
        buff.append("%s\t%s\t%s\t%s\t%s\t%s\t%s" % (producer_id,target_id,analysis_time, parameter_id, (r'\N' if forecaster_id == "" else forecaster_id), leadtime, value))

        lines = lines+1

    rows = LoadToDatabase(cur, "%s_forecasts" % (opts.producer),buff)
    totrows += rows
    totlines += lines
    print("total rows: %d loaded to database: %d" % (totlines, totrows))

def main():

    global cur

    opts = ParseCommandLine()

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
    conn.autocommit  = True

    cur = conn.cursor()

    for file in opts.file:
        Load(opts, file)

if __name__ == "__main__":
    main()
