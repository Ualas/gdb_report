import psycopg2
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()
DBUSER = os.getenv('DBUSER')
DBHOST = os.getenv('DBHOST')
DBPASS = os.getenv('DBPASS')

def connectionDB(dbname):
    con = psycopg2.connect(database=dbname, user=DBUSER, password=DBPASS, host=DBHOST, port='5432')
    con.set_client_encoding('UTF8')
    connectionDB.dbname = dbname
    return(con)

def disconnectDB(con):
    con.close()

def getData(con):
    layers_data = []
    styles_data = []
    databases_data = []
    rasters_data = []
    dbtables = []
    styles_layers = []
    rasters_layers = []
    cur = con.cursor()

    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false and datname != 'postgres';")
    rows = cur.fetchall()
    for row in rows:
        databases_data.append(row[0])

    cur.execute("SELECT schemaname,viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
    rows = cur.fetchall()
    for row in rows:
        dbtables.append({"schema" : row[0], "table" : row[1]})
    df_dbtables = pd.DataFrame(dbtables)

    for index, row in df_dbtables.iterrows():
        table = row['table']
        schema = row['schema']
        schema_table = schema + "." + table
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s'" % (table, schema))
        rs = cur.fetchall()
        for r in rs:
            arcgis_type = []
            if r[1] == "integer":
                arcgis_type = "Object ID"
            elif r[1] == "character varying":
                arcgis_type = "Text"
            elif r[1] == "smallint":
                arcgis_type = "Short"
            elif "timestamp" in r[1]:
                arcgis_type = "Date"
            elif r[1] == "bigint":
                arcgis_type = "Long"
            elif r[1] == "numeric":
                arcgis_type = "Double"
            else:
                arcgis_type = r[1]
            layers_data.append({"Table" : table[:-4] ,"Field" : r[0], "Type" : arcgis_type})
            if (('type' in dict(rs) or 'subtype' in dict(rs)) and 'stylename' in dict(rs)):
                    styles_layers.append(schema_table)
        if ("extents" in table or "cone" in table) and ("land" not in table and "basemap" not in table):
            rasters_layers.append(schema_table)

    styles_layers = list(set([i for i in styles_layers]))

    for i in styles_layers:
        cur = con.cursor()
        if "brasilia" in connectionDB.dbname or "rice" in connectionDB.dbname:
            cur.execute("SELECT subtype,stylename,COUNT(shape) FROM %s GROUP BY subtype,stylename" % i)
        else:
            cur.execute("SELECT type,stylename,COUNT(objectid) FROM %s GROUP BY type,stylename" % i)
        rs = cur.fetchall()
        for r in rs:
            styles_data.append({"Table" : i[:-4], "Type" : r[0], "Stylename" : r[1], "Count" : r[2]})

    for i in rasters_layers:
        cur = con.cursor()
        cur.execute("SELECT ss_id,ssc_id,notes,title,creator,firstyear,lastyear FROM %s" % i)
        rs = cur.fetchall()
        for r in rs:
            rasters_data.append({"Table" : i[:-4], "SS ID" : r[0], "SSC ID" : r[1], "Notes" : r[2], "Title" : r[3], "Creator" : r[4], "FirstYear" : r[5], "LastYear" : r[6]})

    return(databases_data, layers_data, styles_data, rasters_data)
