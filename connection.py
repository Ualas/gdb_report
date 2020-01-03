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
    return(con)

def disconnectDB(con):
    con.close()

def getData(con):
    layers_data = []
    styles_data = []
    databases_data = []
    dbtables = []
    dbs = []
    styles_layers = []
    cur = con.cursor()
    rasterfeatureclasses = [('basemapextentspoly'),('cone')]
    typefields = ['type','subtype']

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
        try:
            cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s'" % (table, schema))
            rs = cur.fetchall()
            for r in rs:
                layers_data.append({"Table" : table[:-4] ,"Field" : r[0], "Type" : r[1]})
                if (('type' in dict(rs) or 'subtype' in dict(rs)) and 'stylename' in dict(rs)):
                        styles_layers.append(schema_table)

        except psycopg2.OperationalError: traceback.print_exc()
        #except: continue

    styles_layers = list(set([i for i in styles_layers]))
    for i in styles_layers:
        cur = con.cursor()
        cur.execute("SELECT type,stylename,COUNT(objectid) FROM %s GROUP BY type,stylename" % i)
        rs = cur.fetchall()
        for r in rs:
            styles_data.append({"Table" : row['table'][:-4], "Type" : r[0], "Stylename" : r[1], "Count" : r[2]})


    return(databases_data, layers_data, styles_data)
