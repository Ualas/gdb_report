import psycopg2
import os
from pprint import pprint
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
DBUSER = os.getenv('DBUSER')
DBHOST = os.getenv('DBHOST')
DBPASS = os.getenv('DBPASS')

def getData(report):
    data = []
    dbtables = []
    con = psycopg2.connect(database='ir_rio', user=DBUSER, password=DBPASS, host=DBHOST, port='5432')

    con.set_client_encoding('UTF8')
    cur = con.cursor()
    cur.execute("SELECT schemaname,viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
    rows = cur.fetchall()

    for row in rows:
        dbtables.append({"schema" : row[0], "table" : row[1]})

    df_dbtables = pd.DataFrame(dbtables)
    cur = con.cursor()

    for index, row in df_dbtables.iterrows():
        table = row['table']
        schema = row['schema']
        schema_table = schema + "." + table
        try:
            if (report == 'styles'):
                if not ("extent" in table or "cone" in table):
                    cur.execute("SELECT type,stylename,COUNT(objectid) FROM %s GROUP BY type,stylename" % schema_table)
                    rs = cur.fetchall()
                    for r in rs:
                        data.append({"Table" : row['table'][:-4], "Type" : r[0], "Stylename" : r[1], "Count" : r[2]})
            elif report == 'layers':
                cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s'" % (table, schema))
                rs = cur.fetchall()
                for r in rs:
                    data.append({"Table" : table[:-4] ,"Field" : r[0], "Type" : r[1]})
        #except: continue
        except psycopg2.OperationalError: traceback.print_exc()
    con.close()
    #pprint(data)
    return(data)

#df = pd.DataFrame(data, columns=['Table', 'Type', 'Stylename'])
#pt = pd.pivot_table(df,index=["Table","Type","Stylename"], aggfunc=len) #FIX ISSUE HERE
#data = getData()
#pprint(data)
