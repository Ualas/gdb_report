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
dbtables = []
data = []
fields_data = []

def getData():
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
        try:
            schema_table = row['schema'] + "." + row['table']
            cur.execute("SELECT type,stylename FROM %s GROUP BY type,stylename" % schema_table)
            rs = cur.fetchall()
            for r in rs:
                data.append({"Table" : row['table'][:-4], "Type" : r[0], "Stylename" : r[1]})
        #except psycopg2.OperationalError: traceback.print_exc()
        except: continue
    con.close()
    return(data)

#df = pd.DataFrame(data, columns=['Table', 'Type', 'Stylename'])
#pt = pd.pivot_table(df,index=["Table","Type","Stylename"], aggfunc=len) #FIX ISSUE HERE
#data = getData()
#pprint(data)
