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

con = psycopg2.connect(database='ir_rio', user=DBUSER, password=DBPASS, host=DBHOST, port='5432')

con.set_client_encoding('UTF8')
cur = con.cursor()
cur.execute("SELECT schemaname,viewname FROM pg_catalog.pg_views WHERE viewname LIKE '%evw'")
rows = cur.fetchall()

for row in rows:
    dbtables.append({"schema" : row[0], "table" : row[1]})

df_dbtables = pd.DataFrame(dbtables)

for index, row in df_dbtables.iterrows():
    try:
        schema_table = row['schema'] + '.' + row['table']
        cur.execute("""SELECT type,stylename FROM %s""" % schema_table)
        rows = cur.fetchall()
        for row in rows:
            data.append({"Table" : schema_table ,"Type" : row[0], "Stylename" : row[1]})
        cur.execute("""SELECT type,stylename FROM %s""" % schema_table)
        rows = cur.fetchall()
    except:
        continue
con.close()

df = pd.DataFrame(data, columns=['Table', 'Type', 'Stylename'])
pt = pd.pivot_table(df,index=["Table","Type","Stylename"], aggfunc=len) #FIX ISSUE HERE
print(pt)
