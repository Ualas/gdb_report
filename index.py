from flask import *
from connection import getData
from connection import connectionDB
from connection import disconnectDB
from flask import jsonify
import pandas as pd
from pprint import pprint

app = Flask(__name__)

layers_columns = [
{
"field": "Table",
"title": "FeatureClass",
"sortable": True,
},
  {
    "field": "Field", # which is the field's name of data key
    "title": "Field", # display as the table header's name
    "sortable": True,
  },
  {
    "field": "Type",
    "title": "Type",
    "sortable": True,
  }
]
styles_columns = [
{
"field": "Table",
"title": "FeatureClass",
"sortable": True,
},
  {
    "field": "Type", # which is the field's name of data key
    "title": "Type", # display as the table header's name
    "sortable": True,
  },
  {
    "field": "Stylename",
    "title": "Stylename",
    "sortable": True,
  },
    {
      "field": "Count",
      "title": "Count",
      "sortable": True,
    }
]

@app.route('/')
def index():
    dbname = request.args.get('db')
    if not dbname:
        dbname = "ir_rio"

    con = connectionDB(dbname)

    databases_data = getData(con,"databases")
    pprint(databases_data)

    layers_data = getData(con,"layers")
    layers_df = pd.DataFrame(layers_data, columns=['Table','Field', 'Type'])
    layers_df.set_index(['Table'], inplace=True)

    styles_data = getData(con,"styles")
    styles_df = pd.DataFrame(styles_data, columns=['Table','Type', 'Stylename', 'Count'])
    styles_df.set_index(['Table'], inplace=True)

    disconnectDB(con)

    return render_template('table.html',data=styles_data, columns=styles_columns, layers_data=layers_data ,layers_columns=layers_columns, databases=databases_data)

if __name__ == "__main__":
    app.run(debug=True)
