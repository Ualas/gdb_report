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
    "title": "F. Name", # display as the table header's name
    "sortable": True,
  },
  {
    "field": "Type",
    "title": "F. Type",
    "sortable": True,
  }
]
styles_columns = [
{
    "field": "Stylename",
    "title": "Stylename",
    "sortable": True,
},
  {
    "field": "Type",
    "title": "Type",
    "sortable": True,
  },
  {
    "field": "Table",
    "title": "FeatureClass",
    "sortable": True,
  },
    {
      "field": "Count",
      "title": "Count",
      "sortable": True,
    }
]
rasters_columns = [
{
    "field": "Table",
    "title": "FeatureClass",
    "sortable": True,
},
  {
    "field": "SS ID",
    "title": "SS ID",
    "sortable": True,
  },
  {
    "field": "SSC ID",
    "title": "SSC ID",
    "sortable": True,
  },
    {
      "field": "Notes",
      "title": "Notes",
      "sortable": True,
    },
    {
    "field": "Title",
    "title": "Title",
    "sortable": True,
    },
    {
    "field": "Creator",
    "title": "Creator",
    "sortable": True,
    },
    {
      "field": "FirstYear",
      "title": "FirstYear",
      "sortable": True,
    },
    {
      "field": "LastYear",
      "title": "LastYear",
      "sortable": True,
    }
]
@app.route('/')
def index():
    dbname = request.args.get('db')
    if not dbname:
        dbname = "ir_rio"

    con = connectionDB(dbname)

    databases_data, layers_data, styles_data, rasters_data = getData(con)

    layers_df = pd.DataFrame(layers_data, columns=['Table','Field', 'Type'])
    layers_df.set_index(['Table'], inplace=True)

    styles_df = pd.DataFrame(styles_data, columns=['Table','Type', 'Stylename', 'Count'])
    styles_df.set_index(['Table'], inplace=True)

    rasters_df = pd.DataFrame(rasters_data, columns=['Table','SS ID', 'SSC ID', 'Notes', 'Title', 'Creator', 'FirstYear', 'LastYear'])
    rasters_df.set_index(['Table'], inplace=True)

    disconnectDB(con)

    return render_template('table.html',dbname = dbname, data=styles_data, columns=styles_columns, layers_data=layers_data ,layers_columns=layers_columns, databases=databases_data, rasters_data=rasters_data, rasters_columns = rasters_columns)

if __name__ == "__main__":
    app.run(debug=True)
