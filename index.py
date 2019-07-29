from flask import *
from connection import getData
from flask import jsonify
import pandas as pd

app = Flask(__name__)

@app.route('/')
def index():
    layers_data = getData("layers")
    layers_df = pd.DataFrame(layers_data, columns=['Table','Field', 'Type'])
    layers_df.set_index(['Table'], inplace=True)

    styles_data = getData("styles")
    styles_df = pd.DataFrame(styles_data, columns=['Table','Type', 'Stylename', 'Count'])
    styles_df.set_index(['Table'], inplace=True)

    return render_template('view.html',tables=[styles_df.to_html(classes='Type')],
    titles = ['Type'])

if __name__ == "__main__":
    app.run(debug=True)
