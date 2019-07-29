from flask import *
from connection import getData
from flask import jsonify
import pandas as pd

app = Flask(__name__)

@app.route('/')
def index():
    data = getData()
    df = pd.DataFrame(data, columns=['Table','Type', 'Stylename'])
    df.set_index(['Table','Type'], inplace=True)
    return render_template('view.html',tables=[df.to_html(classes='Type')],
    titles = ['Types'])

if __name__ == "__main__":
    app.run(debug=True)
