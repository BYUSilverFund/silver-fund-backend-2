from flask import Flask
from service.service import Service


app = Flask(__name__)

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/data")
def data():
    data = Service.handleData()
    return data

if __name__ == '__main__':
    app.run(debug=False)
