import requests
from flask import Flask, request
from services.service import Service
import json

app = Flask(__name__)
service = Service()

@app.route("/")
def home():
    return "Welcome to the 47 Fund API v1.0"

@app.route("/test")
def test():
    parameter = request.args.get("fund")
    return json.dumps({"fund": parameter})


@app.route("/portfolio_total_return", methods=["GET"])
def portfolio_total_return():

    fund = request.args.get("fund")
    start_date = request.args.get("start")
    end_date = request.args.get("end")

    response = service.portfolio_total_return(fund, start_date, end_date)

    return response




if __name__ == '__main__':
    app.run(debug=False)
