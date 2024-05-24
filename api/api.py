import requests
from flask import Flask, request
from services.service import Service

app = Flask(__name__)
service = Service()

@app.route("/")
def home():
    return "Welcome to the 47 Fund API v1.0"

@app.route("/test")
def test():
    parameter = request.args.get("fund")


@app.route("/portfolio_total_return", methods=["POST"])
def portfolio_total_return():

    data = request.get_json()
    fund = data["fund"]
    start_date = data["start_date"]
    end_date = data["end_date"]

    # Temp Variables
    # fund = "undergrad"
    # start_date = "2024-04-01"
    # end_date = "2024-05-20"

    response = service.portfolio_total_return(fund, start_date, end_date)

    return response


@app.route("/portfolio_returns_vector")
def portfolio_returns_vector():
    # Temp Variables
    fund = "undergrad"
    start_date = "2024-04-01"
    end_date = "2024-05-20"

    response = service.portfolio_returns_vector(fund, start_date, end_date)

    return response


if __name__ == '__main__':
    app.run(debug=False)
