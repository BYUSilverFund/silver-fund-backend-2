from flask import Flask, request
from flask_cors import CORS
from flask_talisman import Talisman
from services.service import Service
import json
import os
import bcrypt

app = Flask(__name__)
service = Service()
CORS(app)
Talisman(app)

@app.before_request
def before_request():

    if request.method != "OPTIONS" and request.endpoint != "home":
        if request.headers.get("x-api-key") is None:
            return json.dumps({"error": "No authorization token provided"}), 401

        api_key = request.headers.get("x-api-key")
        if not bcrypt.checkpw(api_key.encode("utf-8"), os.getenv("HASHED_API_KEY").encode("utf-8")):
            return json.dumps({"error": "Invalid authorization token"}), 401
        
@app.route("/")
def home():
    return "Welcome to the 47 Fund API v1.0"


@app.route("/test")
def test():
    parameter = request.args.get("fund")
    return json.dumps({"fund": parameter})


@app.route("/portfolio_return", methods=["GET"])
def portfolio_return():
    

    fund = request.args.get("fund")
    start_date = request.args.get("start")
    end_date = request.args.get("end")

    response = service.portfolio_return(fund, start_date, end_date)
    return response


@app.route("/holding_return", methods=["GET"])
def holding_return():
    fund = request.args.get("fund")
    ticker = request.args.get("ticker").upper()
    start_date = request.args.get("start")
    end_date = request.args.get("end")

    response = service.holding_return(fund, ticker, start_date, end_date)

    return response


if __name__ == '__main__':
    app.run(debug=False)
