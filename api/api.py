from flask import Flask, request
from flask_cors import CORS
from flask_talisman import Talisman
from services.service import Service
import json
import os
import bcrypt
import boto3

app = Flask(__name__)
service = Service()
CORS(app)
Talisman(app)

def check_user(username):
    cognito = boto3.client("cognito-idp", region_name="us-west-2", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))
    try:
        response = cognito.admin_get_user(
            UserPoolId=os.getenv("AWS_USER_POOL_ID"),
            Username=username
        )
        return True
    except Exception as e:
        return False

@app.before_request
def before_request():

    if request.method != "OPTIONS" and request.endpoint != "home":
        if request.headers.get("x-api-key") is None:
            return json.dumps({"error": "No authorization token provided"}), 401
        
        valid_user = check_user(request.headers.get("username"))
        if not valid_user:
            return json.dumps({"error": "Invalid username provided"}), 401

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

@app.route("/all_holdings", methods=["GET"])
def all_holding_returns():
    fund = request.args.get("fund")
    start_date = request.args.get("start")
    end_date = request.args.get("end")

    response = service.all_holding_returns(fund, start_date, end_date)

    return response



if __name__ == '__main__':
    app.run(debug=False)
