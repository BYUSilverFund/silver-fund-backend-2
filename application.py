from flask import Flask, request
from flask_cors import CORS
from flask_talisman import Talisman
from waitress import serve
from server.service import Service
import json
import os
import bcrypt
import boto3

application = Flask(__name__)
service = Service()

# CORS(application)
# Talisman(application)

def check_user(username):
    cognito = boto3.client("cognito-idp", region_name="us-west-2", aws_access_key_id=os.getenv("COGNITO_ACCESS_KEY_ID"),
                           aws_secret_access_key=os.getenv("COGNITO_SECRET_ACCESS_KEY"))
    try:
        response = cognito.admin_get_user(
            UserPoolId=os.getenv("COGNITO_USER_POOL_ID"),
            Username=username
        )
        return True
    except Exception as e:
        return False

@application.before_request
def before_request():
    if request.method != "OPTIONS" and request.endpoint != "home" and request.endpoint != "health_check":
        if request.headers.get("x-api-key") is None:
            return json.dumps({"error": "No authorization token provided"}), 401

        valid_user = check_user(request.headers.get("username"))
        if not valid_user:
            return json.dumps({"error": "Invalid username provided"}), 401

        api_key = request.headers.get("x-api-key")
        if not bcrypt.checkpw(api_key.encode("utf-8"), os.getenv("HASHED_API_KEY").encode("utf-8")):
            return json.dumps({"error": "Invalid authorization token"}), 401


@application.route("/", methods=["GET"])
def home():
    return "Welcome to the 47 Fund Elastic Beanstalk API v1.0"

@application.route("/health_check", methods=["GET"])
def health_check():
    response = json.dumps({"message": "Instances are healthy"}), 200
    return response

@application.route("/test")
def test():
    parameter = request.args.get("fund")
    return json.dumps({"fund": parameter})

if __name__ == "__main__":
    application.debug = True
    application.run()


# @application.route("/fund_summary", methods=["GET"])
# def fund_summary():
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.fund_summary(start_date, end_date)
#     return response


# @application.route("/fund_chart", methods=["GET"])
# def fund_chart():
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.fund_chart_data(start_date, end_date)
#     return response


# @application.route("/portfolio_summary", methods=["GET"])
# def portfolio_summary():
#     fund = request.args.get("fund")
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.portfolio_summary(fund, start_date, end_date)
#     return response


# @application.route("/portfolio_chart", methods=["GET"])
# def portfolio_chart():
#     fund = request.args.get("fund")
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.portfolio_chart_data(fund, start_date, end_date)
#     return response


# @application.route("/holding_chart", methods=["GET"])
# def holding_chart():
#     fund = request.args.get("fund")
#     ticker = request.args.get("ticker")
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.holding_chart_data(fund, ticker, start_date, end_date)
#     return response


# @application.route("/benchmark_chart", methods=["GET"])
# def benchmark_chart():
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.benchmark_chart_data(start_date, end_date)
#     return response


# @application.route("/all_portfolios_summary", methods=["GET"])
# def all_portfolios_summary():
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.all_portfolios_summary(start_date, end_date)

#     return response


# @application.route("/all_holdings_summary", methods=["GET"])
# def all_holdings_summary():
#     fund = request.args.get("fund")
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.all_holdings_summary(fund, start_date, end_date)

#     return response


# @application.route("/holding_summary", methods=["GET"])
# def holding_summary():
#     fund = request.args.get("fund")
#     ticker = request.args.get("ticker")
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.holding_summary(fund, ticker, start_date, end_date)

#     return response


# @application.route("/holding_dividends", methods=["GET"])
# def holding_dividends():
#     fund = request.args.get("fund")
#     ticker = request.args.get("ticker")
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.holding_dividends(fund, ticker, start_date, end_date)

#     return response


# @application.route("/holding_trades", methods=["GET"])
# def holding_trades():
#     fund = request.args.get("fund")
#     ticker = request.args.get("ticker")
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.holding_trades(fund, ticker, start_date, end_date)

#     return response


# @application.route("/benchmark_summary", methods=["GET"])
# def benchmark_summary():
#     start_date = request.args.get("start")
#     end_date = request.args.get("end")

#     response = service.benchmark_summary(start_date, end_date)

#     return response

# @application.route("/cron_logs", methods=["GET"])
# def cron_log():
#     response = service.cron_logs()
#     return response


# if __name__ == '__main__':
#     # Check if the environment is set to production
#     # testing or development
#     environment = os.getenv('ENVIRONMENT')
    
#     if environment == 'PRODUCTION':
#         serve(application, host="0.0.0.0", port=5000, url_scheme="https")

#     elif environment == 'DEVELOPMENT':
#         application.run(debug=True, port=8080)

#     else:
#         print("You have not set your ENVIRONMENT in .env")
