from flask import Flask
from services.service import Service

app = Flask(__name__)
service = Service()

@app.route("/")
def home():
    return "Welcome to the 47 Fund API ver1.0"


@app.route("/portfolio_total_return")
def portfolio_total_return():
    # Temp Variables
    fund = "undergrad"
    start_date = "2024-04-01"
    end_date = "2024-05-20"

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
