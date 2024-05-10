from flask import Flask
from services.service import Service


app = Flask(__name__)

@app.route("/example")
def example():
    data = Service.example()
    return data

@app.route("/portfolio_summary") # Alpha, confidence interval, beta, volatility, information ratio, active risk
def portfolio_alpha():
    response = Service.portfolio_summary()
    return response

@app.route("/holdings_returns") # Vector of holding returns
def holdings_returns():
    response = Service.holdings_returns()
    return response

@app.route("/holdings_alphas") # Vector of holding alpha contributions
def holdings_alphas():
    return ""

@app.route("/holdings_active_risk") # Vector of holding contribution to active risk
def holdings_active_risk():
    return ""

if __name__ == '__main__':
    app.run(debug=False)
