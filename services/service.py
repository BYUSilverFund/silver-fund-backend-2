import json
from query.query import Query
from functions.functions import *


class Service:

    def __init__(self):
        self.query = Query()

    def portfolio_return(self, fund: str, start_date: str, end_date: str) -> json:
        df = self.query.get_portfolio_df(fund, start_date, end_date)

        returns_vector = df['return']

        port_return = total_return(returns_vector)

        result = {
            "fund": fund,
            "start_date": start_date,
            "end_date": end_date,
            "return": port_return,
        }

        return json.dumps(result)

    def holding_return(self, fund: str, ticker: str, start_date: str, end_date: str) -> json:
        df = self.query.get_holding_df(fund, ticker, start_date, end_date)

        returns_vector = df['return']

        holding_return = total_return(returns_vector)

        result = {
            "ticker": ticker,
            "return": round(holding_return*100,2),
        }

        return json.dumps(result)

    def all_holding_returns(self, fund: str, start_date: str, end_date: str) -> json:
        tickers = self.query.get_tickers(fund, start_date, end_date)

        result = []
        for ticker in tickers:
            result.append(
                self.holding_return(fund, ticker, start_date, end_date)
            )

        return json.dumps(result)
