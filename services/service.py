import json
from query.query import Query
from functions.functions import *


class Service:

    def __init__(self):
        self.query = Query()

    def fund_summary(self, start_date: str, end_date: str) -> json:
        df = self.query.get_fund_df(start_date, end_date)

        fund_return = total_return(df['return'], annualized=False)
        fund_volatility = volatility((df['return']))
        fund_alpha = alpha(df['xs_return'], df['xs_bmk_return'])
        fund_beta = beta(df['xs_return'], df['xs_bmk_return'])
        fund_sharpe_ratio = sharpe_ratio(df['return'], df['rf_return'])
        fund_information_ratio = information_ratio(df['return'], df['bmk_return'], df['rf_return'])
        fund_tracking_error = tracking_error(df['return'], df['bmk_return'])

        result = {
            "return": round(fund_return * 100, 2),
            "volatility": round(fund_volatility * 100, 2),
            "alpha": round(fund_alpha * 100, 2),
            "beta": round(fund_beta, 2),
            "sharpe_ratio": round(fund_sharpe_ratio, 2),
            "information_ratio": round(fund_information_ratio, 2),
            "tracking_error": round(fund_tracking_error, 2),
        }

        return json.dumps(result)

    def portfolio_summary(self, fund: str, start_date: str, end_date: str) -> json:
        df = self.query.get_portfolio_df(fund, start_date, end_date)

        port_return = total_return(df['return'], annualized=False)
        port_volatility = volatility((df['return']))
        port_alpha = alpha(df['xs_return'], df['xs_bmk_return'])
        port_beta = beta(df['xs_return'], df['xs_bmk_return'])
        port_sharpe_ratio = sharpe_ratio(df['return'], df['rf_return'])
        port_information_ratio = information_ratio(df['return'], df['bmk_return'], df['rf_return'])
        port_tracking_error = tracking_error(df['return'], df['bmk_return'])

        result = {
            "fund": fund,
            "return": round(port_return * 100, 2),
            "volatility": round(port_volatility * 100, 2),
            "alpha": round(port_alpha * 100, 2),
            "beta": round(port_beta, 2),
            "sharpe_ratio": round(port_sharpe_ratio, 2),
            "information_ratio": round(port_information_ratio, 2),
            "tracking_error": round(port_tracking_error, 2),
        }

        return json.dumps(result)

    def holding_summary(self, fund: str, ticker: str, start_date: str, end_date: str) -> json:
        df = self.query.get_holding_df(fund, ticker, start_date, end_date)

        holding_return = total_return(df['return'])
        holding_alpha = alpha(df['xs_return'], df['xs_bmk_return'])
        holding_beta = beta(df['xs_return'], df['xs_bmk_return'])

        result = {
            "ticker": ticker,
            "return": round(holding_return * 100, 2),
            "alpha": round(holding_alpha * 100, 2),
            "beta": round(holding_beta, 2)
        }

        return result

    def all_holdings_summary(self, fund: str, start_date: str, end_date: str) -> json:
        tickers = self.query.get_tickers(fund, start_date, end_date)

        result = []
        for ticker in tickers:
            result.append(
                self.holding_summary(fund, ticker, start_date, end_date)
            )

        return json.dumps(result)
