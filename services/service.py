import json
from query.query import Query
from functions.functions import *


class Service:

    def __init__(self):
        self.query = Query()

    def portfolio_returns_vector(self, fund: str, start_date: str, end_date: str) -> json:
        df = self.query.get_portfolio_df(fund, start_date, end_date)

        returns = returns_vector(df['starting_value'], df['ending_value'])

        result = {
            "return_vector": returns.tolist()
        }

        return result

    def portfolio_total_return(self, fund: str, start_date: str, end_date: str) -> json:
        df = self.query.get_portfolio_df(fund, start_date, end_date)

        starting_value = df['starting_value'].iloc[0]
        ending_value = df['ending_value'].iloc[-1]

        port_return = total_return(starting_value, ending_value)

        result = {
            "total_return": port_return,
        }

        return json.dumps(result)

    def portfolio_alpha(self, fund: str, start_date: str, end_date: str) -> json:


        result = {
            "portfolio_alpha": "port_alpha"
        }

        return json.dumps(result)
