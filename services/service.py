import json
from query.query import Query
from functions.functions import *


class Service:

    def __init__(self):
        self.query = Query()

    def portfolio_total_return(self, fund: str, start_date: str, end_date: str) -> json:
        df = self.query.get_portfolio_df(fund, start_date, end_date)

        starting_values = df['starting_value']
        ending_values = df['ending_value']

        port_return = total_return(starting_values, ending_values)

        result = {
            "total_return": port_return,
        }

        return json.dumps(result)

    def portfolio_alpha(self, fund: str, start_date: str, end_date: str) -> json:
        result = {
            "portfolio_alpha": "port_alpha"
        }

        return json.dumps(result)
