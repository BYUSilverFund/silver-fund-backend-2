from database.database import Database
import pandas as pd


class Query:

    def __init__(self):
        self.db = Database()

    def get_portfolio_df(self, fund: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
        SELECT date, fund, 
        "StartingValue"::DECIMAL AS starting_value, 
        "EndingValue"::DECIMAL AS ending_value,
        (ending_value / starting_value - 1) AS pct_change
        FROM delta_nav 
        WHERE fund = '{fund}' 
        AND date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date
        '''

        df = self.db.execute_query(query_string)

        return df
