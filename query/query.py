from database.database import Database
import pandas as pd


class Query:

    def __init__(self):
        self.db = Database()

    def get_risk_free_df(self, start_date: str, end_date: str) -> pd.DataFrame:
        df = self.db.fred_risk_free()
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        df.loc[:, 'value'] = df['value'] * .01

        df = df.sort_values(by='date')

        df['value'] = df['value'].fillna(df['value'].shift(1))
        df['lagged_value'] = df['value'].shift(1)

        df['P0'] = 100 / (1 + df['lagged_value'] * 30 / 360)
        df['P1'] = 100 / (1 + df['value'] * 29 / 360)

        df['risk_free_return'] = df['P1'] / df['P0'] - 1
        df['rf'] = df['value'] / 360
        df = df[['date', 'rf', 'risk_free_return']]

        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

        return df

    def get_benchmark_df(self, start_date: str, end_date: str) -> pd.DataFrame:
        previous_day = pd.Timestamp(start_date) - pd.Timedelta(5, 'D')
        new_start_date = previous_day.strftime("%Y-%m-%d")

        query_string = f'''
            SELECT date,
                   "Symbol"    AS symbol,
                   "MarkPrice"::DECIMAL AS ending_value
            FROM positions
            WHERE fund = 'undergrad'
              AND "Symbol" = 'IWV'
              AND date BETWEEN '{new_start_date}' AND '{end_date}'
            ORDER BY date
        '''

        df = self.db.execute_query(query_string)
        df['starting_value'] = df['ending_value'].shift(1)
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

        return df

    def get_portfolio_df(self, fund: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
        SELECT date, fund, 
        "StartingValue"::DECIMAL AS starting_value, 
        "EndingValue"::DECIMAL AS ending_value
        FROM delta_nav 
        WHERE fund = '{fund}' 
        AND date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date
        '''

        df = self.db.execute_query(query_string)

        return df
