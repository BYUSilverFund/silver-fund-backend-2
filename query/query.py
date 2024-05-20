from database.database import Database


class Query:

    def __init__(self):
        self.db = Database()

    def get_portfolio_df(self, fund, start_date, end_date):
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
