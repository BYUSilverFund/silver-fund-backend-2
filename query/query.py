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
        ("EndingValue"::DECIMAL / "StartingValue"::DECIMAL - 1) AS return
        FROM delta_nav 
        WHERE fund = '{fund}' 
        AND "StartingValue"::DECIMAL <> 0
        AND ("EndingValue"::DECIMAL / "StartingValue"::DECIMAL - 1) <> 0
        AND date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_holding_df(self, fund: str, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH parsed_table AS (
                SELECT
                    p.date,
                    p.fund,
                    p."Symbol" AS ticker,
                    p."Description" AS name,
                    p."Quantity"::DECIMAL AS shares_1,
                    p."MarkPrice"::DECIMAL AS price_1,
                    LAG(p."MarkPrice"::DECIMAL) OVER (PARTITION BY p."Symbol" ORDER BY p.date) AS price_0,
                    LAG(p."Quantity"::DECIMAL) OVER (PARTITION BY p."Symbol" ORDER BY p.date) AS shares_0,
                    p."PositionValue"::DECIMAL AS value,
                    d."GrossRate"::DECIMAL as div_gross_rate,
                    d."GrossAmount"::DECIMAL AS div_gross_amount
                FROM positions p
                LEFT JOIN dividends d ON p.date = d.date AND p."Symbol" = d."Symbol"
                WHERE p.fund = '{fund}'
                  AND p."Symbol" = '{ticker}'
            )
            SELECT
                date,
                fund,
                ticker,
                name,
                shares_0,
                shares_1,
                price_0,
                price_1,
                value,
                div_gross_rate,
                div_gross_amount,
                (price_1 * shares_1 + COALESCE(div_gross_amount, 0)) / (price_0 * shares_0) - 1 AS return
            FROM parsed_table
            WHERE (price_1 * shares_1 + COALESCE(div_gross_amount, 0)) / (price_0 * shares_0) - 1 <> 0
            AND date BETWEEN '{start_date}' AND '{end_date}'
        '''

        df = self.db.execute_query(query_string)

        return df
