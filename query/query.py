from database.database import Database
import pandas as pd


class Query:

    def __init__(self):
        self.db = Database()

    def get_portfolio_df(self, fund: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH
            query_table AS
                (SELECT date,
                     fund,
                     "StartingValue"::DECIMAL AS starting_value,
                     "EndingValue"::DECIMAL AS ending_value
                FROM delta_nav
                WHERE fund = '{fund}'
                    AND date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY date),
            xf_table as
                (SELECT *,
                     ending_value / starting_value - 1 AS return
                FROM query_table
                WHERE fund = 'undergrad'
                    AND starting_value <> 0
                    AND ending_value / starting_value - 1 <> 0
                ORDER BY date)
            SELECT * FROM xf_table
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_holding_df(self, fund: str, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH
                positions_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        "Description" AS name,
                        "Quantity"::DECIMAL AS shares_1,
                        "MarkPrice"::DECIMAL AS price_1,
                        "PositionValue"::DECIMAL AS value
                    FROM positions
                    WHERE fund = '{fund}'
                        AND "Symbol" = '{ticker}'
                    ORDER BY date
                ),
                positions_xf AS (
                    SELECT
                        *,
                        LAG(shares_1) OVER (PARTITION BY ticker ORDER BY date) as shares_0,
                        LAG(price_1) OVER (PARTITION BY ticker ORDER BY date) as price_0
                    FROM positions_query
                ),
                dividends_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        "GrossRate"::DECIMAL AS div_gross_rate,
                        "GrossAmount"::DECIMAL AS div_gross_amount
                    FROM dividends
                    WHERE fund = '{fund}'
                        AND "Symbol" = '{ticker}'
                    ORDER BY date
                ),
                join_table AS (
                    SELECT p.date,
                           p.fund,
                           p.ticker,
                           p.name,
                           p.shares_0,
                           p.shares_1,
                           p.price_0,
                           p.price_1,
                           p.value,
                           d.div_gross_rate,
                           d.div_gross_amount,
                           (p.price_1 * p.shares_1 + COALESCE(d.div_gross_amount, 0)) / (p.price_0 * p.shares_0) - 1 AS return
                    FROM positions_xf p
                    LEFT JOIN dividends_query d ON p.date = d.date AND p.ticker = d.ticker AND p.fund = d.fund
                    WHERE (p.price_1 * p.shares_1 + COALESCE(d.div_gross_amount, 0)) / (p.price_0 * p.shares_0) - 1 <> 0
                        AND p.date BETWEEN '{start_date}' AND '{end_date}'
                )
                SELECT * FROM join_table
        '''

        df = self.db.execute_query(query_string)

        return df
