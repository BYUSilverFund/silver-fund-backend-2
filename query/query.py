import numpy as np

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
                        CASE WHEN shares_1 > 0 THEN 1 ELSE -1 END AS side,
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
                trades_query AS(
                    SELECT
                        date,
                        fund,
                        "Symbol" as ticker,
                        SUM("Quantity"::DECIMAL) as shares_traded
                    FROM trades
                    WHERE fund = '{fund}'
                        AND "Symbol" = '{ticker}'
                    GROUP BY date, fund, "Symbol"
                ),
                join_table_1 AS(
                    SELECT p.date,
                           p.fund,
                           p.ticker,
                           p.name,
                           p.shares_0,
                           (p.shares_1 - COALESCE(shares_traded,0)) AS shares_1, --Adjusts shares_1 for trades made on that day
                           t.shares_traded,
                           p.price_0,
                           p.price_1,
                           p.side,
                           p.value,
                           d.div_gross_rate,
                           d.div_gross_amount,
                           p.side * ((p.price_1 * (p.shares_1 - COALESCE(shares_traded,0)) + COALESCE(d.div_gross_amount, 0)) / (p.price_0 * p.shares_0) - 1) AS return
                    FROM positions_xf p
                    LEFT JOIN trades_query t ON p.date = t.date AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.date = d.date AND p.ticker = d.ticker AND p.fund = d.fund
                ),
                bmk_query AS(
                    SELECT date,
                        LAG(ending_value) OVER (ORDER BY date) AS starting_value,
                        ending_value
                    FROM benchmark
                ),
                bmk_xf AS(
                    SELECT
                        date,
                        ending_value / starting_value - 1 as return
                    FROM bmk_query
                ),
                join_table_2 AS(
                    SELECT
                       a.date,
                       a.fund,
                       a.ticker,
                       a.name,
                       a.shares_1 AS shares,
                       a.price_1 AS price,
                       a.value,
                       a.side,
                       a.return AS return,
                       b.return AS bmk_return,
                       c.return AS rf_return,
                       a.return - c.return AS xs_return,
                       b.return - c.return AS xs_bmk_return
                    FROM join_table_1 a
                    INNER JOIN bmk_xf b ON a.date = b.date
                    INNER JOIN risk_free_rate c ON a.date = c.date
                    WHERE a.return <> 0
                        AND a.shares_1 <> 0
                        AND a.date BETWEEN '2023-01-01' AND '2025-01-01'
                )
            SELECT * FROM join_table_2
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_tickers(self, fund, start_date, end_date) -> np.ndarray:
        query_string = f'''
            SELECT DISTINCT("Symbol")
            FROM positions
            WHERE fund = '{fund}'
                AND date BETWEEN '{start_date}' AND '{end_date}'
        
        '''

        df = self.db.execute_query(query_string)

        return df['Symbol'].tolist()

