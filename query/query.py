import numpy as np

from database.database import Database
import pandas as pd


class Query:

    def __init__(self):
        self.db = Database()

    def get_fund_df(self, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH
                fund_query AS(
                    SELECT
                        date,
                        SUM("StartingValue"::DECIMAL) as starting_value,
                        SUM("EndingValue"::DECIMAL) as ending_value
                    FROM delta_nav
                    WHERE "StartingValue"::DECIMAL <> 0
                    GROUP BY date
                    ORDER BY date
                ),
                dividends_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossRate"::DECIMAL) AS div_gross_rate,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount
                    FROM dividends
                    WHERE fund = 'undergrad'
                        AND "Symbol" = 'IWV'
                    GROUP BY date, fund, "Symbol"
                    ORDER BY date
                ),
                bmk_query AS(
                    SELECT
                        b.date,
                        LAG(b.ending_value) OVER (ORDER BY b.date) AS starting_value,
                        b.ending_value,
                        d.div_gross_rate,
                        d.div_gross_amount
                    FROM benchmark b
                    LEFT JOIN dividends_query d ON b.date = d.date AND d.ticker = 'IWV' AND d.fund = 'undergrad'
                ),
                bmk_xf AS(
                    SELECT
                        date,
                        (ending_value ) / starting_value - 1 AS return -- + COALESCE(div_gross_rate, 0)
                    FROM bmk_query
                    WHERE starting_value <> 0
                ),
                fund_xf AS(
                    SELECT
                        date,
                        starting_value,
                        ending_value,
                        ending_value / starting_value - 1 AS return
                    FROM fund_query
                ),
                join_table AS(
                    SELECT
                        f.date,
                        f.starting_value,
                        f.ending_value,
                        f.return,
                        b.return AS bmk_return,
                        r.return AS rf_return,
                        f.return - r.return AS xs_return,
                        b.return - r.return AS xs_bmk_return
                    FROM fund_xf f
                    INNER JOIN bmk_xf b ON b.date = f.date
                    INNER JOIN risk_free_rate r ON f.date = r.date
                    WHERE f.date BETWEEN '{start_date}' AND '{end_date}'

                )
            SELECT * FROM join_table;
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_portfolio_df(self, fund: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH
                port_query AS(
                    SELECT 
                         date,
                         fund,
                         "StartingValue"::DECIMAL AS starting_value,
                         "EndingValue"::DECIMAL - "DepositsWithdrawals"::DECIMAL AS ending_value
                    FROM delta_nav
                    WHERE fund = '{fund}'
                ),
                dividends_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossRate"::DECIMAL) AS div_gross_rate,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount
                    FROM dividends
                    WHERE fund = '{fund}'
                        AND "Symbol" = 'IWV'
                    GROUP BY date, fund, "Symbol"
                    ORDER BY date
                ),
                bmk_query AS(
                    SELECT 
                        b.date,
                        LAG(b.ending_value) OVER (ORDER BY b.date) AS starting_value,
                        b.ending_value,
                        d.div_gross_rate,
                        d.div_gross_amount
                    FROM benchmark b
                    LEFT JOIN dividends_query d ON b.date = d.date AND d.ticker = 'IWV' AND d.fund = 'undergrad'
                ),
                bmk_xf AS(
                    SELECT
                        date,
                        (ending_value) / starting_value - 1 AS return --  + COALESCE(div_gross_rate, 0)
                    FROM bmk_query
                    WHERE starting_value <> 0
                ),
                port_xf AS(
                    SELECT
                        date,
                        fund,
                        starting_value,
                        ending_value,
                        ending_value / starting_value - 1 AS return
                    FROM port_query
                ),
                join_table AS (
                    SELECT 
                         p.date,
                         p.fund,
                         p.starting_value,
                         p.ending_value,
                         p.return,
                         b.return as bmk_return,
                         r.return AS rf_return,
                         p.return - r.return AS xs_return,
                         b.return - r.return AS xs_bmk_return
                    FROM port_xf p
                    INNER JOIN bmk_xf b ON p.date = b.date
                    INNER JOIN risk_free_rate r ON p.date = r.date
                    WHERE starting_value <> 0
                        AND ending_value / starting_value - 1 <> 0
                        AND p.date BETWEEN '{start_date}' AND '{end_date}'
                )
            SELECT * FROM join_table
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_all_holdings_df(self, fund: str, start_date: str, end_date: str) -> pd.DataFrame:
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
                        "PositionValue"::DECIMAL AS value,
                        "FXRateToBase":: DECIMAL AS fx_rate_1
                    FROM positions
                    WHERE fund = '{fund}' AND "AssetClass" != 'OPT'
                    ORDER BY date
                ),
                positions_xf AS (
                    SELECT
                        *,
                        CASE WHEN shares_1 > 0 THEN 1 ELSE -1 END AS side,
                        LAG(shares_1) OVER (PARTITION BY ticker ORDER BY date) as shares_0,
                        LAG(price_1) OVER (PARTITION BY ticker ORDER BY date) as price_0
                        LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY date) as fx_rate_0
                    FROM positions_query
                ),
                dividends_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossRate"::DECIMAL) AS div_gross_rate,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount
                    FROM dividends
                    WHERE fund = '{fund}'
                    GROUP BY date, fund, "Symbol"
                ),
                trades_query AS(
                    SELECT
                        date,
                        fund,
                        "Symbol" as ticker,
                        SUM("Quantity"::DECIMAL) as shares_traded
                    FROM trades
                    WHERE fund = '{fund}'
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
                           p.side * ((p.price_1 * (p.shares_1 - COALESCE(shares_traded,0)) * fx_rate_1) / (p.price_0 * p.shares_0 * fx_rate_0) - 1) AS return -- + COALESCE(d.div_gross_amount, 0)
                    FROM positions_xf p
                    LEFT JOIN trades_query t ON p.date = t.date AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.date = d.date AND p.ticker = d.ticker AND p.fund = d.fund
                ),
                bmk_query AS(
                    SELECT 
                        b.date,
                        LAG(b.ending_value) OVER (ORDER BY b.date) AS starting_value,
                        b.ending_value,
                        d.div_gross_rate,
                        d.div_gross_amount
                    FROM benchmark b
                    LEFT JOIN dividends_query d ON b.date = d.date AND d.ticker = 'IWV'
                ),
                bmk_xf AS(
                    SELECT
                        date,
                        (ending_value ) / starting_value - 1 AS return -- + COALESCE(div_gross_rate, 0)
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
                        AND a.date BETWEEN '{start_date}' AND '{end_date}'
                )
            SELECT * FROM join_table_2
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
                        "PositionValue"::DECIMAL AS value,
                        "FXRateToBase":: DECIMAL AS fx_rate_1
                    FROM positions
                    WHERE fund = '{fund}'
                        AND "Symbol" = '{ticker}' 
                        AND "AssetClass" != 'OPT'
                    ORDER BY date
                ),
                positions_xf AS (
                    SELECT
                        *,
                        CASE WHEN shares_1 > 0 THEN 1 ELSE -1 END AS side,
                        LAG(shares_1) OVER (PARTITION BY ticker ORDER BY date) as shares_0,
                        LAG(price_1) OVER (PARTITION BY ticker ORDER BY date) as price_0,
                        LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY date) as fx_rate_0
                    FROM positions_query
                ),
                dividends_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossRate"::DECIMAL) AS div_gross_rate,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount
                    FROM dividends
                    WHERE fund = '{fund}'
                    GROUP BY date, fund, "Symbol"
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
                           p.side * ((p.price_1 * (p.shares_1 - COALESCE(shares_traded,0)) * fx_rate_1 ) / (p.price_0 * p.shares_0 * fx_rate_0) - 1) AS return -- + COALESCE(d.div_gross_amount, 0)
                    FROM positions_xf p
                    LEFT JOIN trades_query t ON p.date = t.date AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.date = d.date AND p.ticker = d.ticker AND p.fund = d.fund
                ),
                bmk_query AS(
                    SELECT 
                        b.date,
                        LAG(b.ending_value) OVER (ORDER BY b.date) AS starting_value,
                        b.ending_value,
                        d.div_gross_rate,
                        d.div_gross_amount
                    FROM benchmark b
                    LEFT JOIN dividends_query d ON b.date = d.date AND d.ticker = 'IWV'
                ),
                bmk_xf AS(
                    SELECT
                        date,
                        (ending_value ) / starting_value - 1 AS return -- + COALESCE(div_gross_rate, 0)
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
                        AND a.date BETWEEN '{start_date}' AND '{end_date}'
                )
            SELECT * FROM join_table_2
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_tickers(self, fund, start_date, end_date) -> np.ndarray:
        query_string = f'''
            SELECT "Symbol"
            FROM positions
            WHERE fund = '{fund}'
                AND "AssetClass" != 'OPT'
                AND date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY "Symbol"
            HAVING COUNT(*) > 1; -- Only include tickers with more than 1 day of data
        '''

        df = self.db.execute_query(query_string)

        return df['Symbol'].tolist()

    def get_current_tickers(self, fund):
        query_string = f'''
        SELECT "Symbol"
        FROM positions
        WHERE fund = '{fund}'
            AND "AssetClass" != 'OPT'
            AND date = (SELECT MAX(date) FROM positions)
        ;
        '''

        df = self.db.execute_query(query_string)

        return df['Symbol'].tolist()



