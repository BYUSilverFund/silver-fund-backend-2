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
                    GROUP BY date
                    ORDER BY date
                ),
                fund_xf AS(
                    SELECT
                        date,
                        starting_value,
                        ending_value,
                        ending_value / starting_value - 1 AS return
                    FROM fund_query
                ),
                dividends_query AS(
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount
                    FROM dividends
                    GROUP BY date, fund, "Symbol"
                ),
                dividends_xf AS(
                    SELECT
                        date,
                        SUM(div_gross_amount) AS dividends
                    FROM dividends_query
                    GROUP BY date
                ),
                join_table AS(
                    SELECT
                        f.date,
                        f.starting_value,
                        f.ending_value,
                        COALESCE(d.dividends, 0) AS dividends,
                        f.return,
                        r.return AS rf_return,
                        f.return - r.return AS xs_return
                    FROM fund_xf f
                    INNER JOIN risk_free_rate r ON f.date = r.date
                    LEFT JOIN dividends_xf d ON f.date = d.date
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
                port_xf AS(
                    SELECT
                        p.date,
                        p.fund,
                        starting_value,
                        ending_value,
                        ending_value / starting_value - 1 AS return
                    FROM port_query p
                ),
                dividends_query AS(
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount
                    FROM dividends
                    WHERE fund = '{fund}'
                    GROUP BY date, fund, "Symbol"
                ),
                dividends_xf AS(
                    SELECT
                        date,
                        fund,
                        SUM(div_gross_amount) AS dividends
                    FROM dividends_query
                    GROUP BY date, fund
                ),
                join_table AS (
                    SELECT 
                         p.date,
                         p.fund,
                         p.starting_value,
                         p.ending_value,
                         COALESCE(d.dividends, 0) AS dividends,
                         p.return,
                         r.return AS rf_return,
                         p.return - r.return AS xs_return
                    FROM port_xf p
                    INNER JOIN risk_free_rate r ON p.date = r.date
                    LEFT JOIN dividends_xf d ON p.date = d.date
                    WHERE starting_value <> 0
                        AND ending_value / starting_value - 1 <> 0
                        AND p.date BETWEEN '{start_date}' AND '{end_date}'
                )
            SELECT * FROM join_table
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
                        "PositionValue"::DECIMAL AS value_1,
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
                        LAG(value_1) OVER (PARTITION BY ticker ORDER BY date) as value_0,
                        LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY date) as fx_rate_0
                    FROM positions_query
                ),
                dividends_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossRate"::DECIMAL) AS div_gross_rate,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount -- Sometimes dividends gets double counted
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
                nav_query AS(
                    SELECT
                        date,
                        fund,
                        "Stock"::DECIMAL AS total_stock_1
                    FROM nav
                    WHERE fund = '{fund}'
                ),
                nav_xf AS(
                    SELECT
                        date,
                        fund,
                        total_stock_1,
                        LAG(total_stock_1) OVER (ORDER BY date) AS total_stock_0
                    FROM nav_query
                ),
                join_table_1 AS(
                    SELECT p.date,
                           p.fund,
                           p.ticker,
                           p.name,
                           p.value_0,
                           p.value_1,
                           p.value_0 / n.total_stock_0 AS weight_0,
                           p.value_1 / n.total_stock_1 AS weight_1,
                           p.shares_0,
                           (p.shares_1 - COALESCE(shares_traded,0)) AS shares_1, --Adjusts shares_1 for trades made on that day
                           t.shares_traded,
                           p.price_0,
                           p.price_1,
                           p.side,
                           d.div_gross_rate,
                           d.div_gross_amount AS dividends,
                           p.side * ((p.price_1 * (p.shares_1 - COALESCE(shares_traded,0)) * fx_rate_1 ) / (p.price_0 * p.shares_0 * fx_rate_0) - 1) AS return,
                           p.side * (((p.price_1 * (p.shares_1 - COALESCE(shares_traded,0)) + COALESCE(d.div_gross_amount, 0)) * fx_rate_1) / (p.price_0 * p.shares_0 * fx_rate_0) - 1) AS div_return 
                    FROM positions_xf p
                    LEFT JOIN trades_query t ON p.date = t.date AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.date = d.date AND p.ticker = d.ticker AND p.fund = d.fund
                    LEFT JOIN nav_xf n ON p.date = n.date AND p.fund = n.fund
                ),
                join_table_2 AS(
                    SELECT
                       a.date,
                       a.fund,
                       a.ticker,
                       a.name,
                       a.weight_1 AS weight,
                       a.shares_1 AS shares,
                       a.price_1 AS price,
                       a.value_1 AS value,
                       a.side,
                       a.div_gross_rate / a.price_1 AS dividend_yield,
                       a.dividends,
                       a.return,
                       a.div_return,                       
                       c.return AS rf_return,
                       a.return - c.return AS xs_return,
                       a.div_return - c.return AS xs_div_return
                    FROM join_table_1 a
                    INNER JOIN risk_free_rate c ON a.date = c.date
                    WHERE a.return <> 0
                        AND a.shares_1 <> 0
                        AND a.date BETWEEN '{start_date}' AND '{end_date}'
                )
            SELECT * FROM join_table_2
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_all_holdings_df(self, fund: str, start: str, end: str):
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
                        "PositionValue"::DECIMAL AS value_1,
                        "FXRateToBase":: DECIMAL AS fx_rate_1
                    FROM positions
                    WHERE fund = '{fund}'
                        AND "AssetClass" != 'OPT'
                    ORDER BY date
                ),
                positions_xf AS (
                    SELECT
                        *,
                        CASE WHEN shares_1 > 0 THEN 1 ELSE -1 END AS side,
                        LAG(shares_1) OVER (PARTITION BY ticker ORDER BY date) as shares_0,
                        LAG(price_1) OVER (PARTITION BY ticker ORDER BY date) as price_0,
                        LAG(value_1) OVER (PARTITION BY ticker ORDER BY date) as value_0,
                        LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY date) as fx_rate_0
                    FROM positions_query
                ),
                dividends_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossRate"::DECIMAL) AS div_gross_rate,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount -- Sometimes dividends gets double counted
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
                nav_query AS(
                    SELECT
                        date,
                        fund,
                        "Stock"::DECIMAL AS total_stock_1
                    FROM nav
                    WHERE fund = '{fund}'
                ),
                nav_xf AS(
                    SELECT
                        date,
                        fund,
                        total_stock_1,
                        LAG(total_stock_1) OVER (ORDER BY date) AS total_stock_0
                    FROM nav_query
                ),
                join_table_1 AS(
                    SELECT p.date,
                           p.fund,
                           p.ticker,
                           p.name,
                           p.value_0 / n.total_stock_0 AS weight_0,
                           p.value_1 / n.total_stock_1 AS weight_1,
                           p.shares_0,
                           (p.shares_1 - COALESCE(shares_traded,0)) AS shares_1, --Adjusts shares_1 for trades made on that day
                           t.shares_traded,
                           p.price_0,
                           p.price_1,
                           p.side,
                           p.value_0,
                           p.value_1,
                           d.div_gross_rate,
                           d.div_gross_amount AS dividends,
                           p.side * ((p.price_1 * (p.shares_1 - COALESCE(shares_traded,0)) * fx_rate_1 ) / (p.price_0 * p.shares_0 * fx_rate_0) - 1) AS return,
                           p.side * (((p.price_1 * (p.shares_1 - COALESCE(shares_traded,0)) + COALESCE(d.div_gross_amount, 0)) * fx_rate_1) / (p.price_0 * p.shares_0 * fx_rate_0) - 1) AS div_return
                    FROM positions_xf p
                    LEFT JOIN trades_query t ON p.date = t.date AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.date = d.date AND p.ticker = d.ticker AND p.fund = d.fund
                    LEFT JOIN nav_xf n ON p.date = n.date AND p.fund = n.fund
                ),
                join_table_2 AS(
                    SELECT
                       a.date,
                       a.fund,
                       a.ticker,
                       a.name,
                       a.weight_1 AS weight,
                       a.shares_1 AS shares,
                       a.price_1 AS price,
                       a.value_1 AS value,
                       a.side,
                       a.div_gross_rate / a.price_1 AS dividend_yield,
                       a.dividends,
                       a.return,
                       a.div_return,
                       c.return AS rf_return,
                       a.return - c.return AS xs_return,
                       a.div_return - c.return AS xs_div_return
                    FROM join_table_1 a
                    INNER JOIN risk_free_rate c ON a.date = c.date
                    WHERE a.return <> 0
                        AND a.date BETWEEN '{start}' AND '{end}'
                )
                SELECT * FROM join_table_2
        ;
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_benchmark_df(self, start: str, end: str):
        query_string = f"""
            WITH
            bmk_query AS(
                SELECT
                    date,
                    LAG(ending_value) OVER (ORDER BY date) AS starting_value,
                    ending_value
                FROM benchmark
            ),
            dividend_query AS(
                SELECT
                    date,
                    AVG("GrossRate"::DECIMAL) AS div_gross_rate
                FROM dividends
                WHERE fund = 'undergrad' AND "Symbol" = 'IWV'
                GROUP BY date
            ),
            bmk_xf AS(
                SELECT
                    b.date,
                    b.starting_value,
                    b.ending_value,
                    d.div_gross_rate AS dividends,
                    d.div_gross_rate / b.ending_value AS dividend_yield,
                    b.ending_value / b.starting_value - 1 AS return,
                    (b.ending_value + COALESCE(d.div_gross_rate,0)) / b.starting_value - 1 AS div_return
                FROM bmk_query b
                LEFT JOIN dividend_query d ON d.date = b.date
            ),
            join_table AS(
                SELECT
                    b.date,
                    b.starting_value,
                    b.ending_value,
                    b.dividends,
                    b.dividend_yield,
                    b.return,
                    b.div_return,
                    r.return AS rf_return,
                    b.return - r.return AS xs_return,
                    b.div_return - r.return AS xs_div_return
                FROM bmk_xf b
                INNER JOIN risk_free_rate r ON r.date = b.date
                WHERE b.ending_value / b.starting_value <> 1
                    AND b.date BETWEEN '{start}' AND '{end}'
            )
            SELECT * FROM join_table;
        """

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
            HAVING COUNT(*) > 2; -- Only include tickers with more than 2 days of data
        '''

        df = self.db.execute_query(query_string)

        return df['Symbol'].tolist()

    def get_current_tickers(self, fund):
        query_string = f'''
        SELECT "Symbol"
        FROM positions
        WHERE fund = '{fund}'
            AND "AssetClass" != 'OPT'
            AND date = (SELECT MAX(date) FROM positions WHERE fund = '{fund}')
        ;
        '''

        df = self.db.execute_query(query_string)

        return df['Symbol'].tolist()

    def get_dividends(self, fund, ticker, start, end):
        query_string = f'''
        SELECT
            fund,
            date,
            "Symbol" AS ticker,
            AVG("GrossRate"::DECIMAL) AS gross_rate,
            AVG("GrossAmount"::DECIMAL) AS gross_amount
        FROM dividends
        WHERE "Symbol" = '{ticker}'
            AND fund = '{fund}'
            AND date BETWEEN '{start}' AND '{end}'
        GROUP BY fund, date, "Symbol"
        ;
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_trades(self, fund, ticker, start, end):
        query_string = f'''
        SELECT
            date,
            fund,
            "Symbol" AS ticker,
            SUM("Quantity"::DECIMAL) AS shares,
            AVG("TradePrice"::DECIMAL) AS price,
            SUM("TradeMoney"::DECIMAL) AS value,
            "Buy/Sell" AS side
        FROM trades
        WHERE fund = '{fund}'
            AND "Symbol" = '{ticker}'
            AND date BETWEEN '{start}' AND '{end}'
        GROUP BY date, fund, "Symbol", "Buy/Sell"
        ;
        '''

        df = self.db.execute_query(query_string)

        return df



