import numpy as np
import pandas as pd
from shared.database import Database

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
                        COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.date)) AS rf_return,
                        f.return - COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.date)) AS xs_return
                    FROM fund_xf f
                    LEFT JOIN risk_free_rate r ON f.date = r.date
                    LEFT JOIN dividends_xf d ON f.date = d.date
                    WHERE f.return <> 0
                        AND f.date BETWEEN '{start_date}' AND '{end_date}'
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
                        COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.date)) AS rf_return,
                        p.return - COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.date)) AS xs_return
                    FROM port_xf p
                    LEFT JOIN risk_free_rate r ON p.date = r.date
                    LEFT JOIN dividends_xf d ON p.date = d.date
                    WHERE starting_value <> 0
                        AND p.return <> 0
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
                        CASE WHEN "Quantity"::DECIMAL > 0 THEN 1 ELSE -1 END AS side,
                        "Quantity"::DECIMAL AS shares_1,
                        "MarkPrice"::DECIMAL AS price_1,
                        "FXRateToBase":: DECIMAL AS fx_rate_1
                    FROM positions
                    WHERE fund = '{fund}'
                        AND "Symbol" = '{ticker}'
                        AND "AssetClass" != 'OPT'
                    ORDER BY date
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
                        AND "Symbol" = '{ticker}'
                    GROUP BY date, fund, "Symbol"
                ),
                trades_query AS (
                    SELECT
                        date,
                        fund,
                        "Symbol" AS ticker,
                        CASE WHEN SUM("Quantity"::DECIMAL) > 0 THEN 1 ELSE -1 END AS trade_type,
                        SUM("Quantity"::DECIMAL) AS shares_traded,
                        AVG("TradePrice"::DECIMAL) AS trade_price
                     FROM trades
                     WHERE fund = '{fund}'
                       AND "Symbol" = '{ticker}'
                       AND "AssetClass" != 'OPT'
                     GROUP BY date, fund, "Symbol"
                ),
                trades_xf AS(
                    SELECT
                        date,
                        fund,
                        ticker,
                        trade_type,
                        shares_traded,
                        trade_price
                    FROM trades_query
                    WHERE shares_traded <> 0
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
                join_table_1 AS( -- Merge trades and dividends into positions
                    SELECT
                           COALESCE(p.date, t.date) AS date,
                           COALESCE(p.fund, t.fund) AS fund,
                           COALESCE(p.ticker, t.ticker) AS ticker,
                           p.side,
                           p.shares_1,
                           p.price_1,
                           p.fx_rate_1,
                           t.trade_type,
                           COALESCE(t.shares_traded, 0) AS shares_traded,
                           t.trade_price,
                           COALESCE(d.div_gross_rate,0) as div_gross_rate,
                           COALESCE(d.div_gross_amount, 0) AS dividends
                    FROM positions_query p
                    FULL JOIN trades_xf t ON p.date = t.date AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.date = d.date AND p.ticker = d.ticker AND p.fund = d.fund
                    ORDER BY COALESCE(p.date, t.date)
                ),
                join_table_2 AS( -- Coalesce and lag missing values
                    SELECT
                           date,
                           fund,
                           ticker,
                           COALESCE(LAG(side) OVER (PARTITION BY ticker ORDER BY date), side) AS side,
                           COALESCE(LAG(shares_1) OVER (PARTITION BY ticker ORDER BY date), shares_traded) AS shares_0,
                           COALESCE(shares_1 - shares_traded , shares_traded * trade_type) AS shares_1,
                           COALESCE(LAG(price_1) OVER (PARTITION BY ticker ORDER BY date), trade_price) AS price_0,
                           COALESCE(price_1, trade_price) AS price_1,
                           COALESCE(LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY date), fx_rate_1) AS fx_rate_0,
                           COALESCE(fx_rate_1, LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY date)) AS fx_rate_1,
                           trade_type,
                           shares_traded,
                           trade_price,
                           div_gross_rate,
                           dividends
                    FROM join_table_1
                ),
                join_table_3 AS(
                    SELECT
                           date,
                           fund,
                           ticker,
                           side,
                           shares_0,
                           CASE WHEN shares_1 = 0 THEN shares_0 ELSE shares_1 END AS shares_1,
                           price_0,
                           price_1,
                           fx_rate_0,
                           fx_rate_1,
                           shares_0 * price_0 * fx_rate_0 AS value_0,
                           CASE WHEN shares_1 = 0 THEN shares_0 ELSE shares_1 END * price_1 * fx_rate_1 AS value_1,
                           trade_type,
                           shares_traded,
                           trade_price,
                           div_gross_rate,
                           dividends
                    FROM join_table_2
                ),
                join_table_4 AS( -- Compute weights and returns
                    SELECT
                        p.date,
                        p.fund,
                        ticker,
                        p.side,
                        shares_1,
                        price_1,
                        fx_rate_1,
                        value_1,
                        n.total_stock_1,
                        value_1 / n.total_stock_1 AS weight_1,
                        div_gross_rate,
                        dividends,
                        side * value_1 / value_0 - 1 AS return,
                        side * (value_1 + dividends) / value_0 - 1 AS div_return
                    FROM join_table_3 p
                    LEFT JOIN nav_xf n ON p.date = n.date AND p.fund = n.fund
                ),
                join_table_5 AS( -- Compute excess returns
                    SELECT
                       a.date,
                       a.fund,
                       a.ticker,
                       a.side,
                       a.price_1 AS price,
                       a.shares_1 AS shares,
                       a.fx_rate_1 AS fx_rate,
                       a.value_1 AS value,
                       a.weight_1 AS weight,
                       a.div_gross_rate / a.price_1 AS dividend_yield,
                       a.dividends,
                       a.return,
                       a.div_return,
                       COALESCE(c.return, LAG(c.return) OVER (ORDER BY c.date)) AS rf_return,
                       a.return - COALESCE(c.return, LAG(c.return) OVER (ORDER BY c.date)) AS xs_return,
                       a.div_return - COALESCE(c.return, LAG(c.return) OVER (ORDER BY c.date)) AS xs_div_return
                    FROM join_table_4 a
                    LEFT JOIN risk_free_rate c ON a.date = c.date
                    INNER JOIN calendar d ON a.date = d.date -- remove holidays
                    WHERE a.date BETWEEN '{start_date}' AND '{end_date}'
                )
            SELECT * FROM join_table_5;
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
                        CASE WHEN "Quantity"::DECIMAL > 0 THEN 1 ELSE -1 END AS side,
                        "Quantity"::DECIMAL AS shares_1,
                        "MarkPrice"::DECIMAL AS price_1,
                        "FXRateToBase":: DECIMAL AS fx_rate_1
                    FROM positions
                    WHERE fund = '{fund}'
                        AND "AssetClass" != 'OPT'
                    ORDER BY ticker, date
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
                        CASE WHEN SUM("Quantity"::DECIMAL) > 0 THEN 1 ELSE -1 END AS trade_type,
                        SUM("Quantity"::DECIMAL) AS shares_traded,
                        AVG("TradePrice"::DECIMAL) as trade_price
                    FROM trades
                    WHERE fund = '{fund}'
                        AND "AssetClass" != 'OPT'
                    GROUP BY date, fund, "Symbol"
                ),
                trades_xf AS(
                    SELECT
                        date,
                        fund,
                        ticker,
                        trade_type,
                        shares_traded,
                        trade_price
                    FROM trades_query
                    WHERE shares_traded <> 0
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
                join_table_1 AS( -- Merge trades and dividends into positions
                    SELECT
                        COALESCE(p.date, t.date) AS date,
                        COALESCE(p.fund, t.fund) AS fund,
                        COALESCE(p.ticker, t.ticker) AS ticker,
                        p.side,
                        p.shares_1,
                        p.price_1,
                        p.fx_rate_1,
                        t.trade_type,
                        COALESCE(t.shares_traded, 0) AS shares_traded,
                        t.trade_price,
                        COALESCE(d.div_gross_rate,0) as div_gross_rate,
                        COALESCE(d.div_gross_amount, 0) AS dividends
                    FROM positions_query p
                    FULL JOIN trades_xf t ON p.date = t.date AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.date = d.date AND p.ticker = d.ticker AND p.fund = d.fund
                    ORDER BY ticker, COALESCE(p.date, t.date)
                ),
                join_table_2 AS( -- Coalesce and lag missing values
                    SELECT
                        date,
                        fund,
                        ticker,
                        COALESCE(LAG(side) OVER (PARTITION BY ticker ORDER BY date), side) AS side,
                        COALESCE(LAG(shares_1) OVER (PARTITION BY ticker ORDER BY date), shares_traded) AS shares_0,
                        COALESCE(shares_1 - CASE WHEN trade_type = 1 THEN 0 ELSE shares_traded END, shares_traded * trade_type) AS shares_1,
                        COALESCE(LAG(price_1) OVER (PARTITION BY ticker ORDER BY date), trade_price) AS price_0,
                        COALESCE(price_1, trade_price) AS price_1,
                        COALESCE(LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY date), fx_rate_1) AS fx_rate_0,
                        COALESCE(fx_rate_1, LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY date)) AS fx_rate_1,
                        trade_type,
                        shares_traded,
                        trade_price,
                        div_gross_rate,
                        dividends
                    FROM join_table_1
                ),
                join_table_3 AS(
                    SELECT
                        date,
                        fund,
                        ticker,
                        side,
                        shares_0,
                        shares_1,
                        price_0,
                        price_1,
                        fx_rate_0,
                        fx_rate_1,
                        shares_0 * price_0 * fx_rate_0 AS value_0,
                        shares_1 * price_1 * fx_rate_1 AS value_1,
                        trade_type,
                        shares_traded,
                        trade_price,
                        div_gross_rate,
                        dividends
                    FROM join_table_2
                ),
                join_table_4 AS( -- Compute weights and returns
                    SELECT
                        p.date,
                        p.fund,
                        ticker,
                        p.side,
                        shares_1,
                        price_1,
                        fx_rate_1,
                        value_1,
                        n.total_stock_1,
                        value_1 / n.total_stock_1 AS weight_1,
                        div_gross_rate,
                        dividends,
                        side * value_1 / value_0 - 1 AS return,
                        side * (value_1 + dividends) / value_0 - 1 AS div_return
                    FROM join_table_3 p
                    LEFT JOIN nav_xf n ON p.date = n.date AND p.fund = n.fund
                    ORDER BY ticker, p.date
                ),
                join_table_5 AS( -- Compute excess returns
                    SELECT
                        a.date,
                        a.fund,
                        a.ticker,
                        a.side,
                        a.price_1 AS price,
                        a.shares_1 AS shares,
                        a.fx_rate_1 AS fx_rate,
                        a.value_1 AS value,
                        a.weight_1 AS weight,
                        a.div_gross_rate / a.price_1 AS dividend_yield,
                        a.dividends,
                        a.return,
                        a.div_return,
                        COALESCE(c.return, LAG(c.return) OVER (PARTITION BY a.ticker ORDER BY c.date)) AS rf_return,
                        a.return - COALESCE(c.return, LAG(c.return) OVER (PARTITION BY a.ticker ORDER BY c.date)) AS xs_return,
                        a.div_return - COALESCE(c.return, LAG(c.return) OVER (PARTITION BY a.ticker ORDER BY c.date)) AS xs_div_return
                    FROM join_table_4 a
                    LEFT JOIN risk_free_rate c ON a.date = c.date
                    INNER JOIN calendar d ON a.date = d.date -- remove holidays
                    WHERE a.date BETWEEN '{start}' AND '{end}'
                    ORDER BY ticker, a.date
                )
                SELECT * FROM join_table_5;
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
                    COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.date)) AS rf_return,
                    b.return - COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.date)) AS xs_return,
                    b.div_return - COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.date)) AS xs_div_return
                FROM bmk_xf b
                LEFT JOIN risk_free_rate r ON r.date = b.date
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
            ORDER BY "Symbol";
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
        ORDER BY "Symbol";
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
    
    def get_cron_log(self) -> pd.DataFrame:
        query_string = f'''
        SELECT * FROM "ETL_Cron_Log"
        ORDER BY date DESC
        LIMIT 8
        ;
        '''

        df = self.db.execute_query(query_string)

        return df



