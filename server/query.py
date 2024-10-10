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
                        CALDT,
                        SUM(STARTING_VALUE) as STARTING_VALUE,
                        SUM(ENDING_VALUE) as ENDING_VALUE
                    FROM DELTA_NAV
                    GROUP BY CALDT
                    ORDER BY CALDT
                ),
                fund_xf AS(
                    SELECT
                        CALDT,
                        starting_value,
                        ending_value,
                        ending_value / starting_value - 1 AS return
                    FROM fund_query
                    WHERE starting_value <> 0
                ),
                dividends_query AS(
                    SELECT
                        CALDT,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount
                    FROM dividends
                    GROUP BY CALDT, fund, "Symbol"
                ),
                dividends_xf AS(
                    SELECT
                        CALDT,
                        SUM(div_gross_amount) AS dividends
                    FROM dividends_query
                    GROUP BY CALDT
                ),
                join_table AS(
                    SELECT
                        f.CALDT,
                        f.starting_value,
                        f.ending_value,
                        COALESCE(d.dividends, 0) AS dividends,
                        f.return,
                        COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.CALDT)) AS rf_return,
                        f.return - COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.CALDT)) AS xs_return
                    FROM fund_xf f
                    LEFT JOIN risk_free_rate r ON f.CALDT = r.CALDT
                    LEFT JOIN dividends_xf d ON f.CALDT = d.CALDT
                    WHERE f.return <> 0
                        AND f.CALDT BETWEEN '{start_date}' AND '{end_date}'
                    ORDER BY CALDT
                )
            SELECT * FROM join_table;
        '''

        df = self.db.get_dataframe(query_string)

        return df

    def get_portfolio_df(self, fund: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH
                port_query AS(
                    SELECT 
                         CALDT,
                         fund,
                         "StartingValue"::DECIMAL AS starting_value,
                         "EndingValue"::DECIMAL - "DepositsWithdrawals"::DECIMAL AS ending_value
                    FROM delta_nav
                    WHERE fund = '{fund}'
                ),
                port_xf AS(
                    SELECT
                        p.CALDT,
                        p.fund,
                        starting_value,
                        ending_value,
                        ending_value / starting_value - 1 AS return
                    FROM port_query p
                ),
                dividends_query AS(
                    SELECT
                        CALDT,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount
                    FROM dividends
                    WHERE fund = '{fund}'
                    GROUP BY CALDT, fund, "Symbol"
                ),
                dividends_xf AS(
                    SELECT
                        CALDT,
                        fund,
                        SUM(div_gross_amount) AS dividends
                    FROM dividends_query
                    GROUP BY CALDT, fund
                ),
                join_table AS (
                    SELECT 
                         p.CALDT,
                         p.fund,
                         p.starting_value,
                         p.ending_value,
                         COALESCE(d.dividends, 0) AS dividends,
                         p.return,
                        COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.CALDT)) AS rf_return,
                        p.return - COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.CALDT)) AS xs_return
                    FROM port_xf p
                    LEFT JOIN risk_free_rate r ON p.CALDT = r.CALDT
                    LEFT JOIN dividends_xf d ON p.CALDT = d.CALDT
                    WHERE starting_value <> 0
                        AND p.return <> 0
                        AND ending_value / starting_value - 1 <> 0
                        AND p.CALDT BETWEEN '{start_date}' AND '{end_date}'
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
                        CALDT,
                        fund,
                        "Symbol" AS ticker,
                        CASE WHEN AVG("Quantity"::DECIMAL) > 0 THEN 1 ELSE -1 END AS side,
                        AVG("Quantity"::DECIMAL) AS shares_1,
                        AVG("MarkPrice"::DECIMAL) AS price_1,
                        AVG("FXRateToBase":: DECIMAL) AS fx_rate_1
                    FROM positions
                    WHERE fund = '{fund}'
                        AND "Symbol" = '{ticker}'
                        AND "AssetClass" != 'OPT'
                    GROUP BY CALDT, fund, "Symbol"
                    ORDER BY CALDT
                ),
                dividends_query AS (
                    SELECT
                        CALDT,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossRate"::DECIMAL) AS div_gross_rate,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount -- Sometimes dividends gets double counted
                    FROM dividends
                    WHERE fund = '{fund}'
                        AND "Symbol" = '{ticker}'
                    GROUP BY CALDT, fund, "Symbol"
                ),
                trades_query AS (
                    SELECT
                        CALDT,
                        fund,
                        "Symbol" AS ticker,
                        CASE WHEN SUM("Quantity"::DECIMAL) > 0 THEN 1 ELSE -1 END AS trade_type,
                        SUM("Quantity"::DECIMAL) AS shares_traded,
                        AVG("TradePrice"::DECIMAL) AS trade_price
                     FROM trades
                     WHERE fund = '{fund}'
                       AND "Symbol" = '{ticker}'
                       AND "AssetClass" != 'OPT'
                       AND "AssetClass" != 'CASH'
                       AND "Buy/Sell" != 'SELL (Ca.)'
                     GROUP BY CALDT, fund, "Symbol"
                ),
                trades_xf AS(
                    SELECT
                        CALDT,
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
                        CALDT,
                        fund,
                        "Stock"::DECIMAL AS total_stock_1
                    FROM nav
                    WHERE fund = '{fund}'
                        AND "Stock"::DECIMAL <> 0
                ),
                join_table_1 AS( -- Merge trades and dividends into positions
                    SELECT
                           COALESCE(p.CALDT, t.CALDT) AS CALDT,
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
                    FULL JOIN trades_xf t ON p.CALDT = t.CALDT AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.CALDT = d.CALDT AND p.ticker = d.ticker AND p.fund = d.fund
                    ORDER BY COALESCE(p.CALDT, t.CALDT)
                ),
                join_table_2 AS( -- Coalesce and lag missing values
                    SELECT
                           CALDT,
                           fund,
                           ticker,
                           COALESCE(LAG(side) OVER (PARTITION BY ticker ORDER BY CALDT), side) AS side,
                           COALESCE(LAG(shares_1) OVER (PARTITION BY ticker ORDER BY CALDT), shares_traded) AS shares_0,
                           COALESCE(CASE WHEN shares_1 - shares_traded = 0 THEN shares_1 ELSE shares_1 - shares_traded END , shares_traded * -1) AS shares_1,
                           COALESCE(LAG(price_1) OVER (PARTITION BY ticker ORDER BY CALDT), trade_price) AS price_0,
                           COALESCE(price_1, trade_price) AS price_1,
                           COALESCE(LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY CALDT), fx_rate_1) AS fx_rate_0,
                           COALESCE(fx_rate_1, LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY CALDT)) AS fx_rate_1,
                           trade_type,
                           shares_traded,
                           trade_price,
                           div_gross_rate,
                           dividends
                    FROM join_table_1
                ),
                join_table_3 AS(
                    SELECT
                           CALDT,
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
                        p.CALDT,
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
                        CASE WHEN side = 1 THEN (value_1 / value_0 - 1) ELSE (value_0 / value_1 - 1) END AS return,
                        CASE WHEN side = 1 THEN ((value_1 + dividends) / value_0 - 1) ELSE (value_0 / (value_1 + dividends) - 1) END AS div_return
                    FROM join_table_3 p
                    LEFT JOIN nav_query n ON p.CALDT = n.CALDT AND p.fund = n.fund
                ),
                join_table_5 AS( -- Compute excess returns
                    SELECT
                       a.CALDT,
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
                       COALESCE(c.return, LAG(c.return) OVER (ORDER BY c.CALDT)) AS rf_return,
                       a.return - COALESCE(c.return, LAG(c.return) OVER (ORDER BY c.CALDT)) AS xs_return,
                       a.div_return - COALESCE(c.return, LAG(c.return) OVER (ORDER BY c.CALDT)) AS xs_div_return
                    FROM join_table_4 a
                    LEFT JOIN risk_free_rate c ON a.CALDT = c.CALDT
                    INNER JOIN calendar d ON a.CALDT = d.CALDT -- remove holidays
                    WHERE a.CALDT BETWEEN '{start_date}' AND '{end_date}'
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
                        CALDT,
                        fund,
                        "Symbol" AS ticker,
                        CASE WHEN AVG("Quantity"::DECIMAL) > 0 THEN 1 ELSE -1 END AS side,
                        AVG("Quantity"::DECIMAL) AS shares_1,
                        AVG("MarkPrice"::DECIMAL) AS price_1,
                        AVG("FXRateToBase":: DECIMAL) AS fx_rate_1
                    FROM positions
                    WHERE fund = '{fund}'
                        AND "AssetClass" != 'OPT'
                        AND "Symbol" != 'VMFXX'
                    GROUP BY CALDT, fund, "Symbol"
                    ORDER BY ticker, CALDT
                ),
                dividends_query AS (
                    SELECT
                        CALDT,
                        fund,
                        "Symbol" AS ticker,
                        AVG("GrossRate"::DECIMAL) AS div_gross_rate,
                        AVG("GrossAmount"::DECIMAL) AS div_gross_amount -- Sometimes dividends gets double counted
                    FROM dividends
                    WHERE fund = '{fund}'
                        AND "Symbol" != 'VMFXX'
                    GROUP BY CALDT, fund, "Symbol"
                ),
                trades_query AS(
                    SELECT
                        CALDT,
                        fund,
                        "Symbol" as ticker,
                        CASE WHEN SUM("Quantity"::DECIMAL) > 0 THEN 1 ELSE -1 END AS trade_type,
                        SUM("Quantity"::DECIMAL) AS shares_traded,
                        AVG("TradePrice"::DECIMAL) as trade_price
                    FROM trades
                    WHERE fund = '{fund}'
                        AND "AssetClass" != 'OPT'
                        AND "AssetClass" != 'CASH'
                        AND "Buy/Sell" != 'SELL (Ca.)'
                        AND "Symbol" != 'VMFXX'
                    GROUP BY CALDT, fund, "Symbol"
                ),
                trades_xf AS(
                    SELECT
                        CALDT,
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
                        CALDT,
                        fund,
                        "Stock"::DECIMAL AS total_stock_1
                    FROM nav
                    WHERE fund = '{fund}'
                        AND "Stock"::DECIMAL <> 0
                ),
                join_table_1 AS( -- Merge trades and dividends into positions
                    SELECT
                        COALESCE(p.CALDT, t.CALDT) AS CALDT,
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
                    FULL JOIN trades_xf t ON p.CALDT = t.CALDT AND p.ticker = t.ticker AND p.fund = t.fund
                    LEFT JOIN dividends_query d ON p.CALDT = d.CALDT AND p.ticker = d.ticker AND p.fund = d.fund
                    ORDER BY ticker, COALESCE(p.CALDT, t.CALDT)
                ),
                join_table_2 AS( -- Coalesce and lag missing values
                    SELECT
                        CALDT,
                        fund,
                        ticker,
                        COALESCE(LAG(side) OVER (PARTITION BY ticker ORDER BY CALDT), side) AS side,
                        COALESCE(LAG(shares_1) OVER (PARTITION BY ticker ORDER BY CALDT), shares_traded) AS shares_0,
                        COALESCE(CASE WHEN shares_1 - shares_traded = 0 THEN shares_1 ELSE shares_1 - shares_traded END , shares_traded * -1) AS shares_1,
                        COALESCE(LAG(price_1) OVER (PARTITION BY ticker ORDER BY CALDT), trade_price) AS price_0,
                        COALESCE(price_1, trade_price) AS price_1,
                        COALESCE(LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY CALDT), fx_rate_1) AS fx_rate_0,
                        COALESCE(fx_rate_1, LAG(fx_rate_1) OVER (PARTITION BY ticker ORDER BY CALDT)) AS fx_rate_1,
                        trade_type,
                        shares_traded,
                        trade_price,
                        div_gross_rate,
                        dividends
                    FROM join_table_1
                ),
                join_table_3 AS(
                    SELECT
                        CALDT,
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
                        p.CALDT,
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
                        CASE WHEN side = 1 THEN (value_1 / value_0 - 1) ELSE (value_0 / value_1 - 1) END AS return,
                        CASE WHEN side = 1 THEN ((value_1 + dividends) / value_0 - 1) ELSE (value_0 / (value_1 + dividends) - 1) END AS div_return
                    FROM join_table_3 p
                    LEFT JOIN nav_query n ON p.CALDT = n.CALDT AND p.fund = n.fund
                    ORDER BY ticker, p.CALDT
                ),
                join_table_5 AS( -- Compute excess returns
                    SELECT
                        a.CALDT,
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
                        COALESCE(c.return, LAG(c.return) OVER (PARTITION BY a.ticker ORDER BY c.CALDT)) AS rf_return,
                        a.return - COALESCE(c.return, LAG(c.return) OVER (PARTITION BY a.ticker ORDER BY c.CALDT)) AS xs_return,
                        a.div_return - COALESCE(c.return, LAG(c.return) OVER (PARTITION BY a.ticker ORDER BY c.CALDT)) AS xs_div_return
                    FROM join_table_4 a
                    LEFT JOIN risk_free_rate c ON a.CALDT = c.CALDT
                    INNER JOIN calendar d ON a.CALDT = d.CALDT -- remove holidays
                    WHERE a.CALDT BETWEEN '{start}' AND '{end}'
                    ORDER BY ticker, a.CALDT
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
                    CALDT,
                    LAG(ending_value) OVER (ORDER BY CALDT) AS starting_value,
                    ending_value
                FROM benchmark
            ),
            dividend_query AS(
                SELECT
                    CALDT,
                    AVG("GrossRate"::DECIMAL) AS div_gross_rate
                FROM dividends
                WHERE fund = 'undergrad' AND "Symbol" = 'IWV'
                GROUP BY CALDT
            ),
            bmk_xf AS(
                SELECT
                    b.CALDT,
                    b.starting_value,
                    b.ending_value,
                    d.div_gross_rate AS dividends,
                    d.div_gross_rate / b.ending_value AS dividend_yield,
                    b.ending_value / b.starting_value - 1 AS return,
                    (b.ending_value + COALESCE(d.div_gross_rate,0)) / b.starting_value - 1 AS div_return
                FROM bmk_query b
                LEFT JOIN dividend_query d ON d.CALDT = b.CALDT
            ),
            join_table AS(
                SELECT
                    b.CALDT,
                    b.starting_value,
                    b.ending_value,
                    b.dividends,
                    b.dividend_yield,
                    b.return,
                    b.div_return,
                    COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.CALDT)) AS rf_return,
                    b.return - COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.CALDT)) AS xs_return,
                    b.div_return - COALESCE(r.return, LAG(r.return) OVER (ORDER BY r.CALDT)) AS xs_div_return
                FROM bmk_xf b
                LEFT JOIN risk_free_rate r ON r.CALDT = b.CALDT
                WHERE b.ending_value / b.starting_value <> 1
                    AND b.CALDT BETWEEN '{start}' AND '{end}'
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
                AND "AssetClass" != 'CASH'
                AND CALDT BETWEEN '{start_date}' AND '{end_date}'
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
            AND "AssetClass" != 'CASH'
            AND CALDT = (SELECT MAX(CALDT) FROM positions WHERE fund = '{fund}')
        ORDER BY "Symbol";
        '''

        df = self.db.execute_query(query_string)

        return df['Symbol'].tolist()

    def get_dividends(self, fund, ticker, start, end):
        query_string = f'''
        SELECT
            fund,
            CALDT,
            "Symbol" AS ticker,
            AVG("GrossRate"::DECIMAL) AS gross_rate,
            AVG("GrossAmount"::DECIMAL) AS gross_amount
        FROM dividends
        WHERE "Symbol" = '{ticker}'
            AND fund = '{fund}'
            AND CALDT BETWEEN '{start}' AND '{end}'
        GROUP BY fund, CALDT, "Symbol"
        ORDER BY CALDT;
        '''

        df = self.db.execute_query(query_string)

        return df

    def get_trades(self, fund, ticker, start, end):
        query_string = f'''
        SELECT
            CALDT,
            fund,
            "Symbol" AS ticker,
            SUM("Quantity"::DECIMAL) AS shares,
            AVG("TradePrice"::DECIMAL) AS price,
            SUM("TradeMoney"::DECIMAL) AS value,
            "Buy/Sell" AS side
        FROM trades
        WHERE fund = '{fund}'
            AND "Symbol" = '{ticker}'
            AND CALDT BETWEEN '{start}' AND '{end}'
        GROUP BY CALDT, fund, "Symbol", "Buy/Sell"
        ;
        '''

        df = self.db.execute_query(query_string)

        return df
    
    def get_cron_log(self) -> pd.DataFrame:
        query_string = f'''
        SELECT * FROM "ETL_Cron_Log"
        ORDER BY CALDT DESC
        LIMIT 8
        ;
        '''

        df = self.db.execute_query(query_string)

        return df
    
    def get_portfolio_defaults(self, fund) -> pd.DataFrame:
        query_string = f'''
            SELECT * FROM portfolio WHERE fund = '{fund}';
        '''
        df = self.db.execute_query(query_string)
        return df
    
    def upsert_portfolio(self, fund, bmk_return, target_te) -> None:
        query_string = f'''
            INSERT INTO portfolio
            (FUND, BENCHMARK_RETURN, TARGET_TRACKING_ERROR)
            VALUES
            ('{fund}',{bmk_return},{target_te})
            ON CONFLICT (FUND)
            DO UPDATE SET
                BENCHMARK_RETURN = EXCLUDED.BENCHMARK_RETURN,
                TARGET_TRACKING_ERROR = EXCLUDED.TARGET_TRACKING_ERROR;
        '''
        self.db.execute_sql(query_string)

    def get_all_holdings(self, fund):
        query_string = f'''
        WITH
        positions_query AS(
            SELECT
                CALDT,
                fund,
                "Symbol" AS ticker,
                "Quantity"::DECIMAL AS shares,
                "MarkPrice"::DECIMAL AS price,
                "PositionValue"::DECIMAL AS value
            FROM positions
            WHERE CALDT = (SELECT MAX(CALDT) FROM positions WHERE fund = '{fund}')
                AND fund = '{fund}'
        ),
        nav_query AS (
            SELECT
                CALDT,
                fund,
                "Stock"::DECIMAL AS total_stock
            FROM nav
            WHERE fund = '{fund}'
        ),
        positions_xf AS(
            SELECT
                p.CALDT,
                p.fund,
                p.ticker,
                p.shares,
                p.price,
                p.value,
                p.value / n.total_stock AS weight
            FROM positions_query p
            JOIN nav_query n ON n.CALDT = p.CALDT
        )
        SELECT
            p.fund,
            p.ticker,
            p.shares,
            p.price,
            p.value,
            COALESCE(h.horizon_date, NULL) AS horizon_date,
            COALESCE(h.target_price, NULL) AS target_price,
            p.weight
        FROM positions_xf p
        FULL OUTER JOIN holding h ON h.ticker = p.ticker AND h.fund = p.fund
        WHERE p.fund = '{fund}';
        '''

        df = self.db.execute_query(query_string)

        return df
    
    def upsert_holding(self, fund, ticker, horizon, target) -> None:
        query_string = f'''
            INSERT INTO holding (FUND, TICKER, HORIZON_DATE, TARGET_PRICE)
            VALUES ('{fund}','{ticker}','{horizon}',{target})
            ON CONFLICT (FUND, TICKER)
            DO UPDATE SET
                HORIZON_DATE = EXCLUDED.HORIZON_DATE,
                TARGET_PRICE = EXCLUDED.TARGET_PRICE;
        '''
        self.db.execute_sql(query_string)



