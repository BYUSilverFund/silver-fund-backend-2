import numpy as np
import pandas as pd
from shared.database import Database

class Query:
    def __init__(self):
        self.db = Database()

    def get_fund_df(self, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH
                FUND_QUERY AS(
                    SELECT
                        CALDT,
                        SUM(STARTING_VALUE) AS STARTING_VALUE,
                        SUM(ENDING_VALUE) AS ENDING_VALUE
                    FROM DELTA_NAV
                    GROUP BY CALDT
                    ORDER BY CALDT
                ),
                FUND_XF AS(
                    SELECT
                        CALDT,
                        STARTING_VALUE,
                        ENDING_VALUE,
                        ENDING_VALUE / STARTING_VALUE - 1 AS RETURN
                    FROM FUND_QUERY
                    WHERE STARTING_VALUE <> 0
                ),
                DIVIDENDS_QUERY AS(
                    SELECT
                        CALDT,
                        FUND,
                        TICKER,
                        AVG(GROSS_AMOUNT) AS DIV_GROSS_AMOUNT
                    FROM DIVIDENDS
                    GROUP BY CALDT, FUND, TICKER
                ),
                DIVIDENDS_XF AS(
                    SELECT
                        CALDT,
                        SUM(DIV_GROSS_AMOUNT) AS DIVIDENDS
                    FROM DIVIDENDS_QUERY
                    GROUP BY CALDT
                ),
                JOIN_TABLE AS(
                    SELECT
                        F.CALDT,
                        F.STARTING_VALUE,
                        F.ENDING_VALUE,
                        COALESCE(D.DIVIDENDS, 0) AS DIVIDENDS,
                        F.RETURN,
                        COALESCE(R.RETURN, LAG(R.RETURN) OVER (ORDER BY R.CALDT)) AS RF_RETURN,
                        F.RETURN - COALESCE(R.RETURN, LAG(R.RETURN) OVER (ORDER BY R.CALDT)) AS XS_RETURN
                    FROM FUND_XF F
                    LEFT JOIN RISK_FREE_RATE R ON F.CALDT = R.CALDT
                    LEFT JOIN DIVIDENDS_XF D ON F.CALDT = D.CALDT
                    WHERE F.RETURN <> 0
                        AND F.CALDT BETWEEN '{start_date}' AND '{end_date}'
                    ORDER BY CALDT
                )
            SELECT * FROM JOIN_TABLE;
        '''

        df = self.db.get_dataframe(query_string)

        return df

    def get_portfolio_df(self, fund: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH
                PORT_QUERY AS(
                    SELECT 
                         CALDT,
                         FUND,
                         "STARTINGVALUE"::DECIMAL AS STARTING_VALUE,
                         "ENDINGVALUE"::DECIMAL - "DEPOSITSWITHDRAWALS"::DECIMAL AS ENDING_VALUE
                    FROM DELTA_NAV
                    WHERE FUND = '{fund}'
                ),
                PORT_XF AS(
                    SELECT
                        P.CALDT,
                        P.FUND,
                        STARTING_VALUE,
                        ENDING_VALUE,
                        ENDING_VALUE / STARTING_VALUE - 1 AS RETURN
                    FROM PORT_QUERY P
                ),
                DIVIDENDS_QUERY AS(
                    SELECT
                        CALDT,
                        FUND,
                        "SYMBOL" AS TICKER,
                        AVG("GROSSAMOUNT"::DECIMAL) AS DIV_GROSS_AMOUNT
                    FROM DIVIDENDS
                    WHERE FUND = '{fund}'
                    GROUP BY CALDT, FUND, "SYMBOL"
                ),
                DIVIDENDS_XF AS(
                    SELECT
                        CALDT,
                        FUND,
                        SUM(DIV_GROSS_AMOUNT) AS DIVIDENDS
                    FROM DIVIDENDS_QUERY
                    GROUP BY CALDT, FUND
                ),
                JOIN_TABLE AS (
                    SELECT 
                         P.CALDT,
                         P.FUND,
                         P.STARTING_VALUE,
                         P.ENDING_VALUE,
                         COALESCE(D.DIVIDENDS, 0) AS DIVIDENDS,
                         P.RETURN,
                        COALESCE(R.RETURN, LAG(R.RETURN) OVER (ORDER BY R.CALDT)) AS RF_RETURN,
                        P.RETURN - COALESCE(R.RETURN, LAG(R.RETURN) OVER (ORDER BY R.CALDT)) AS XS_RETURN
                    FROM PORT_XF P
                    LEFT JOIN RISK_FREE_RATE R ON P.CALDT = R.CALDT
                    LEFT JOIN DIVIDENDS_XF D ON P.CALDT = D.CALDT
                    WHERE STARTING_VALUE <> 0
                        AND P.RETURN <> 0
                        AND ENDING_VALUE / STARTING_VALUE - 1 <> 0
                        AND P.CALDT BETWEEN '{start_date}' AND '{end_date}'
                )
            SELECT * FROM JOIN_TABLE
        '''

        df = self.db.get_dataframe(query_string)

        return df

    def get_holding_df(self, fund: str, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        query_string = f'''
            WITH
                POSITIONS_QUERY AS (
                    SELECT
                        CALDT,
                        FUND,
                        "SYMBOL" AS TICKER,
                        CASE WHEN AVG("QUANTITY"::DECIMAL) > 0 THEN 1 ELSE -1 END AS SIDE,
                        AVG("QUANTITY"::DECIMAL) AS SHARES_1,
                        AVG("MARKPRICE"::DECIMAL) AS PRICE_1,
                        AVG("FXRATETOBASE":: DECIMAL) AS FX_RATE_1
                    FROM POSITIONS
                    WHERE FUND = '{fund}'
                        AND "SYMBOL" = '{ticker}'
                        AND "ASSETCLASS" != 'OPT'
                    GROUP BY CALDT, FUND, "SYMBOL"
                    ORDER BY CALDT
                ),
                DIVIDENDS_QUERY AS (
                    SELECT
                        CALDT,
                        FUND,
                        "SYMBOL" AS TICKER,
                        AVG("GROSSRATE"::DECIMAL) AS DIV_GROSS_RATE,
                        AVG("GROSSAMOUNT"::DECIMAL) AS DIV_GROSS_AMOUNT -- SOMETIMES DIVIDENDS GETS DOUBLE COUNTED
                    FROM DIVIDENDS
                    WHERE FUND = '{fund}'
                        AND "SYMBOL" = '{ticker}'
                    GROUP BY CALDT, FUND, "SYMBOL"
                ),
                TRADES_QUERY AS (
                    SELECT
                        CALDT,
                        FUND,
                        "SYMBOL" AS TICKER,
                        CASE WHEN SUM("QUANTITY"::DECIMAL) > 0 THEN 1 ELSE -1 END AS TRADE_TYPE,
                        SUM("QUANTITY"::DECIMAL) AS SHARES_TRADED,
                        AVG("TRADEPRICE"::DECIMAL) AS TRADE_PRICE
                     FROM TRADES
                     WHERE FUND = '{fund}'
                       AND "SYMBOL" = '{ticker}'
                       AND "ASSETCLASS" != 'OPT'
                       AND "ASSETCLASS" != 'CASH'
                       AND "BUY/SELL" != 'SELL (CA.)'
                     GROUP BY CALDT, FUND, "SYMBOL"
                ),
                TRADES_XF AS(
                    SELECT
                        CALDT,
                        FUND,
                        TICKER,
                        TRADE_TYPE,
                        SHARES_TRADED,
                        TRADE_PRICE
                    FROM TRADES_QUERY
                    WHERE SHARES_TRADED <> 0
                ),
                NAV_QUERY AS(
                    SELECT
                        CALDT,
                        FUND,
                        "STOCK"::DECIMAL AS TOTAL_STOCK_1
                    FROM NAV
                    WHERE FUND = '{fund}'
                        AND "STOCK"::DECIMAL <> 0
                ),
                JOIN_TABLE_1 AS( -- MERGE TRADES AND DIVIDENDS INTO POSITIONS
                    SELECT
                           COALESCE(P.CALDT, T.CALDT) AS CALDT,
                           COALESCE(P.FUND, T.FUND) AS FUND,
                           COALESCE(P.TICKER, T.TICKER) AS TICKER,
                           P.SIDE,
                           P.SHARES_1,
                           P.PRICE_1,
                           P.FX_RATE_1,
                           T.TRADE_TYPE,
                           COALESCE(T.SHARES_TRADED, 0) AS SHARES_TRADED,
                           T.TRADE_PRICE,
                           COALESCE(D.DIV_GROSS_RATE,0) AS DIV_GROSS_RATE,
                           COALESCE(D.DIV_GROSS_AMOUNT, 0) AS DIVIDENDS
                    FROM POSITIONS_QUERY P
                    FULL JOIN TRADES_XF T ON P.CALDT = T.CALDT AND P.TICKER = T.TICKER AND P.FUND = T.FUND
                    LEFT JOIN DIVIDENDS_QUERY D ON P.CALDT = D.CALDT AND P.TICKER = D.TICKER AND P.FUND = D.FUND
                    ORDER BY COALESCE(P.CALDT, T.CALDT)
                ),
                JOIN_TABLE_2 AS( -- COALESCE AND LAG MISSING VALUES
                    SELECT
                           CALDT,
                           FUND,
                           TICKER,
                           COALESCE(LAG(SIDE) OVER (PARTITION BY TICKER ORDER BY CALDT), SIDE) AS SIDE,
                           COALESCE(LAG(SHARES_1) OVER (PARTITION BY TICKER ORDER BY CALDT), SHARES_TRADED) AS SHARES_0,
                           COALESCE(CASE WHEN SHARES_1 - SHARES_TRADED = 0 THEN SHARES_1 ELSE SHARES_1 - SHARES_TRADED END , SHARES_TRADED * -1) AS SHARES_1,
                           COALESCE(LAG(PRICE_1) OVER (PARTITION BY TICKER ORDER BY CALDT), TRADE_PRICE) AS PRICE_0,
                           COALESCE(PRICE_1, TRADE_PRICE) AS PRICE_1,
                           COALESCE(LAG(FX_RATE_1) OVER (PARTITION BY TICKER ORDER BY CALDT), FX_RATE_1) AS FX_RATE_0,
                           COALESCE(FX_RATE_1, LAG(FX_RATE_1) OVER (PARTITION BY TICKER ORDER BY CALDT)) AS FX_RATE_1,
                           TRADE_TYPE,
                           SHARES_TRADED,
                           TRADE_PRICE,
                           DIV_GROSS_RATE,
                           DIVIDENDS
                    FROM JOIN_TABLE_1
                ),
                JOIN_TABLE_3 AS(
                    SELECT
                           CALDT,
                           FUND,
                           TICKER,
                           SIDE,
                           SHARES_0,
                           SHARES_1,
                           PRICE_0,
                           PRICE_1,
                           FX_RATE_0,
                           FX_RATE_1,
                           SHARES_0 * PRICE_0 * FX_RATE_0 AS VALUE_0,
                           SHARES_1 * PRICE_1 * FX_RATE_1 AS VALUE_1,
                           TRADE_TYPE,
                           SHARES_TRADED,
                           TRADE_PRICE,
                           DIV_GROSS_RATE,
                           DIVIDENDS
                    FROM JOIN_TABLE_2
                ),
                JOIN_TABLE_4 AS( -- COMPUTE WEIGHTS AND RETURNS
                    SELECT
                        P.CALDT,
                        P.FUND,
                        TICKER,
                        P.SIDE,
                        SHARES_1,
                        PRICE_1,
                        FX_RATE_1,
                        VALUE_1,
                        N.TOTAL_STOCK_1,
                        VALUE_1 / N.TOTAL_STOCK_1 AS WEIGHT_1,
                        DIV_GROSS_RATE,
                        DIVIDENDS,
                        CASE WHEN SIDE = 1 THEN (VALUE_1 / VALUE_0 - 1) ELSE (VALUE_0 / VALUE_1 - 1) END AS RETURN,
                        CASE WHEN SIDE = 1 THEN ((VALUE_1 + DIVIDENDS) / VALUE_0 - 1) ELSE (VALUE_0 / (VALUE_1 + DIVIDENDS) - 1) END AS DIV_RETURN
                    FROM JOIN_TABLE_3 P
                    LEFT JOIN NAV_QUERY N ON P.CALDT = N.CALDT AND P.FUND = N.FUND
                ),
                JOIN_TABLE_5 AS( -- COMPUTE EXCESS RETURNS
                    SELECT
                       A.CALDT,
                       A.FUND,
                       A.TICKER,
                       A.SIDE,
                       A.PRICE_1 AS PRICE,
                       A.SHARES_1 AS SHARES,
                       A.FX_RATE_1 AS FX_RATE,
                       A.VALUE_1 AS VALUE,
                       A.WEIGHT_1 AS WEIGHT,
                       A.DIV_GROSS_RATE / A.PRICE_1 AS DIVIDEND_YIELD,
                       A.DIVIDENDS,
                       A.RETURN,
                       A.DIV_RETURN,
                       COALESCE(C.RETURN, LAG(C.RETURN) OVER (ORDER BY C.CALDT)) AS RF_RETURN,
                       A.RETURN - COALESCE(C.RETURN, LAG(C.RETURN) OVER (ORDER BY C.CALDT)) AS XS_RETURN,
                       A.DIV_RETURN - COALESCE(C.RETURN, LAG(C.RETURN) OVER (ORDER BY C.CALDT)) AS XS_DIV_RETURN
                    FROM JOIN_TABLE_4 A
                    LEFT JOIN RISK_FREE_RATE C ON A.CALDT = C.CALDT
                    INNER JOIN CALENDAR D ON A.CALDT = D.CALDT -- REMOVE HOLIDAYS
                    WHERE A.CALDT BETWEEN '{start_date}' AND '{end_date}'
                )
            SELECT * FROM JOIN_TABLE_5;
        '''

        df = self.db.get_dataframe(query_string)

        return df

    def get_all_holdings_df(self, fund: str, start_date: str, end_date: str):
        query_string = f'''
            WITH
                POSITIONS_QUERY AS (
                    SELECT
                        CALDT,
                        FUND,
                        "SYMBOL" AS TICKER,
                        CASE WHEN AVG("QUANTITY"::DECIMAL) > 0 THEN 1 ELSE -1 END AS SIDE,
                        AVG("QUANTITY"::DECIMAL) AS SHARES_1,
                        AVG("MARKPRICE"::DECIMAL) AS PRICE_1,
                        AVG("FXRATETOBASE":: DECIMAL) AS FX_RATE_1
                    FROM POSITIONS
                    WHERE FUND = '{fund}'
                        AND "ASSETCLASS" != 'OPT'
                        AND "SYMBOL" != 'VMFXX'
                    GROUP BY CALDT, FUND, "SYMBOL"
                    ORDER BY TICKER, CALDT
                ),
                DIVIDENDS_QUERY AS (
                    SELECT
                        CALDT,
                        FUND,
                        "SYMBOL" AS TICKER,
                        AVG("GROSSRATE"::DECIMAL) AS DIV_GROSS_RATE,
                        AVG("GROSSAMOUNT"::DECIMAL) AS DIV_GROSS_AMOUNT -- SOMETIMES DIVIDENDS GETS DOUBLE COUNTED
                    FROM DIVIDENDS
                    WHERE FUND = '{fund}'
                        AND "SYMBOL" != 'VMFXX'
                    GROUP BY CALDT, FUND, "SYMBOL"
                ),
                TRADES_QUERY AS(
                    SELECT
                        CALDT,
                        FUND,
                        "SYMBOL" AS TICKER,
                        CASE WHEN SUM("QUANTITY"::DECIMAL) > 0 THEN 1 ELSE -1 END AS TRADE_TYPE,
                        SUM("QUANTITY"::DECIMAL) AS SHARES_TRADED,
                        AVG("TRADEPRICE"::DECIMAL) AS TRADE_PRICE
                    FROM TRADES
                    WHERE FUND = '{fund}'
                        AND "ASSETCLASS" != 'OPT'
                        AND "ASSETCLASS" != 'CASH'
                        AND "BUY/SELL" != 'SELL (CA.)'
                        AND "SYMBOL" != 'VMFXX'
                    GROUP BY CALDT, FUND, "SYMBOL"
                ),
                TRADES_XF AS(
                    SELECT
                        CALDT,
                        FUND,
                        TICKER,
                        TRADE_TYPE,
                        SHARES_TRADED,
                        TRADE_PRICE
                    FROM TRADES_QUERY
                    WHERE SHARES_TRADED <> 0
                ),
                NAV_QUERY AS(
                    SELECT
                        CALDT,
                        FUND,
                        "STOCK"::DECIMAL AS TOTAL_STOCK_1
                    FROM NAV
                    WHERE FUND = '{fund}'
                        AND "STOCK"::DECIMAL <> 0
                ),
                JOIN_TABLE_1 AS( -- MERGE TRADES AND DIVIDENDS INTO POSITIONS
                    SELECT
                        COALESCE(P.CALDT, T.CALDT) AS CALDT,
                        COALESCE(P.FUND, T.FUND) AS FUND,
                        COALESCE(P.TICKER, T.TICKER) AS TICKER,
                        P.SIDE,
                        P.SHARES_1,
                        P.PRICE_1,
                        P.FX_RATE_1,
                        T.TRADE_TYPE,
                        COALESCE(T.SHARES_TRADED, 0) AS SHARES_TRADED,
                        T.TRADE_PRICE,
                        COALESCE(D.DIV_GROSS_RATE,0) AS DIV_GROSS_RATE,
                        COALESCE(D.DIV_GROSS_AMOUNT, 0) AS DIVIDENDS
                    FROM POSITIONS_QUERY P
                    FULL JOIN TRADES_XF T ON P.CALDT = T.CALDT AND P.TICKER = T.TICKER AND P.FUND = T.FUND
                    LEFT JOIN DIVIDENDS_QUERY D ON P.CALDT = D.CALDT AND P.TICKER = D.TICKER AND P.FUND = D.FUND
                    ORDER BY TICKER, COALESCE(P.CALDT, T.CALDT)
                ),
                JOIN_TABLE_2 AS( -- COALESCE AND LAG MISSING VALUES
                    SELECT
                        CALDT,
                        FUND,
                        TICKER,
                        COALESCE(LAG(SIDE) OVER (PARTITION BY TICKER ORDER BY CALDT), SIDE) AS SIDE,
                        COALESCE(LAG(SHARES_1) OVER (PARTITION BY TICKER ORDER BY CALDT), SHARES_TRADED) AS SHARES_0,
                        COALESCE(CASE WHEN SHARES_1 - SHARES_TRADED = 0 THEN SHARES_1 ELSE SHARES_1 - SHARES_TRADED END , SHARES_TRADED * -1) AS SHARES_1,
                        COALESCE(LAG(PRICE_1) OVER (PARTITION BY TICKER ORDER BY CALDT), TRADE_PRICE) AS PRICE_0,
                        COALESCE(PRICE_1, TRADE_PRICE) AS PRICE_1,
                        COALESCE(LAG(FX_RATE_1) OVER (PARTITION BY TICKER ORDER BY CALDT), FX_RATE_1) AS FX_RATE_0,
                        COALESCE(FX_RATE_1, LAG(FX_RATE_1) OVER (PARTITION BY TICKER ORDER BY CALDT)) AS FX_RATE_1,
                        TRADE_TYPE,
                        SHARES_TRADED,
                        TRADE_PRICE,
                        DIV_GROSS_RATE,
                        DIVIDENDS
                    FROM JOIN_TABLE_1
                ),
                JOIN_TABLE_3 AS(
                    SELECT
                        CALDT,
                        FUND,
                        TICKER,
                        SIDE,
                        SHARES_0,
                        SHARES_1,
                        PRICE_0,
                        PRICE_1,
                        FX_RATE_0,
                        FX_RATE_1,
                        SHARES_0 * PRICE_0 * FX_RATE_0 AS VALUE_0,
                        SHARES_1 * PRICE_1 * FX_RATE_1 AS VALUE_1,
                        TRADE_TYPE,
                        SHARES_TRADED,
                        TRADE_PRICE,
                        DIV_GROSS_RATE,
                        DIVIDENDS
                    FROM JOIN_TABLE_2
                ),
                JOIN_TABLE_4 AS( -- COMPUTE WEIGHTS AND RETURNS
                    SELECT
                        P.CALDT,
                        P.FUND,
                        TICKER,
                        P.SIDE,
                        SHARES_1,
                        PRICE_1,
                        FX_RATE_1,
                        VALUE_1,
                        N.TOTAL_STOCK_1,
                        VALUE_1 / N.TOTAL_STOCK_1 AS WEIGHT_1,
                        DIV_GROSS_RATE,
                        DIVIDENDS,
                        CASE WHEN SIDE = 1 THEN (VALUE_1 / VALUE_0 - 1) ELSE (VALUE_0 / VALUE_1 - 1) END AS RETURN,
                        CASE WHEN SIDE = 1 THEN ((VALUE_1 + DIVIDENDS) / VALUE_0 - 1) ELSE (VALUE_0 / (VALUE_1 + DIVIDENDS) - 1) END AS DIV_RETURN
                    FROM JOIN_TABLE_3 P
                    LEFT JOIN NAV_QUERY N ON P.CALDT = N.CALDT AND P.FUND = N.FUND
                    ORDER BY TICKER, P.CALDT
                ),
                JOIN_TABLE_5 AS( -- COMPUTE EXCESS RETURNS
                    SELECT
                        A.CALDT,
                        A.FUND,
                        A.TICKER,
                        A.SIDE,
                        A.PRICE_1 AS PRICE,
                        A.SHARES_1 AS SHARES,
                        A.FX_RATE_1 AS FX_RATE,
                        A.VALUE_1 AS VALUE,
                        A.WEIGHT_1 AS WEIGHT,
                        A.DIV_GROSS_RATE / A.PRICE_1 AS DIVIDEND_YIELD,
                        A.DIVIDENDS,
                        A.RETURN,
                        A.DIV_RETURN,
                        COALESCE(C.RETURN, LAG(C.RETURN) OVER (PARTITION BY A.TICKER ORDER BY C.CALDT)) AS RF_RETURN,
                        A.RETURN - COALESCE(C.RETURN, LAG(C.RETURN) OVER (PARTITION BY A.TICKER ORDER BY C.CALDT)) AS XS_RETURN,
                        A.DIV_RETURN - COALESCE(C.RETURN, LAG(C.RETURN) OVER (PARTITION BY A.TICKER ORDER BY C.CALDT)) AS XS_DIV_RETURN
                    FROM JOIN_TABLE_4 A
                    LEFT JOIN RISK_FREE_RATE C ON A.CALDT = C.CALDT
                    INNER JOIN CALENDAR D ON A.CALDT = D.CALDT -- REMOVE HOLIDAYS
                    WHERE A.CALDT BETWEEN '{start_date}' AND '{end_date}'
                    ORDER BY TICKER, A.CALDT
                )
                SELECT * FROM JOIN_TABLE_5;
        '''

        df = self.db.get_dataframe(query_string)

        return df

    def get_benchmark_df(self, start_date: str, end_date: str):
        query_string = f"""
            WITH
            BMK_QUERY AS(
                SELECT
                    CALDT,
                    LAG(ENDING_VALUE) OVER (ORDER BY CALDT) AS STARTING_VALUE,
                    ENDING_VALUE
                FROM BENCHMARK
            ),
            DIVIDEND_QUERY AS(
                SELECT
                    CALDT,
                    AVG("GROSSRATE"::DECIMAL) AS DIV_GROSS_RATE
                FROM DIVIDENDS
                WHERE FUND = 'UNDERGRAD' AND "SYMBOL" = 'IWV'
                GROUP BY CALDT
            ),
            BMK_XF AS(
                SELECT
                    B.CALDT,
                    B.STARTING_VALUE,
                    B.ENDING_VALUE,
                    D.DIV_GROSS_RATE AS DIVIDENDS,
                    D.DIV_GROSS_RATE / B.ENDING_VALUE AS DIVIDEND_YIELD,
                    B.ENDING_VALUE / B.STARTING_VALUE - 1 AS RETURN,
                    (B.ENDING_VALUE + COALESCE(D.DIV_GROSS_RATE,0)) / B.STARTING_VALUE - 1 AS DIV_RETURN
                FROM BMK_QUERY B
                LEFT JOIN DIVIDEND_QUERY D ON D.CALDT = B.CALDT
            ),
            JOIN_TABLE AS(
                SELECT
                    B.CALDT,
                    B.STARTING_VALUE,
                    B.ENDING_VALUE,
                    B.DIVIDENDS,
                    B.DIVIDEND_YIELD,
                    B.RETURN,
                    B.DIV_RETURN,
                    COALESCE(R.RETURN, LAG(R.RETURN) OVER (ORDER BY R.CALDT)) AS RF_RETURN,
                    B.RETURN - COALESCE(R.RETURN, LAG(R.RETURN) OVER (ORDER BY R.CALDT)) AS XS_RETURN,
                    B.DIV_RETURN - COALESCE(R.RETURN, LAG(R.RETURN) OVER (ORDER BY R.CALDT)) AS XS_DIV_RETURN
                FROM BMK_XF B
                LEFT JOIN RISK_FREE_RATE R ON R.CALDT = B.CALDT
                WHERE B.ENDING_VALUE / B.STARTING_VALUE <> 1
                    AND B.CALDT BETWEEN '{start_date}' AND '{end_date}'
            )
            SELECT * FROM JOIN_TABLE;
        """

        df = self.db.get_dataframe(query_string)

        return df

    def get_tickers(self, fund, start_date, end_date) -> np.ndarray:
        query_string = f'''
            SELECT "SYMBOL"
            FROM POSITIONS
            WHERE FUND = '{fund}'
                AND "ASSETCLASS" != 'OPT'
                AND "ASSETCLASS" != 'CASH'
                AND CALDT BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY "SYMBOL"
            ORDER BY "SYMBOL";
        '''

        df = self.db.get_dataframe(query_string)

        return df['Symbol'].tolist()

    def get_current_tickers(self, fund):
        query_string = f'''
        SELECT "SYMBOL"
        FROM POSITIONS
        WHERE FUND = '{fund}'
            AND "ASSETCLASS" != 'OPT'
            AND "ASSETCLASS" != 'CASH'
            AND CALDT = (SELECT MAX(CALDT) FROM POSITIONS WHERE FUND = '{fund}')
        ORDER BY "SYMBOL";
        '''

        df = self.db.get_dataframe(query_string)

        return df['Symbol'].tolist()

    def get_dividends(self, fund, ticker, start_date, end_date):
        query_string = f'''
        SELECT
            FUND,
            CALDT,
            "SYMBOL" AS TICKER,
            AVG("GROSSRATE"::DECIMAL) AS GROSS_RATE,
            AVG("GROSSAMOUNT"::DECIMAL) AS GROSS_AMOUNT
        FROM DIVIDENDS
        WHERE "SYMBOL" = '{ticker}'
            AND FUND = '{fund}'
            AND CALDT BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY FUND, CALDT, "SYMBOL"
        ORDER BY CALDT;
        '''

        df = self.db.get_dataframe(query_string)

        return df

    def get_trades(self, fund, ticker, start_date, end_date):
        query_string = f'''
        SELECT
            CALDT,
            FUND,
            "SYMBOL" AS TICKER,
            SUM("QUANTITY"::DECIMAL) AS SHARES,
            AVG("TRADEPRICE"::DECIMAL) AS PRICE,
            SUM("TRADEMONEY"::DECIMAL) AS VALUE,
            "BUY/SELL" AS SIDE
        FROM TRADES
        WHERE FUND = '{fund}'
            AND "SYMBOL" = '{ticker}'
            AND CALDT BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY CALDT, FUND, "SYMBOL", "BUY/SELL"
        ;
        '''

        df = self.db.get_dataframe(query_string)

        return df
    
    def get_cron_log(self) -> pd.DataFrame:
        query_string = f'''
        SELECT * FROM "ETL_CRON_LOG"
        ORDER BY CALDT DESC
        LIMIT 8
        ;
        '''

        df = self.db.get_dataframe(query_string)

        return df
    
    def get_portfolio_defaults(self, fund) -> pd.DataFrame:
        query_string = f'''
            SELECT * FROM PORTFOLIO WHERE FUND = '{fund}';
        '''
        df = self.db.get_dataframe(query_string)
        return df
    
    def upsert_portfolio(self, fund, bmk_return, target_te) -> None:
        query_string = f'''
            INSERT INTO PORTFOLIO
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
        POSITIONS_QUERY AS(
            SELECT
                CALDT,
                FUND,
                "SYMBOL" AS TICKER,
                "QUANTITY"::DECIMAL AS SHARES,
                "MARKPRICE"::DECIMAL AS PRICE,
                "POSITIONVALUE"::DECIMAL AS VALUE
            FROM POSITIONS
            WHERE CALDT = (SELECT MAX(CALDT) FROM POSITIONS WHERE FUND = '{fund}')
                AND FUND = '{fund}'
        ),
        NAV_QUERY AS (
            SELECT
                CALDT,
                FUND,
                "STOCK"::DECIMAL AS TOTAL_STOCK
            FROM NAV
            WHERE FUND = '{fund}'
        ),
        POSITIONS_XF AS(
            SELECT
                P.CALDT,
                P.FUND,
                P.TICKER,
                P.SHARES,
                P.PRICE,
                P.VALUE,
                P.VALUE / N.TOTAL_STOCK AS WEIGHT
            FROM POSITIONS_QUERY P
            JOIN NAV_QUERY N ON N.CALDT = P.CALDT
        )
        SELECT
            P.FUND,
            P.TICKER,
            P.SHARES,
            P.PRICE,
            P.VALUE,
            COALESCE(H.HORIZON_DATE, NULL) AS HORIZON_DATE,
            COALESCE(H.TARGET_PRICE, NULL) AS TARGET_PRICE,
            P.WEIGHT
        FROM POSITIONS_XF P
        FULL OUTER JOIN HOLDING H ON H.TICKER = P.TICKER AND H.FUND = P.FUND
        WHERE P.FUND = '{fund}';
        '''

        df = self.db.get_dataframe(query_string)

        return df
    
    def upsert_holding(self, fund, ticker, horizon, target) -> None:
        query_string = f'''
            INSERT INTO HOLDING (FUND, TICKER, HORIZON_DATE, TARGET_PRICE)
            VALUES ('{fund}','{ticker}','{horizon}',{target})
            ON CONFLICT (FUND, TICKER)
            DO UPDATE SET
                HORIZON_DATE = EXCLUDED.HORIZON_DATE,
                TARGET_PRICE = EXCLUDED.TARGET_PRICE;
        '''
        self.db.execute_sql(query_string)



