import json
import pandas as pd
from server.query import Query
from server.functions import *


class Service:

    def __init__(self):
        self.query = Query()

    def fund_summary(self, start_date: str, end_date: str) -> json:
        df = self.query.get_fund_df(start_date, end_date)
        bmk = self.query.get_benchmark_df(start_date, end_date)

        with pd.option_context('future.no_silent_downcasting', True):  # This just prevents a future warning from printing
            left = pd.merge(left=df, right=bmk, how='left', on='caldt', suffixes=('_port', '_bmk')).fillna(0)

        true_start = str(df['caldt'].iloc[0]).split(" ")[0]
        true_end = str(df['caldt'].iloc[-1]).split(" ")[0]
        fund_value = df['ending_value'].iloc[-1]
        fund_return = total_return(df['return'], annualized=False)
        fund_volatility = volatility((df['return']))
        fund_sharpe_ratio = sharpe_ratio(df['return'], df['rf_return'])
        fund_dividends = df['dividends'].sum()

        fund_alpha = alpha(left['xs_return_port'], left['xs_div_return'], annualized=False)
        fund_beta = beta(left['xs_return_port'], left['xs_div_return'])
        fund_information_ratio = information_ratio(left['return_port'], left['div_return'], left['rf_return_port'])
        fund_tracking_error = tracking_error(left['return_port'], left['div_return'])

        result = {
            "start": true_start,
            "end": true_end,
            "value": round(fund_value, 2),
            "total_return": round(fund_return * 100, 2),
            "dividends": round(fund_dividends, 2),
            "volatility": round(fund_volatility * 100, 2),
            "sharpe_ratio": round(fund_sharpe_ratio, 2),
            "alpha": round(fund_alpha * 100, 2),
            "beta": round(fund_beta, 2),
            "information_ratio": round(fund_information_ratio, 2),
            "tracking_error": round(fund_tracking_error * 100, 2),
        }

        return json.dumps(result)

    def portfolio_summary(self, fund: str, start_date: str, end_date: str) -> json:
        df = self.query.get_portfolio_df(fund, start_date, end_date)
        bmk = self.query.get_benchmark_df(start_date, end_date)

        with pd.option_context('future.no_silent_downcasting', True):  # This just prevents a future warning from printing
            left = pd.merge(left=df, right=bmk, how='left', on='caldt', suffixes=('_port', '_bmk')).fillna(0)

        true_start = str(df['caldt'].iloc[0]).split(" ")[0]
        true_end = str(df['caldt'].iloc[-1]).split(" ")[0]
        port_value = df['ending_value'].iloc[-1]
        port_return = total_return(df['return'], annualized=False)
        port_volatility = volatility((df['return']))
        port_sharpe_ratio = sharpe_ratio(df['return'], df['rf_return'])
        port_dividends = df['dividends'].sum()

        port_alpha = alpha(left['xs_return_port'], left['xs_div_return'], annualized=False)
        port_beta = beta(left['xs_return_port'], left['xs_div_return'])
        port_information_ratio = information_ratio(left['return_port'], left['div_return'], left['rf_return_port'])
        port_tracking_error = tracking_error(left['return_port'], left['div_return'])

        result = {
            "start": true_start,
            "end": true_end,
            "fund": fund,
            "value": round(port_value, 2),
            "total_return": round(port_return * 100, 2),
            "dividends": round(port_dividends, 2),
            "volatility": round(port_volatility * 100, 2),
            "alpha": round(port_alpha * 100, 2),
            "beta": round(port_beta, 2),
            "sharpe_ratio": round(port_sharpe_ratio, 2),
            "information_ratio": round(port_information_ratio, 2),
            "tracking_error": round(port_tracking_error * 100, 2),
        }

        return json.dumps(result)

    def all_portfolios_summary(self, start_date: str, end_date: str) -> json:
        funds = ['undergrad', 'grad', 'brigham_capital', 'quant']

        result = []
        for fund in funds:
            result.append(
                self.portfolio_summary(fund, start_date, end_date)
            )
        return json.dumps(result)

    def holding_summary(self, fund: str, ticker: str, start_date: str, end_date: str) -> json:
        df = self.query.get_holding_df(fund, ticker, start_date, end_date)
        bmk = self.query.get_benchmark_df(start_date, end_date)
        current_tickers = self.query.get_current_tickers(fund)

        with pd.option_context('future.no_silent_downcasting', True):  # This just prevents a future warning from printing
            left = pd.merge(left=df, right=bmk, how='left', on='caldt', suffixes=('_port', '_bmk')).fillna(0)
            outer = pd.merge(left=df, right=bmk, how='outer', on='caldt', suffixes=('_port', '_bmk')).fillna(0)

        true_start = str(df['caldt'].iloc[0]).split(" ")[0]
        true_end = str(df['caldt'].iloc[-1]).split(" ")[0]
        shares = df['shares'].iloc[-1] if (ticker in current_tickers) else 0
        price = df['price'].iloc[-1]
        value = df['value'].iloc[-1]
        initial_weight = df['weight'].iloc[0]
        current_weight = df['weight'].iloc[-1] if (ticker in current_tickers) else 0

        holding_volatility = volatility((df['return']))
        holding_return = total_return(df['return'], annualized=False)
        holding_div_return = total_return(df['div_return'], annualized=False)
        holdings_dividends = df['dividends'].sum()
        holdings_dividend_yield = 0 if df['dividend_yield'].isna().all() else df['dividend_yield'].mean()

        holding_alpha = alpha(left['xs_div_return_port'], left['xs_div_return_bmk'], annualized=False)
        holding_beta = beta(left['xs_div_return_port'], left['xs_div_return_bmk'])

        holding_alpha_contribution = alpha_contribution(outer['xs_div_return_port'], outer['xs_div_return_bmk'], outer['weight'], annualized=False)

        result = {
            "start": true_start,
            "end": true_end,
            "ticker": ticker,
            "active": ticker in current_tickers,
            "shares": shares,
            "initial_weight": round(initial_weight, 4),
            "current_weight": round(current_weight, 4),
            "price": price,
            "value": round(value, 2),
            "volatility": round(holding_volatility * 100, 2),
            "total_return": round(holding_return * 100, 2),
            "total_div_return": round(holding_div_return * 100, 2),
            "dividends": round(holdings_dividends, 2),
            "dividend_yield": round(holdings_dividend_yield * 100, 2),
            "alpha": round(holding_alpha * 100, 2),
            "alpha_contribution": round(holding_alpha_contribution * 100, 2),
            "beta": round(holding_beta, 2)
        }

        return json.dumps(result)

    def all_holdings_summary(self, fund: str, start_date: str, end_date: str) -> json:
        df = Query().get_all_holdings_df(fund, start_date, end_date)
        bmk = Query().get_benchmark_df(start_date, end_date)
        current_tickers = Query().get_current_tickers(fund)

        results = pd.DataFrame()
        with pd.option_context('future.no_silent_downcasting', True):  # This just prevents a future warning from printing
            left = pd.merge(left=df, right=bmk, how='left', on='caldt', suffixes=('_port', '_bmk')).fillna(0)
            outer = pd.merge(left=df, right=bmk, how='outer', on='caldt', suffixes=('_port', '_bmk')).fillna(0)

        # DF metrics
        results['ticker'] = df['ticker'].unique()
        results['active'] = results['ticker'].isin(current_tickers)
        results['shares'] = df.groupby('ticker')['shares'].last().reset_index(drop=True)
        results['price'] = df.groupby('ticker')['price'].last().reset_index(drop=True)
        results['value'] = df.groupby('ticker')['value'].last().reset_index(drop=True)
        results['initial_weight'] = df.groupby('ticker')['weight'].first().reset_index(drop=True)
        results['current_weight'] = df.groupby('ticker')['weight'].last().reset_index(drop=True)

        # Adjust holdings that are inactive
        results['shares'] = np.where(results['active'], results['shares'], 0)
        results['value'] = np.where(results['active'], results['value'], 0)
        results['current_weight'] = np.where(results['active'], results['current_weight'], 0)

        # DF metrics
        results['volatility'] = df.groupby('ticker')['return'].apply(volatility).reset_index(drop=True)
        results['total_return'] = df.groupby('ticker')['return'].apply(total_return, annualized=False).reset_index(drop=True)
        results['total_div_return'] = df.groupby('ticker')['div_return'].apply(total_return,annualized=False).reset_index(drop=True)
        results['dividends'] = df.groupby('ticker')['dividends'].sum().reset_index(drop=True)
        results['dividend_yield'] = df.groupby('ticker')['dividend_yield'].mean().fillna(0).reset_index(drop=True)

        # LEFT metrics
        results['alpha'] = left.groupby('ticker').apply(
            lambda group: alpha(group['xs_div_return_port'], group['xs_div_return_bmk'], annualized=False),
            include_groups=False).reset_index(drop=True)
        results['beta'] = left.groupby('ticker').apply(
            lambda group: beta(group['xs_div_return_port'], group['xs_div_return_bmk']),
            include_groups=False).reset_index(drop=True)

        # OUTER Metrics
        results['alpha_contribution'] = outer.groupby('ticker').apply(
            lambda group: alpha_contribution(group['xs_div_return_port'], group['xs_div_return_bmk'], group['weight'], annualized=False),
            include_groups=False).reset_index(drop=True)

        # Formatting
        results['initial_weight'] = round(results['initial_weight'], 4)
        results['current_weight'] = round(results['current_weight'], 4)
        results['value'] = round(results['value'], 2)
        results['volatility'] = round(results['volatility'] * 100, 2)
        results['total_return'] = round(results['total_return'] * 100, 2)
        results['total_div_return'] = round(results['total_div_return'] * 100, 2)
        results['dividends'] = round(results['dividends'], 2)
        results['dividend_yield'] = round(results['dividend_yield'] * 100, 2)
        results['alpha'] = round(results['alpha'] * 100, 2)
        results['alpha_contribution'] = round(results['alpha_contribution'] * 100, 2)
        results['beta'] = round(results['beta'], 2)

        # print the count of nan values in the dataframe with the column name
        # print(results.isnull().sum())

        results['initial_weight'] = results['initial_weight'].fillna('null')
        results['volatility'] = results['volatility'].fillna('null')

        results = results.to_dict(orient='records')

        return json.dumps(results)

    def old_all_holdings_summary(self, fund, start_date, end_date):
        tickers = self.query.get_tickers(fund, start_date, end_date)

        result = []
        for ticker in tickers:
            res = self.holding_summary(fund, ticker, start_date, end_date)
            res = json.loads(res)
            result.append(res)

        return json.dumps(result)

    def benchmark_summary(self, start_date, end_date) -> json:
        df = self.query.get_benchmark_df(start_date, end_date)
        bmk = self.query.get_benchmark_df(start_date, end_date)

        left = pd.merge(left=df, right=bmk, how='left', on='caldt', suffixes=('_port', '_bmk')).fillna(0)

        bmk_volatility = volatility((df['return']))
        bmk_return = total_return(df['return'], annualized=False)
        bmk_div_return = total_return(df['div_return'], annualized=False)
        bmk_dividends = df['dividends'].sum()
        bmk_sharpe_ratio = sharpe_ratio(df['return'], df['rf_return'])
        bmk_dividend_yield = 0 if df['dividend_yield'].isna().all() else df['dividend_yield'].mean()

        bmk_alpha = alpha(left['xs_div_return_port'], left['xs_div_return_bmk'], annualized=False)
        bmk_beta = beta(left['xs_div_return_port'], left['xs_div_return_bmk'])

        result = {
            "volatility": round(bmk_volatility * 100, 2),
            "total_return": round(bmk_return * 100, 2),
            "total_div_return": round(bmk_div_return * 100, 2),
            "dividends": round(bmk_dividends, 2),
            "dividend_yield": round(bmk_dividend_yield * 100, 2),
            "alpha": round(bmk_alpha * 100, 2),
            "beta": round(bmk_beta, 2),
            "sharpe_ratio": round(bmk_sharpe_ratio, 2),
        }

        return json.dumps(result)

    def fund_chart_data(self, start_date: str, end_date: str) -> json:
        df = self.query.get_fund_df(start_date, end_date)
        bmk = self.query.get_benchmark_df(start_date, end_date)

        with pd.option_context('future.no_silent_downcasting', True):  # This just prevents a future warning from printing
            left = pd.merge(left=df, right=bmk, how='left', on='caldt', suffixes=('_port', '_bmk')).fillna(0)

        result = cumulative_return_vector(left, 'caldt', 'ending_value_port', 'return_port', 'div_return')

        day_zero = pd.DataFrame({
            'caldt': (pd.to_datetime(result.iloc[0,0]) - pd.Timedelta(days=1)).strftime('%Y-%m-%d'),
            'ending_value_port': result.iloc[0,1] / (1 + result.iloc[0,2] / 100),
            'cumulative_return_port': [0],
            'cumulative_return_bmk': [0]
        })

        result = pd.concat([day_zero, result]).reset_index(drop=True)

        result = result.rename(columns={'caldt': 'date'})

        result = result.to_dict(orient='records')

        return json.dumps(result)

    def portfolio_chart_data(self, fund: str, start_date: str, end_date: str) -> json:
        df = self.query.get_portfolio_df(fund, start_date, end_date)
        bmk = self.query.get_benchmark_df(start_date, end_date)

        with pd.option_context('future.no_silent_downcasting', True):  # This just prevents a future warning from printing
            left = pd.merge(left=df, right=bmk, how='left', on='caldt', suffixes=('_port', '_bmk')).fillna(0)

        result = cumulative_return_vector(left, 'caldt', 'ending_value_port', 'return_port', 'div_return')

        day_zero = pd.DataFrame({
            'caldt': (pd.to_datetime(result.iloc[0,0]) - pd.Timedelta(days=1)).strftime('%Y-%m-%d'),
            'ending_value_port': result.iloc[0,1] / (1 + result.iloc[0,2] / 100),
            'cumulative_return_port': [0],
            'cumulative_return_bmk': [0]
        })

        result = pd.concat([day_zero, result]).reset_index(drop=True)

        result = result.rename(columns={'caldt': 'date'})

        result = result.to_dict(orient='records')

        return json.dumps(result)

    def holding_chart_data(self, fund: str, ticker: str, start_date: str, end_date: str) -> json:

        df = self.query.get_holding_df(fund, ticker, start_date, end_date)
        bmk = self.query.get_benchmark_df(start_date, end_date)

        with pd.option_context('future.no_silent_downcasting', True):  # This just prevents a future warning from printing
            left = pd.merge(left=df, right=bmk, how='left', on='caldt', suffixes=('_port', '_bmk')).fillna(0)

        result = cumulative_return_vector(left, 'caldt', 'price', 'div_return_port', 'div_return_bmk')

        day_zero = pd.DataFrame({
            'caldt': (pd.to_datetime(result.iloc[0,0]) - pd.Timedelta(days=1)).strftime('%Y-%m-%d'),
            'price': result.iloc[0,1] / (1 + result.iloc[0,2] / 100),
            'cumulative_return_port': [0],
            'cumulative_return_bmk': [0]
        })

        result = pd.concat([day_zero, result]).reset_index(drop=True)

        result = result.rename(columns={'caldt': 'date'})

        result = result.to_dict(orient='records')

        return json.dumps(result)

    def holding_dividends(self, fund: str, ticker: str, start_date: str, end_date: str):

        df = self.query.get_dividends(fund, ticker, start_date, end_date)

        df['caldt'] = pd.to_datetime(df['caldt']).dt.strftime("%Y-%m-%d")

        result = df.to_dict(orient='records')

        return json.dumps(result)

    def holding_trades(self, fund: str, ticker: str, start_date: str, end_date: str):

        df = self.query.get_trades(fund, ticker, start_date, end_date)

        df['caldt'] = pd.to_datetime(df['caldt']).dt.strftime("%Y-%m-%d")

        result = df.to_dict(orient='records')

        return json.dumps(result)
    
    def cron_logs(self):
        df = self.query.get_cron_log()

        df['caldt'] = pd.to_datetime(df['caldt']).dt.strftime("%Y-%m-%d")

        result = df.to_dict(orient='records')

        return json.dumps(result)
    

    ############################# Portfolio Optimizer #############################

    def get_portfolio_defaults(self, fund):
        df = self.query.get_portfolio_defaults(fund)
        df = df.fillna('null')
        result = df.to_dict(orient='records')
        return json.dumps(result)
    
    def upsert_portfolio(self, fund, bmk_return, target_te):
        self.query.upsert_portfolio(fund,bmk_return,target_te)

    def get_all_holdings(self,fund):
        df = self.query.get_all_holdings(fund)
        df = df.fillna('null')
        result = df.to_dict(orient='records')
        return json.dumps(result)
    
    def upsert_holding(self, fund, ticker, horizon, target):
        self.query.upsert_holding(fund,ticker,horizon,target)
