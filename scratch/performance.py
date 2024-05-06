import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt

# pd.set_option('display.max_rows', None)

class Performance:
    def __init__(self, update, date1=None, date2=None):
        self.update = update
        self.date1 = date1
        self.date2 = date2

        # pick date range
        min_date_df1 = self.update.portfolio_returns['date'].min()
        min_date_df2 = self.update.individual_returns['date'].min()

        max_date_df1 = self.update.portfolio_returns['date'].max()
        max_date_df2 = self.update.individual_returns['date'].max()

        min_date = max(min_date_df1, min_date_df2)
        max_date = min(max_date_df1, max_date_df2)
        if date1 is None:
            self.date1 = min_date
        if date2 is None:
            self.date2 = max_date

        mask1 = (self.update.portfolio_returns['date'] >= self.date1) & (self.update.portfolio_returns['date'] <= self.date2)
        self.pret_df = self.update.portfolio_returns.loc[mask1]

        mask2 = (self.update.individual_returns['date'] >= self.date1) & (self.update.individual_returns['date'] <= self.date2)
        self.iret_df = self.update.individual_returns.loc[mask2]
        cash_mask = self.iret_df['symbol'] == 'CASH'
        self.iret_df.loc[cash_mask, 'ret'] = self.iret_df.loc[cash_mask, 'risk_free_return'].values

        #self.iret_df['wret'] = self.iret_df['ret'] * self.iret_df['weight_lagged']
        #sum_df = self.iret_df.groupby('date')['wret'].sum().reset_index().rename(columns={'wret': 'chk'})
        #sumdf2 = pd.merge(sum_df, self.pret_df, on='date', how='left')
        #diff = sumdf2['chk'] - sumdf2['total_return']

        # Temporary Data Edits


        self.get_portfolio_alpha()
        self.get_individual_alpha()
        self.get_total_returns()
        self.get_tracking_error()
        self.get_information_ratio()
        self.get_volatility()
        self.get_information_ratio()
        self.create_chart()

        print('###################################')
        print('Performance Report')
        print('###################################')
        print('Date Range:', self.date1, 'to', self.date2)
        print()
        print(self.portfolio_alpha)
        print()
        print(self.individual_alpha)
        print()
        print(self.total_returns)
        print()
        print('Annulaized Tracking error', self.tracking_error)
        print()
        print(self.contribution_to_tracking_error)
        print()
        print('Information Ratio', self.information_ratio)
        print()
        print('Annualized Portfolio Vol', self.portfolio_volatility)



    ###############################################
    def get_portfolio_alpha(self):
        pret = self.pret_df.copy()
        # Locate the max date
        max_date = pret['date'].max()
        # Check if risk_free_rate is NaN for max_date, and drop the row if True
        if pret.loc[pret['date'] == max_date, 'risk_free_return'].isna().any():
            pret = pret[pret['date'] != max_date]

        Y = pret['total_return'] - pret['risk_free_return']
        X = pret['ETF_return'] - pret['risk_free_return']

        df = pd.DataFrame()
        df['Y'] = Y
        df['X'] = X
        df.to_csv('output.csv', index=False)

        X = sm.add_constant(X)

        model = sm.OLS(Y, X)
        results = model.fit()
        portfolio_alpha = results.params['const'] * 250
        self.tmp=portfolio_alpha
        portfolio_alpha_CI = results.conf_int(alpha=0.05).loc['const'] * 250
        portfolio_beta = results.params[0]
        portfolio_alpha = {
            'portfolio_alpha': [portfolio_alpha],
            'CI_lower': [portfolio_alpha_CI[0]],
            'CI_upper': [portfolio_alpha_CI[1]],
            'portfolio_beta': [portfolio_beta]
        }
        self.portfolio_alpha = pd.DataFrame(portfolio_alpha)
        # Assuming df is your DataFrame
        self.portfolio_alpha.rename(index={0: "annualized daily alpha"}, inplace=True)

    ###############################################
    def get_individual_alpha(self):
        iret = self.iret_df.copy()

        max_date = iret['date'].max()
        # Check if risk_free_rate is NaN for max_date, and drop the row if True
        if iret.loc[iret['date'] == max_date, 'risk_free_return'].isna().any():
            iret = iret[iret['date'] != max_date]

        iret['exreti'] = iret['ret'] - iret['risk_free_return']
        iret['exretb'] = iret['ETF_return'] - iret['risk_free_return']

        counts = iret['symbol'].value_counts()
        iret = iret[iret['symbol'].isin(counts[counts > 5].index)]

        # fill in dates with ret and weight for all symbols
        # 1) Get unique dates and symbols
        unique_dates = iret['date'].unique()
        unique_symbols = iret['symbol'].unique()

        # 2) Create a MultiIndex of all combinations of dates and symbols
        idx = pd.MultiIndex.from_product([unique_dates, unique_symbols], names=['date', 'symbol'])

        # 3) Reindex original dataframe to this new MultiIndex
        filled_df = iret.set_index(['date', 'symbol']).reindex(idx).reset_index()

        # 4) Fill NaN values in 'ret' and 'weight' columns with 0
        filled_df['exreti'] = filled_df['exreti'].fillna(0)
        filled_df['weight_lagged'] = filled_df['weight_lagged'].fillna(0)

        # 5) Fill in exretb
        filled_df['exretb'] = filled_df.groupby('date')['exretb'].transform(lambda x: x.fillna(x.mean()))
        filled_df['exretw'] = filled_df['exreti'] * filled_df['weight_lagged']

        #tmp = filled_df[filled_df['symbol'] == 'CASH']
        #tmp = tmp.sort_values(by=['symbol', 'date'])
        #print(tmp)
        #quit()
        # run regressions by symbol
        def reg_alpha(df0, Yvar):
            grouped = df0.groupby('symbol')
            results_dict = {}
            for name, group in grouped:
                Y = group[Yvar]
                X = group['exretb']
                X = sm.add_constant(X)

                model = sm.OLS(Y, X)
                results = model.fit()
                results_dict[name] = results

                # initialize an empty dataframe
            dflist = []
            btlist = []


            # iterate over results
            for symbol, result in results_dict.items():
                # extract intercept
                intercept = result.params['const']
                beta = result.params['exretb']

                # append to dataframe
                dflist.append({'symbol': symbol, 'alpha': intercept, 'beta': beta})

            df = pd.DataFrame(dflist)

            # Merge the DataFrames on the 'symbol' column
            return df

        ialpha = reg_alpha(iret, 'exreti') # standard alphas and betas
        ialpha_c = reg_alpha(filled_df, 'exretw') # contribution to alpha and beta
        #print('straight alpha')
        #print(ialpha)
        #print('contribution to alpha')
        #print(ialpha_c)
        #print('hello from performance.py')
        #quit()
        #breakpoint()

        self.individual_alpha = ialpha_c
        self.individual_alpha['alpha'] = self.individual_alpha['alpha'] * 250
        total_alpha = self.individual_alpha['alpha'].sum()
        total_beta = self.individual_alpha['beta'].sum()
        #self.individual_alpha['alpha'] = self.individual_alpha['alpha'].apply(lambda x: x / total_alpha)
        #self.individual_alpha['beta'] = self.individual_alpha['beta'].apply(lambda x: x / total_beta)
        self.individual_alpha = self.individual_alpha.sort_values('alpha', ascending=False)

    def get_total_returns(self):
        pret = self.pret_df.copy()
        iret = self.iret_df.copy()
        pret['gross_tret'] = pret['total_return'] + 1
        pret['gross_etf'] = pret['ETF_return'] + 1
        pret['gross_rf'] = pret['risk_free_return'] + 1
        tret_total = pret['gross_tret'].prod() - 1
        etf_total = pret['gross_etf'].prod() - 1
        self.rf_total = pret['gross_rf'].prod() - 1
        iret['gross_iret'] = iret['ret'] + 1
        gross_iret = iret.groupby('symbol')['gross_iret'].prod() - 1

        gross_iret_df = gross_iret.to_frame(name='Total Return')
        # Assuming df is your DataFrame
        gross_iret_df = gross_iret_df.sort_values(by='Total Return', ascending=False)
        tret_total_df = pd.DataFrame({'Total Return': [tret_total]}, index=['Portfolio'])
        etf_total_df = pd.DataFrame({'Total Return': [etf_total]}, index=['Benchmark'])
        result = pd.concat([tret_total_df, etf_total_df, gross_iret_df])
        # Assuming df is your DataFrame
        result = result.drop('CASH', errors='ignore')
        result = result.drop('IWV', errors='ignore')
        self.total_returns = result

    def get_tracking_error(self):
        pret = self.pret_df.copy()
        iret = self.iret_df.copy()

        pret['h'] = pret['total_return'] - pret['ETF_return']
        self.tracking_error = pret['h'].std() * np.sqrt(252)
        tdata = pd.merge(iret, pret[['date', 'total_return']], on='date')
        #######
        tdata['exreti'] = tdata['ret'] - tdata['ETF_return']
        tdata['exretp'] = tdata['total_return'] - tdata['ETF_return']

        # fill in dates with ret and weight for all symbols
        # 1) Get unique dates and symbols
        unique_dates = tdata['date'].unique()
        unique_symbols = tdata['symbol'].unique()

        # 2) Create a MultiIndex of all combinations of dates and symbols
        idx = pd.MultiIndex.from_product([unique_dates, unique_symbols], names=['date', 'symbol'])

        # 3) Reindex original dataframe to this new MultiIndex
        filled_df = tdata.set_index(['date', 'symbol']).reindex(idx).reset_index()

        # 4) Fill NaN values in 'ret' and 'weight' columns with 0
        filled_df['exreti'] = filled_df['exreti'].fillna(0)
        filled_df['weight_lagged'] = filled_df['weight_lagged'].fillna(0)

        # 5) Fill in exretp
        filled_df['exretp'] = filled_df.groupby('date')['exretp'].transform(lambda x: x.fillna(x.mean()))
        filled_df['exretw'] = filled_df['exreti'] * filled_df['weight_lagged']

        # run regressions by symbol
        def reg_alpha(df0):
            grouped = df0.groupby('symbol')
            results_dict = {}
            for name, group in grouped:
                Y = group['exretw']
                X = group['exretp']
                X = sm.add_constant(X)

                model = sm.OLS(Y, X)
                results = model.fit()
                results_dict[name] = results

                # initialize an empty dataframe
            dflist = []

            # iterate over results
            for symbol, result in results_dict.items():
                beta = result.params['exretp']
                # append to dataframe
                dflist.append({'symbol': symbol, 'beta': beta})

            df = pd.DataFrame(dflist)

            return df

        tbeta = reg_alpha(filled_df) # contribution to beta
        tbeta = tbeta.sort_values(by='beta', ascending=False)
        self.contribution_to_tracking_error = tbeta
        #print(self.contribution_to_tracking_error)



        #print(hbeta)

    def get_information_ratio(self):
        ar= self.tracking_error
        tr=self.total_returns


        portfolio_return = tr.loc['Portfolio', 'Total Return']  # Portfolio return
        benchmark_return = tr.loc['Benchmark', 'Total Return']  # Benchmark return
        # Get the minimum and maximum dates

        num_days = (self.date2- self.date1).days

        difference = (portfolio_return - benchmark_return)*252/num_days
        self.information_ratio = self.tmp / ar

    def get_volatility(self):
        pret = self.pret_df.copy()
        iret = self.iret_df.copy()
        pvol = pret['total_return'].std()
        self. portfolio_volatility = pvol * np.sqrt(252)
        #std_dev_by_symbol = iret.groupby('symbol')['ret'].std() * np.sqrt(252)
        #new_row = {'Symbol': 'Portfolio', 'Portfolio Volatility': portfolio_volatility}
        #df = df.append(new_row, ignore_index=True)


    def create_chart(self):
        pret = self.pret_df.copy()

        pret['total_cumulative'] = (1 + pret['total_return']).cumprod()
        pret['ETF_cumulative'] = (1 + pret['ETF_return']).cumprod()
        pret = pret[['date', 'total_cumulative', 'ETF_cumulative']]
        min_date = pret['date'].min()
        min_date_minus_1 = min_date - pd.Timedelta(days=1)
        new_row = {'date': min_date_minus_1, 'total_cumulative': 1.0, 'ETF_cumulative': 1.0}
        pret = pret.append(new_row, ignore_index=True)

        # Plot the cumulative products
        plt.figure(figsize=(10, 6))
        plt.plot(pret['date'], pret['total_cumulative'], label='Total Return')
        plt.plot(pret['date'], pret['ETF_cumulative'], label='ETF Return')
        plt.xlabel('Date')
        plt.ylabel('Cumulative Return')
        plt.title('Cumulative Return Comparison')
        plt.legend()
        plt.grid(True)
        plt.show()
        plt.savefig('plot_grad.png')
        pret.to_csv('data.csv', index=False)