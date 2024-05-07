import re
import time
from io import StringIO
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import requests

pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)


class IBKRupdate:
    def __init__(self, class0=None, token=None, cashfile=None, holdingsfile=None, dividendsfile=None, ttwfile=None, mmsfile=None):
        self.token = token
        if class0 == 'grad':
            self.queryID1 = "946117"
            self.queryID2 = "946128"
            self.queryID3 = "946136"
            self.queryID4 = "951285"
            self.queryID5 = "954248"
            self.queryID6 = "999999"
            self.f_cash = 'data/Cash_365_Base_grad.csv'
            self.f_holdings = 'data/Holdings_365_Base_grad.csv'
            self.f_dividends = 'data/Dividends_365_Base_grad.csv'
            self.f_ttw = 'data/ttw_365_Base_grad.csv'
            self.f_mms = 'data/mms_365_Base_grad.csv'
            self.cash_path = 'data/grad_cash.parquet'
            self.holdings_path = 'data/grad_holdings.parquet'
            self.dividends_path = 'data/grad_dividends.parquet'
            self.ttw_path = 'data/grad_ttw.parquet'
            self.mms_path = 'data/grad_mms.parquet'
        elif class0 == 'undergrad':
            self.queryID1 = "965039"
            self.queryID2 = "965045"
            self.queryID3 = "965080"
            self.queryID4 = "965132"
            self.queryID5 = "965057"
            self.queryID6 = "999999"
            self.f_cash = 'data/Cash_365_Base_undergrad.csv'
            self.f_holdings = 'data/Holdings_365_Base_undergrad.csv'
            self.f_dividends = 'data/Dividends_365_Base_undergrad.csv'
            self.f_ttw = 'data/ttw_365_Base_undergrad.csv'
            self.f_mms = 'data/mms_365_Base_undergrad.csv'
            self.cash_path = 'data/undergrad_cash.parquet'
            self.holdings_path = 'data/undergrad_holdings.parquet'
            self.dividends_path = 'data/undergrad_dividends.parquet'
            self.ttw_path = 'data/undergrad_ttw.parquet'
            self.mms_path = 'data/undergrad_mms.parquet'
        elif class0 == 'BC':
            self.queryID1 = "965159"
            self.queryID2 = "965164"
            self.queryID3 = "965180"
            self.queryID4 = "965209"
            self.queryID5 = "965172"
            self.queryID6 = "999999"
            self.f_cash = 'data/Cash_365_Base_BC.csv'
            self.f_holdings = 'data/Holdings_365_Base_BC.csv'
            self.f_dividends = 'data/Dividends_365_Base_BC.csv'
            self.f_ttw = 'data/ttw_365_Base_BC.csv'
            self.f_mms = 'data/mms_365_Base_BC.csv'
            self.cash_path = 'data/BC_cash.parquet'
            self.holdings_path = 'data/BC_holdings.parquet'
            self.dividends_path = 'data/BC_dividends.parquet'
            self.ttw_path = 'data/BC_ttw.parquet'
            self.mms_path = 'data/BC_mms.parquet'
        elif class0 is None:
            self.cashfile = cashfile
            self.holdingsfile = holdingsfile
            self.dividendsfile = dividendsfile
            self.ttwfile = ttwfile
            self.mmsfile = mmsfile


        self.update_IBKR()
        self.create_portfolio_returns()
        self.create_individual_returns()

    ###############################################
    def update_IBKR(self):
        print()
        print('###################################')
        print('Updating IBKR Data')
        print('###################################')
        if self.token is not None:
            self.new_cash = self.load_cash(queryID=self.queryID1, histload=True)  # get latest cash file
            self.new_holdings = self.load_holdings(queryID=self.queryID2, histload=True)  # get latest holdings file
            self.new_dividends = self.load_dividends(queryID=self.queryID3, histload=True)  # get latest dividends file
            self.new_ttw = self.load_ttw(queryID=self.queryID4, histload=True)  # get latest trades, transfers, withdrawals file
            self.new_mms = self.load_mms(queryID=self.queryID5, histload=True)  # get latest mark-to-market file
        elif self.cashfile is None and self.holdingsfile is None and self.dividendsfile is None and self.ttwfile is None and self.mmsfile is None:
            self.new_cash = self.load_cash(file=self.f_cash, histload=False)  # get cash 365 file
            self.new_holdings = self.load_holdings(file=self.f_holdings, histload=False)  # get holdings 365 file
            self.new_dividends = self.load_dividends(file=self.f_dividends, histload=False)  # get dividends 365 file
            self.new_ttw = self.load_ttw(file=self.f_ttw, histload=False)  # get trades, transfers, withdrawals 365 file
            self.new_mms = self.load_mms(file=self.f_mms, histload=False)  # get mark-to-market 365 file
        else:
            self.new_cash=self.cashfile
            self.new_holdings=self.holdingsfile
            self.new_dividends=self.dividendsfile
            self.new_ttw=self.ttwfile
            self.new_mms=self.mmsfile

    # function to perform IBKR query
    def IBKR_query(self, queryID):

        if self.token is None:
            print('No token specified')
            return

        url = f'https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/SendRequest?t={self.token}&q={queryID}&v=3'
        user_agent = {'User-agent': 'Python/3.9'}
        response = requests.get(url, headers=user_agent)

        reference_code = re.findall('(?<=<ReferenceCode>)\d*(?=<\/ReferenceCode>)', response.text)[0]
        # print(reference_code)
        url = f'https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/GetStatement?t={self.token}&q={reference_code}&v=3'
        time.sleep(2)
        response = requests.get(url)
        csv_string = StringIO(response.text)
        c0 = pd.read_csv(csv_string)
        return c0

    ###############################################
    def load_cash(self, queryID=None, file=None, histload=None):

        if queryID is None and file is None:
            print('No queryID or file specified')
            return
        if queryID is not None:
            c0 = self.IBKR_query(queryID)
        elif file is not None:
            c0 = pd.read_csv(file, header=None)

        cols = ['date', 'cash', 'cashlong', 'cashshort', 'stock', 'stocklong', 'stockshort', 'd_accruals', 'd_accrualslong', 'd_accrualsshort', 'i_accruals', 'i_accrualslong', 'i_accrualsshort', 'total', 'totallong', 'totalshort', 'funds', 'fundslong', 'fundsshort']
        c0.columns = cols
        c1 = c0[c0['date'] != 'ReportDate']
        c2 = c1.copy()
        c2['date'] = pd.to_datetime(c2['date'], format='%Y%m%d')
        c2['cash'] = pd.to_numeric(c2['cash'], errors='coerce')
        c2['cashlong'] = pd.to_numeric(c2['cashlong'], errors='coerce')
        c2['cashshort'] = pd.to_numeric(c2['cashshort'], errors='coerce')
        c2['stock'] = pd.to_numeric(c2['stock'], errors='coerce')
        c2['stocklong'] = pd.to_numeric(c2['stocklong'], errors='coerce')
        c2['stockshort'] = pd.to_numeric(c2['stockshort'], errors='coerce')
        c2['d_accruals'] = pd.to_numeric(c2['d_accruals'], errors='coerce')
        c2['d_accrualslong'] = pd.to_numeric(c2['d_accrualslong'], errors='coerce')
        c2['d_accrualsshort'] = pd.to_numeric(c2['d_accrualsshort'], errors='coerce')
        c2['i_accruals'] = pd.to_numeric(c2['i_accruals'], errors='coerce')
        c2['i_accrualslong'] = pd.to_numeric(c2['i_accrualslong'], errors='coerce')
        c2['i_accrualsshort'] = pd.to_numeric(c2['i_accrualsshort'], errors='coerce')
        c2['total'] = pd.to_numeric(c2['total'], errors='coerce')
        c2['totallong'] = pd.to_numeric(c2['totallong'], errors='coerce')
        c2['totalshort'] = pd.to_numeric(c2['totalshort'], errors='coerce')
        c2['funds'] = pd.to_numeric(c2['funds'], errors='coerce')
        c2['fundslong'] = pd.to_numeric(c2['fundslong'], errors='coerce')
        c2['fundsshort'] = pd.to_numeric(c2['fundsshort'], errors='coerce')

        # concatenate with base cash
        if histload is True or histload is None:
            base_cash = pq.read_table(self.cash_path).to_pandas()
            cash = pd.concat([base_cash, c2], axis=0, ignore_index=True)
        else:
            cash = c2

        cash = cash.round(2)  # Rounds all numeric columns to 2 decimal places
        cash = cash.drop_duplicates(subset=['date', 'cash', 'stock', 'd_accruals', 'i_accruals', 'total', 'funds'])
        cash = cash[['date', 'cash', 'stock', 'd_accruals', 'i_accruals', 'total', 'funds']]
        cash = cash.sort_values(by=['date']).reset_index(drop=True)
        # Step 1: Group the DataFrame by the date column
        grouped = cash.groupby('date')

        # Step 2: Define a function to check the differences between values
        def check_differences(group):
            # Selecting all columns except the date column
            cols_to_check = group.columns.difference(['date'])
            # Calculating the difference for each column
            diff = group[cols_to_check].diff().fillna(0)
            # Checking if the maximum difference is less than or equal to 0.01 (1 cent)
            return (diff.abs() <= 0.01).all(axis=1)

        # Step 3: Apply the function and filter the DataFrame
        mask = grouped.apply(check_differences)
        filtered_cash = cash.loc[mask.reset_index(drop=True)]

        # check for duplicate dates
        chk1 = filtered_cash.duplicated(subset=['date'])
        all_false = not chk1.any()
        print('No Duplicate Dates in Cash File:', all_false)
        min_date = filtered_cash['date'].min().date()
        max_date = filtered_cash['date'].max().date()
        print("Cash File: Min/Max Dates:", min_date, max_date)
        print()

        return filtered_cash

    ###############################################
    def load_holdings(self, queryID=None, file=None, histload=None):

        if queryID is None and file is None:
            print('No queryID or file specified')
            return
        if queryID is not None:
            h0 = self.IBKR_query(queryID)
        elif file is not None:
            h0 = pd.read_csv(file, header=None)

        cols = ['symbol', 'securityID', 'date', 'shares', 'price', 'value']
        h0.columns = cols
        h1 = h0[h0['symbol'] != 'Symbol']
        h2 = h1.copy()
        h2['price'] = pd.to_numeric(h2['price'], errors='coerce')
        h2['shares'] = pd.to_numeric(h2['shares'], errors='coerce')
        h2['value'] = pd.to_numeric(h2['value'], errors='coerce')
        h2['date'] = pd.to_datetime(h2['date'], format='%Y%m%d')

        # concatenate with base holdings
        if histload is True or histload is None:
            base_holdings = pq.read_table(self.holdings_path).to_pandas()
            holdings = pd.concat([base_holdings, h2], axis=0, ignore_index=True)
        else:
            holdings = h2

        # holdings = holdings.round(2)  # Rounds all numeric columns to 2 decimal places
        holdings = holdings.drop_duplicates(subset=['symbol', 'securityID', 'date', 'shares', 'price', 'value'])
        holdings = holdings[['symbol', 'securityID', 'date', 'shares', 'price', 'value']]

        # check for duplicate dates
        chk1 = holdings.duplicated(subset=['date', 'symbol'])
        all_false = not chk1.any()
        print('No Duplicate Dates/Symbols in Holdings File:', all_false)
        min_date = holdings['date'].min().date()
        max_date = holdings['date'].max().date()
        print("Holdings File: Min/Max Dates:", min_date, max_date)
        print()

        holdings = holdings.sort_values(by=['symbol', 'date'])
        holdings['lagged_price'] = holdings.groupby('symbol')['price'].shift()
        return holdings

    ###############################################
    def load_dividends(self, queryID=None, file=None, histload=None):

        if queryID is None and file is None:
            print('No queryID or file specified')
            return
        if queryID is not None:
            d0 = self.IBKR_query(queryID)
        elif file is not None:
            d0 = pd.read_csv(file, header=None)

        cols = ['symbol', 'securityID', 'exdate', 'paydate', 'shares', 'dividend', 'tax', 'fee', 'divpershare', 'net']
        d0.columns = cols
        d1 = d0[d0['symbol'] != 'Symbol']
        d2 = d1[['symbol', 'securityID', 'shares', 'exdate', 'paydate', 'divpershare']].drop_duplicates()
        d3 = d2.copy()

        d3['shares'] = pd.to_numeric(d3['shares'], errors='coerce')
        d3['divpershare'] = pd.to_numeric(d3['divpershare'], errors='coerce')
        d3['exdate'] = pd.to_datetime(d3['exdate'], format='%Y%m%d')
        d3['paydate'] = pd.to_datetime(d3['paydate'], format='%Y%m%d')
        # Sum dividends if two are paid on same date
        d4 = d3.groupby(['symbol', 'securityID', 'exdate', 'paydate'])['divpershare'].sum().reset_index()

        # concatenate with base dividends

        if histload is True or histload is None:
            base_dividends = pq.read_table(self.dividends_path).to_pandas()
            dividends = pd.concat([base_dividends, d4], axis=0, ignore_index=True)
        else:
            dividends = d4

        # dividends = dividends.round(2)  # Rounds all numeric columns to 2 decimal places
        dividends = dividends.drop_duplicates(subset=['symbol', 'securityID', 'exdate', 'paydate', 'divpershare'])
        dividends = dividends[['symbol', 'securityID', 'exdate', 'paydate', 'divpershare']]

        # check for duplicate dates
        chk1 = dividends.duplicated(subset=['symbol', 'exdate'])
        all_false = not chk1.any()
        print('No Duplicate Exdates/Symbols in Dividend File:', all_false)
        min_date = dividends['exdate'].min().date()
        max_date = dividends['exdate'].max().date()
        print("Dividends File: Min/Max Dates:", min_date, max_date)
        print()
        return dividends

    ###############################################
    def load_ttw(self, queryID=None, file=None, histload=None):

        if queryID is None and file is None:
            print('No queryID or file specified')
            return
        if queryID is not None:
            t0 = self.IBKR_query(queryID)
        elif file is not None:
            t0 = pd.read_csv(file, header=None)

        num_cols = t0.shape[1]
        # IBKR Query sometimes returns 17 columns and sometimes 27 columns
        if num_cols == 18:
            cols = ['FromDate', 'ToDate', 'NetTradesPurchases', 'NetTradesPurchasesSecurities', 'NetTradesPurchasesCommodities', 'NetTradesSales', 'NetTradesSalesSecurities', 'NetTradesSalesCommodities', 'AccountTransfers',
                    'AccountTransfersSecurities', 'AccountTransfersCommodities', 'Deposits', 'DepositsSecurities', 'DepositsCommodities', 'Withdrawals', 'WithdrawalsSecurities', 'WithdrawalsCommodities', 'Currency']
        elif num_cols == 28:
            cols = ['FromDate', 'ToDate', 'NetTradesPurchases', 'NetTradesPurchasesSecurities', 'NetTradesPurchasesCommodities', 'NetTradesPurchasesMTD', 'NetTradesPurchasesYTD', 'NetTradesSales', 'NetTradesSalesSecurities',
                    'NetTradesSalesCommodities', 'NetTradesSalesMTD', 'NetTradesSalesYTD', 'AccountTransfers', 'AccountTransfersSecurities', 'AccountTransfersCommodities', 'AccountTransfersMTD', 'AccountTransfersYTD',
                    'Deposits', 'DepositsSecurities', 'DepositsCommodities', 'DepositsMTD', 'DepositsYTD', 'Withdrawals', 'WithdrawalsSecurities', 'WithdrawalsCommodities', 'WithdrawalsMTD', 'WithdrawalsYTD', 'Currency']
        else:
            print('Unknown number of columns in ttw file')
            return

        t0.columns = cols
        t0a = t0[t0['Currency']=='BASE_SUMMARY']

        t1 = t0a[['FromDate', 'ToDate', 'NetTradesPurchases', 'NetTradesSales', 'AccountTransfers', 'Deposits', 'Withdrawals']]
        t2 = t1[t1['FromDate'] != 'FromDate']
        t3 = t2.copy()

        t3['NetTradesPurchases'] = pd.to_numeric(t3['NetTradesPurchases'], errors='coerce')
        t3['NetTradesSales'] = pd.to_numeric(t3['NetTradesSales'], errors='coerce')
        t3['AccountTransfers'] = pd.to_numeric(t3['AccountTransfers'], errors='coerce')
        t3['Deposits'] = pd.to_numeric(t3['Deposits'], errors='coerce')
        t3['Withdrawals'] = pd.to_numeric(t3['Withdrawals'], errors='coerce')
        t4 = t3.copy()
        t4['FromDate'] = pd.to_datetime(t4['FromDate'], format='%Y%m%d')
        t4['ToDate'] = pd.to_datetime(t4['ToDate'], format='%Y%m%d')

        all_dates_equal = (t4['FromDate'] == t4['ToDate']).all()
        print("'FromDate' and 'ToDate' equal for all rows?", all_dates_equal)
        t4['date'] = t4['FromDate']

        # concatenate with base file
        if histload is True or histload is None:
            base_file = pq.read_table(self.ttw_path).to_pandas()
            outfile = pd.concat([base_file, t4], axis=0, ignore_index=True)
        else:
            outfile = t4


        outfile = outfile.drop_duplicates(subset=['date', 'NetTradesPurchases', 'NetTradesSales', 'AccountTransfers', 'Deposits', 'Withdrawals'])
        outfile = outfile[['date', 'NetTradesPurchases', 'NetTradesSales', 'AccountTransfers', 'Deposits', 'Withdrawals']]

        # check for duplicate dates
        chk1 = outfile.duplicated(subset=['date'])
        all_false = not chk1.any()
        print('No Duplicate Dates in Transfer File:', all_false)
        min_date = outfile['date'].min().date()
        max_date = outfile['date'].max().date()
        print("TTW File: Min/Max Dates:", min_date, max_date)
        print()
        return outfile

    def load_mms(self, queryID=None, file=None, histload=None):

        if queryID is None and file is None:
            print('No queryID or file specified')
            return
        if queryID is not None:
            t0 = self.IBKR_query(queryID)
        elif file is not None:
            t0 = pd.read_csv(file, header=None)

        cols = ['date', 'SecurityID', 'Symbol', 'MarktoMarket']

        t0.columns = cols
        t2 = t0[t0['date'] != 'ReportDate']
        t3 = t2.copy()
        t3['MarktoMarket'] = pd.to_numeric(t3['MarktoMarket'], errors='coerce')
        t4 = t3.copy()
        t4['date'] = pd.to_datetime(t4['date'], format='%Y%m%d')
        t4 = t4.groupby(['date'])['MarktoMarket'].sum().reset_index()

        # concatenate with base file
        if histload is True or histload is None:
            base_file = pq.read_table(self.mms_path).to_pandas()
            outfile = pd.concat([base_file, t4], axis=0, ignore_index=True)
        else:
            outfile = t4

        outfile = outfile.drop_duplicates(subset=['date', 'MarktoMarket'])
        outfile = outfile[['date', 'MarktoMarket']]

        # check for duplicate dates
        chk1 = outfile.duplicated(subset=['date'])
        all_false = not chk1.any()
        print('No Duplicate Dates in MarktoMarket File:', all_false)
        min_date = outfile['date'].min().date()
        max_date = outfile['date'].max().date()
        print("MMS File: Min/Max Dates:", min_date, max_date)
        print()
        return outfile

    ###############################################
    # Other Functions to create class attributes
    ###############################################
    def create_stock_returns(self):
        ifile1 = pd.merge(self.new_holdings, self.new_dividends[['securityID', 'exdate', 'paydate', 'divpershare']], left_on=['securityID', 'date'], right_on=['securityID', 'exdate'], how='left')
        ifile1['divpershare'] = ifile1['divpershare'].fillna(0)

        chk0 = ifile1.dropna(subset=['exdate'])
        chk1 = chk0.duplicated(subset=['symbol', 'exdate'])
        all_false = not chk1.any()
        print('No Duplicate Exdates/Symbols in Stock Returns File:', all_false)

        ifile1['ret'] = (ifile1['price'] + ifile1['divpershare']) / ifile1['lagged_price'] - 1
        ifile1.loc[ifile1['shares'] < 0, 'ret'] *= -1

        # get weights and create weighted returns
        ifile1['total_position_stock'] = ifile1.groupby('date')['value'].transform('sum')
        ifile1['sweight0'] = ifile1['value'] / ifile1['total_position_stock']  # get weights
        ifile1 = ifile1.sort_values(by=['symbol', 'date'])
        ifile1['stockweight_lagged'] = ifile1.groupby('symbol')['sweight0'].shift(1)

        valuechk = ifile1['value'] - ifile1['price'] * ifile1['shares']
        diff1 = valuechk.abs().max()
        print('Calculated stock values equal price time shares? (should be close to zero, units=dollars):', diff1)

        self.stock_returns = ifile1[['date', 'symbol', 'securityID', 'shares', 'price', 'divpershare', 'exdate', 'paydate', 'ret', 'stockweight_lagged', 'value']]

    # build stock portfolio return from individual stock returns.  I use this as a check on the implied stock return calculation
    def create_stockp_return_chk(self):
        self.create_stock_returns()
        ifile = self.stock_returns.copy()
        ifile['sweighted_returns'] = ifile['stockweight_lagged'] * ifile['ret']
        self.stockp_return_chk = ifile.groupby('date', as_index=False)['sweighted_returns'].sum()

    def create_total_holdings_df(self):
        # create dataframe for total holdings in stock and cash
        # merge in trades, transfers, withdrawals, deposits, mark-to-market summary, and total dividend

        thold0 = pd.merge(self.new_cash, self.new_ttw, on=['date'], how='left')
        thold1 = pd.merge(thold0, self.new_mms, on='date', how='left')
        dividends = pd.merge(self.new_holdings[['date', 'securityID', 'shares']], self.new_dividends, left_on=['date', 'securityID'], right_on=['exdate', 'securityID'], how='right')
        dividends['total_dividend'] = dividends['shares'] * dividends['divpershare']
        sum_dividends = dividends.groupby('exdate')['total_dividend'].sum().reset_index()
        thold2 = pd.merge(thold1, sum_dividends, left_on='date', right_on='exdate', how='left')
        thold2['total_dividend'] = thold2['total_dividend'].fillna(0)
        self.total_holdings_df = thold2

    def create_stock_portfolio_return(self):
        self.create_stockp_return_chk()
        self.create_total_holdings_df()
        thold2 = self.total_holdings_df.copy()
        thold2['implied_stock'] = thold2['stock'] + thold2['NetTradesPurchases'] + thold2['NetTradesSales'] - thold2['MarktoMarket'] + thold2['total_dividend']
        thold3 = thold2.sort_values(by='date')
        thold3['lagged_stock'] = thold3['stock'].shift(1)
        thold3['stock_portfolio_return'] = np.where(thold3['lagged_stock'] == 0, 0, thold3['implied_stock'] / thold3['lagged_stock'] - 1)
        thold4 = pd.merge(thold3, self.stockp_return_chk, on='date', how='left')
        # check implied stock value calculation
        retchk = thold4['stock_portfolio_return'] - thold4['sweighted_returns']
        diff1 = retchk.abs().max()
        print('Calculated stock return equal reported? (should be close to zero, units=decimal return):', diff1)
        self.stock_portfolio_return = thold4[['date', 'stock_portfolio_return', 'sweighted_returns']]
        self.build_total_holdings1 = thold4

    def create_risk_free_return(self):
        fred_key = '971fa55a70d1cee395102f9d52510052'
        fred_series = 'DGS1MO'
        url = f'https://api.stlouisfed.org/fred/series/observations?series_id={fred_series}&api_key={fred_key}&file_type=json'
        response = requests.get(url)
        data = response.json()
        fred_data = pd.DataFrame(data['observations'])
        fred_data['date'] = pd.to_datetime(fred_data['date'])
        fred_data['value'] = pd.to_numeric(fred_data['value'], errors='coerce')
        fred_data = fred_data[['date', 'value']]
        fred_data['value'] = fred_data['value'] * .01
        fred_data = fred_data.sort_values(by='date')
        fred_data['value'] = fred_data['value'].fillna(fred_data['value'].shift(1))
        fred_data['lagged_value'] = fred_data['value'].shift(1)
        fred_data['P0'] = 100 / (1 + fred_data['lagged_value'] * 30 / 360)
        fred_data['P1'] = 100 / (1 + fred_data['value'] * 29 / 360)
        fred_data['risk_free_return'] = fred_data['P1'] / fred_data['P0'] - 1
        fred_data['rf'] = fred_data['value'] / 360
        self.risk_free_return = fred_data[['date', 'rf', 'risk_free_return']]

    def create_cash_return(self):
        self.create_stock_portfolio_return()
        thold4 = self.build_total_holdings1.copy()
        thold4 = thold4.sort_values(by=['date'])
        thold4[['lag_cash', 'lag_i_accruals']] = thold4[['cash', 'i_accruals']].shift(1)
        thold4['cash_return'] = (thold4['i_accruals'] - thold4['lag_i_accruals']) / thold4['lag_cash']
        thold4.loc[thold4['cash_return'] < 0, 'cash_return'] = thold4['i_accruals'] / thold4['lag_cash']
        implied_cash_chk = thold4['cash'] + thold4['d_accruals'] + thold4['i_accruals'] + thold4['stock'] + thold4['funds'] - thold4['total']
        thold4['implied_cash_chk'] = implied_cash_chk  # Creating a new column in DataFrame to store calculated values
        diff1 = implied_cash_chk.abs().max()
        print('Calculated total equals reported NAV? (should be close to zero, units=dollars):', diff1)

        # implied cash at the end of the day after all transfers, etc. I use implied cash to determine (lagged) weight
        thold4['implied_cash'] = thold4['cash'] + thold4['d_accruals'] + thold4['i_accruals']
        # implied total is the total net any transfers, etc. I use implied total in the numerator of total return
        thold4['implied_total'] = thold4['total'] - thold4['AccountTransfers'] - thold4['Deposits'] - thold4['Withdrawals']

        # create total return
        thold4 = thold4.sort_values(by=['date'])
        thold4[['lagged_total', 'lagged_stock']] = thold4[['total', 'stock']].shift(1)
        thold4['total_return'] = thold4['implied_total'] / thold4['lagged_total'] - 1

        # stock and cash weights at end of day after all transactions, etc.
        thold4['w_stock0'] = thold4['stock'] / thold4['total']
        thold4['w_cash0'] = thold4['implied_cash'] / thold4['total']
        thold4 = thold4.sort_values(by=['date'])
        thold4[['w_stocklag', 'w_cashlag']] = thold4[['w_stock0', 'w_cash0']].shift(1)

        # create implied return on cash and mark-to-market gains/losses
        cash_return = thold4[['date', 'cash_return', 'implied_cash']]
        cash_return2 = cash_return.copy()
        cash_return2['symbol'] = 'CASH'
        cash_return2['securityID'] = '999'
        cash_return2['divpershare'] = 0
        self.cash_return = cash_return2.rename(columns={'cash_return': 'ret', 'implied_cash': 'value'})
        self.build_total_holdings2 = thold4

        print()
        print('Summary Stats: Return on Cash and Mark-to-market gains/losses:')
        print(thold4['cash_return'].describe())
        print()

    def create_portfolio_returns(self):
        self.create_cash_return()
        thold5 = self.build_total_holdings2.copy()
        port_return = thold5[['date', 'total_return', 'stock_portfolio_return', 'cash_return', 'w_stocklag', 'w_cashlag']]
        ETF_return = self.stock_returns[self.stock_returns['symbol'] == 'IWV']
        ETF_return = ETF_return[['date', 'ret']].rename(columns={'ret': 'ETF_return'})
        port_return = pd.merge(port_return, ETF_return, on='date', how='left')
        IWV = pd.read_csv('data/IWV.csv')
        IWV['date'] = pd.to_datetime(IWV['date'])
        port_return = pd.merge(port_return, IWV[['date', 'IWV_return']], on='date', how='left')
        port_return['ETF_return'] = port_return['ETF_return'].fillna(port_return['IWV_return'])
        port_return = port_return.drop(['IWV_return'], axis=1)
        self.portfolio_returns = port_return.dropna(subset=['total_return'])
        self.create_risk_free_return()
        self.portfolio_returns = pd.merge(self.portfolio_returns, self.risk_free_return[['date', 'risk_free_return']], on='date', how='left')
        self.portfolio_returns = self.portfolio_returns.rename(columns={'w_stocklag': 'weight_stock', 'w_cashlag': 'weight_cash'})

    def create_individual_returns(self):
        iret = pd.concat([self.stock_returns, self.cash_return], axis=0, ignore_index=True)
        iret['total_value'] = iret.groupby('date')['value'].transform('sum')
        iret['weight'] = iret['value'] / iret['total_value']
        iret = iret.sort_values(by=['symbol', 'date'])
        iret['weight_lagged'] = iret.groupby('symbol')['weight'].shift(1)
        iret = iret.dropna(subset=['ret'])
        IVW = iret[iret['symbol'] == 'IWV']
        IVW = IVW[['date', 'ret']].rename(columns={'ret': 'ETF_return'})
        iret2 = pd.merge(iret, IVW, on='date', how='left')
        iret3 = pd.merge(iret2, self.risk_free_return[['date', 'risk_free_return']], on='date', how='left')
        IWV = pd.read_csv('data/IWV.csv')
        IWV['date'] = pd.to_datetime(IWV['date'])
        iret3 = pd.merge(iret3, IWV[['date', 'IWV_return']], on='date', how='left')
        iret3['ETF_return'] = iret3['ETF_return'].fillna(iret3['IWV_return'])
        iret3 = iret3.drop(['IWV_return'], axis=1)
        self.individual_returns = iret3

    def create_cash_return_old(self):
        self.create_stock_portfolio_return()
        thold4 = self.build_total_holdings1.copy()
        # check implied cash calculation
        implied_cash_chk = thold4['cash'] + thold4['d_accruals'] + thold4['i_accruals'] + thold4['stock'] + thold4['funds'] - thold4['total']
        thold4['implied_cash_chk'] = implied_cash_chk  # Creating a new column in DataFrame to store calculated values

        # Creating a new DataFrame which only contains rows where 'implied_cash_chk' is greater than 10
        # filtered_thold4 = thold4[thold4['implied_cash_chk'] < -10]
        # print(filtered_thold4)
        # quit()


        diff1 = implied_cash_chk.abs().max()
        print('Calculated total equals reported NAV? (should be close to zero, units=dollars):', diff1)
        # create implied return on cash and mark-to-market gains/losses
        # implied cash at the end of the day after all transfers, etc. I use implied cash to determine (lagged) weight
        thold4['implied_cash'] = thold4['total'] - thold4['stock']
        # implied total is the total net any transfers, etc. I use implied total in the numerator of total return
        thold4['implied_total'] = thold4['total'] - thold4['AccountTransfers'] - thold4['Deposits'] - thold4['Withdrawals']
        thold4 = thold4.sort_values(by=['date'])
        thold4[['lagged_total', 'lagged_stock']] = thold4[['total', 'stock']].shift(1)
        thold4['total_return'] = thold4['implied_total'] / thold4['lagged_total'] - 1
        # stock and cash weights at end of day after all transactions, etc.
        thold4['w_stock0'] = thold4['stock'] / thold4['total']
        thold4['w_cash0'] = thold4['implied_cash'] / thold4['total']
        thold4 = thold4.sort_values(by=['date'])
        thold4[['w_stocklag', 'w_cashlag']] = thold4[['w_stock0', 'w_cash0']].shift(1)
        # create impied return on cash and mark-to-market gains/losses
        thold4['cash_return'] = (thold4['total_return'] - thold4['w_stocklag'] * thold4['stock_portfolio_return']) / thold4['w_cashlag']
        cash_return = thold4[['date', 'cash_return', 'implied_cash']]
        cash_return2 = cash_return.copy()
        cash_return2['symbol'] = 'CASH'
        cash_return2['securityID'] = '999'
        cash_return2['divpershare'] = 0
        self.cash_return = cash_return2.rename(columns={'cash_return': 'ret', 'implied_cash': 'value'})
        self.build_total_holdings2 = thold4

        print()
        print('Summary Stats: Return on Cash and Mark-to-market gains/losses:')
        print(thold4['cash_return'].describe())
        print()
