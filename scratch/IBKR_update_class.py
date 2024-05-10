import re
import time
from io import StringIO
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import requests
import formatting_functions as ff

pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)


class update:
    def __init__(self, class0, token):
        self.token = token
        self.class0 = class0
        if class0 == 'grad':
            self.queryID1 = "987356" # Delta NAV
            self.queryID2 = "987359" # Holdings
            self.queryID3 = "987361" # Dividends
            self.queryID4 = "987363" # Trades
            self.delta_NAV_path = 'data_parquet/Delta_NAV_grad.parquet'
            self.holdings_path = 'data_parquet/Holdings_grad.parquet'
            self.dividends_path = 'data_parquet/Trades_grad.parquet'
            self.trades_path = 'data_parquet/Trades_grad.parquet'
        elif class0 == 'undergrad':
            raise ValueError("Error: Not yet implemented for undergrad account.")
        elif class0 == 'BC':
            raise ValueError("Error: Not yet implemented for BC account.")
        elif class0 is None:
            raise ValueError("Error: Must specify account to update.")

        self.load_delta_NAV(queryID=self.queryID1)  # get latest cash file
        self.load_holdings(queryID=self.queryID2)  # get latest holdings file
        self.load_dividends(queryID=self.queryID3)  # get latest dividends file
        self.load_trades(queryID=self.queryID4)  # get latest trades

    ################################################
    # function to perform IBKR query
    ################################################
    def IBKR_query(self, queryID):

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
    def load_delta_NAV(self, queryID):
        c0 = self.IBKR_query(queryID)
        c1 = c0[c0['FromDate'] != 'FromDate']
        c2 = c1.copy()
        c3 = ff.format_delta_nav(c2)
        historical_data = pd.read_parquet('data_parquet/Delta_NAV_grad.parquet')
        historical_data = historical_data._append(c3, ignore_index=True)
        historical_data_unique = historical_data.drop_duplicates(subset=['FromDate', 'ToDate'], keep='last')
        historical_data_unique.to_parquet('data_parquet/Delta_NAV_grad.parquet')

    def load_holdings(self, queryID):
        c0 = self.IBKR_query(queryID)
        c1 = c0[c0['Conid'] != 'Conid']
        c2 = c1.copy()
        c3 = ff.format_holdings(c2)
        historical_data = pd.read_parquet('data_parquet/Holdings_grad.parquet')
        historical_data = historical_data._append(c3, ignore_index=True)
        historical_data_unique = historical_data.drop_duplicates(subset=['CUSIP', 'ReportDate', 'Quantity'], keep='last')
        historical_data_unique.to_parquet('data_parquet/Holdings_grad.parquet')

    def load_dividends(self, queryID):
        c0 = self.IBKR_query(queryID)
        c1 = c0[c0['ExDate'] != 'ExDate']
        c2 = c1.copy()
        c3 = ff.format_dividends(c2)
        c3[['AccountAlias', 'Model']] = c2[['AccountAlias', 'Model']].where(pd.notnull(c2), None)
        historical_data = pd.read_parquet('data_parquet/Dividends_grad.parquet')
        historical_data = historical_data._append(c3, ignore_index=True)
        historical_data_unique = historical_data.drop_duplicates(subset=['CUSIP', 'ExDate', 'PayDate'], keep='last')
        historical_data_unique.to_parquet('data_parquet/Dividends_grad.parquet')

    def load_trades(self, queryID):
        c0 = self.IBKR_query(queryID)
        c1 = c0[c0['Conid'] != 'Conid']
        c2 = c1.copy()
        c3 = ff.format_trades(c2)
        historical_data = pd.read_parquet('data_parquet/Trades_grad.parquet')
        historical_data = historical_data._append(c3, ignore_index=True)
        historical_data_unique = historical_data.drop_duplicates(subset=['CUSIP', 'TradeID', 'ReportDate'], keep='last')
        historical_data_unique.to_parquet('data_parquet/Trades_grad.parquet')

